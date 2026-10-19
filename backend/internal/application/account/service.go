// Package account 账号应用层，负责编排注册、登录、刷新、登出、改密和改名用例。
package account

import (
	"context"
	"errors"
	"time"

	domainaccount "feedsystem_video_go/internal/domain/account"
)

// Service 账号应用服务。
type Service struct {
	accounts      domainaccount.Repository
	refreshTokens domainaccount.RefreshTokenRepository
	tokens        domainaccount.TokenService
	hasher        domainaccount.PasswordHasher
	now           func() time.Time
}

// NewService 创建账号应用服务。
func NewService(
	accounts domainaccount.Repository,
	refreshTokens domainaccount.RefreshTokenRepository,
	tokens domainaccount.TokenService,
	hasher domainaccount.PasswordHasher,
) *Service {
	return &Service{
		accounts:      accounts,
		refreshTokens: refreshTokens,
		tokens:        tokens,
		hasher:        hasher,
		now:           time.Now,
	}
}

// Register 注册账号。
func (s *Service) Register(ctx context.Context, username, password string) error {
	normalized, err := domainaccount.NormalizeUsername(username)
	if err != nil {
		return err
	}
	if err := domainaccount.ValidatePassword(password); err != nil {
		return err
	}

	passwordHash, err := s.hasher.Hash(password)
	if err != nil {
		return err
	}

	return s.accounts.Create(ctx, &domainaccount.Account{
		Username:     normalized,
		PasswordHash: passwordHash,
	})
}

// Login 校验账号密码并签发双 Token。
func (s *Service) Login(ctx context.Context, username, password string) (*domainaccount.TokenPair, error) {
	normalized, err := domainaccount.NormalizeUsername(username)
	if err != nil {
		return nil, domainaccount.ErrInvalidCredentials
	}
	if err := domainaccount.ValidatePassword(password); err != nil {
		return nil, domainaccount.ErrInvalidCredentials
	}

	account, err := s.accounts.FindByUsername(ctx, normalized)
	if err != nil {
		if errors.Is(err, domainaccount.ErrAccountNotFound) {
			return nil, domainaccount.ErrInvalidCredentials
		}
		return nil, err
	}
	if err := s.hasher.Compare(account.PasswordHash, password); err != nil {
		return nil, domainaccount.ErrInvalidCredentials
	}

	pair, err := s.tokens.Issue(account.ID, account.Username, "")
	if err != nil {
		return nil, err
	}
	if err := s.saveRefreshToken(ctx, account.ID, pair); err != nil {
		return nil, err
	}
	return pair, nil
}

// Refresh 使用 Refresh Token 轮换出一对新 Token。
// 同一个 Refresh Token 被第二次使用时，视为重放攻击并撤销整个 Token Family。
func (s *Service) Refresh(ctx context.Context, refreshToken string) (*domainaccount.TokenPair, error) {
	claims, err := s.tokens.ParseRefresh(refreshToken)
	if err != nil {
		return nil, domainaccount.ErrTokenInvalid
	}

	record, err := s.refreshTokens.FindByJTI(ctx, claims.JTI)
	if err != nil {
		return nil, domainaccount.ErrTokenInvalid
	}
	if record.AccountID != claims.AccountID || record.FamilyID != claims.FamilyID {
		return nil, domainaccount.ErrTokenInvalid
	}
	if record.RevokedAt != nil {
		// 已撤销的令牌再次出现，说明可能被窃取，撤销整个登录会话。
		_ = s.refreshTokens.RevokeFamily(ctx, record.FamilyID)
		return nil, domainaccount.ErrTokenRevoked
	}
	if s.now().After(record.ExpiresAt) {
		return nil, domainaccount.ErrTokenInvalid
	}

	rotated, err := s.refreshTokens.RevokeIfActive(ctx, claims.JTI)
	if err != nil {
		return nil, err
	}
	if !rotated {
		// 并发刷新时只有一个请求能成功，其余请求视为重放。
		_ = s.refreshTokens.RevokeFamily(ctx, record.FamilyID)
		return nil, domainaccount.ErrTokenRevoked
	}

	account, err := s.accounts.FindByID(ctx, record.AccountID)
	if err != nil {
		return nil, domainaccount.ErrTokenInvalid
	}

	pair, err := s.tokens.Issue(account.ID, account.Username, record.FamilyID)
	if err != nil {
		return nil, err
	}
	if err := s.saveRefreshToken(ctx, account.ID, pair); err != nil {
		return nil, err
	}
	return pair, nil
}

// Logout 撤销当前 Refresh Token 所属的登录会话。
func (s *Service) Logout(ctx context.Context, accountID uint, refreshToken string) error {
	claims, err := s.tokens.ParseRefresh(refreshToken)
	if err != nil {
		// 登出是幂等操作，Refresh Token 无效时直接返回成功。
		return nil
	}
	if claims.AccountID != accountID {
		return domainaccount.ErrPermissionDenied
	}
	return s.refreshTokens.RevokeFamily(ctx, claims.FamilyID)
}

// LogoutAll 撤销账号的全部登录会话。
func (s *Service) LogoutAll(ctx context.Context, accountID uint) error {
	if accountID == 0 {
		return domainaccount.ErrPermissionDenied
	}
	return s.refreshTokens.RevokeAllByAccount(ctx, accountID)
}

// ChangePassword 修改密码，并撤销该账号的全部登录会话。
func (s *Service) ChangePassword(ctx context.Context, accountID uint, oldPassword, newPassword string) error {
	if err := domainaccount.ValidatePassword(oldPassword); err != nil {
		return domainaccount.ErrInvalidCredentials
	}
	if err := domainaccount.ValidatePassword(newPassword); err != nil {
		return err
	}

	account, err := s.accounts.FindByID(ctx, accountID)
	if err != nil {
		return err
	}
	if err := s.hasher.Compare(account.PasswordHash, oldPassword); err != nil {
		return domainaccount.ErrInvalidCredentials
	}

	passwordHash, err := s.hasher.Hash(newPassword)
	if err != nil {
		return err
	}
	if err := s.accounts.UpdatePassword(ctx, accountID, passwordHash); err != nil {
		return err
	}
	return s.refreshTokens.RevokeAllByAccount(ctx, accountID)
}

// Rename 修改用户名，撤销旧会话并签发新 Token。
func (s *Service) Rename(ctx context.Context, accountID uint, newUsername string) (*domainaccount.TokenPair, error) {
	account, err := s.accounts.FindByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if err := account.Rename(newUsername); err != nil {
		return nil, err
	}
	if err := s.accounts.UpdateUsername(ctx, accountID, account.Username); err != nil {
		return nil, err
	}
	if err := s.refreshTokens.RevokeAllByAccount(ctx, accountID); err != nil {
		return nil, err
	}

	pair, err := s.tokens.Issue(account.ID, account.Username, "")
	if err != nil {
		return nil, err
	}
	if err := s.saveRefreshToken(ctx, account.ID, pair); err != nil {
		return nil, err
	}
	return pair, nil
}

// FindByID 查询账号。
func (s *Service) FindByID(ctx context.Context, id uint) (*domainaccount.Account, error) {
	return s.accounts.FindByID(ctx, id)
}

// FindByUsername 按用户名查询账号。
func (s *Service) FindByUsername(ctx context.Context, username string) (*domainaccount.Account, error) {
	normalized, err := domainaccount.NormalizeUsername(username)
	if err != nil {
		return nil, err
	}
	return s.accounts.FindByUsername(ctx, normalized)
}

// ValidateAccessToken 校验 Access Token 并返回声明。
func (s *Service) ValidateAccessToken(token string) (*domainaccount.Claims, error) {
	return s.tokens.ParseAccess(token)
}

// saveRefreshToken 持久化刷新令牌记录。
func (s *Service) saveRefreshToken(ctx context.Context, accountID uint, pair *domainaccount.TokenPair) error {
	if pair == nil {
		return domainaccount.ErrTokenInvalid
	}
	return s.refreshTokens.Create(ctx, &domainaccount.RefreshToken{
		AccountID: accountID,
		JTI:       pair.RefreshJTI,
		FamilyID:  pair.FamilyID,
		ExpiresAt: pair.RefreshExpiresAt,
		CreatedAt: s.now(),
	})
}
