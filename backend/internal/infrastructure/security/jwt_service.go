// Package security 提供认证相关的基础设施实现：JWT 签发/解析、密码哈希。
package security

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"time"

	domainaccount "feedsystem_video_go/internal/domain/account"

	"github.com/golang-jwt/jwt/v5"
)

const (
	defaultAccessTTL  = 15 * time.Minute
	defaultRefreshTTL = 7 * 24 * time.Hour
	issuer            = "svideo"
)

// JWTService 基于 HS256 的双 Token 服务。
type JWTService struct {
	secret     []byte
	accessTTL  time.Duration
	refreshTTL time.Duration
	now        func() time.Time
}

// NewJWTServiceFromEnv 从 JWT_SECRET 环境变量创建令牌服务。
// 未配置密钥时直接返回错误，避免使用不安全的默认密钥。
func NewJWTServiceFromEnv() (*JWTService, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return nil, errors.New("JWT_SECRET is required")
	}
	return NewJWTService([]byte(secret), defaultAccessTTL, defaultRefreshTTL), nil
}

// NewJWTService 创建令牌服务，主要用于测试或未来扩展不同有效期。
func NewJWTService(secret []byte, accessTTL, refreshTTL time.Duration) *JWTService {
	return &JWTService{
		secret:     secret,
		accessTTL:  accessTTL,
		refreshTTL: refreshTTL,
		now:        time.Now,
	}
}

// tokenClaims JWT 内部声明，增加了类型和会话家族字段。
type tokenClaims struct {
	AccountID uint                    `json:"account_id"`
	Username  string                  `json:"username"`
	TokenType domainaccount.TokenType `json:"token_type"`
	FamilyID  string                  `json:"family_id,omitempty"`
	jwt.RegisteredClaims
}

// Issue 签发双 Token。
func (s *JWTService) Issue(accountID uint, username string, familyID string) (*domainaccount.TokenPair, error) {
	if s == nil || len(s.secret) == 0 {
		return nil, errors.New("jwt service is not initialized")
	}
	if accountID == 0 {
		return nil, errors.New("account id is required")
	}

	now := s.now()
	if familyID == "" {
		familyID = newRandomID()
	}

	accessJTI := newRandomID()
	refreshJTI := newRandomID()
	accessExpiresAt := now.Add(s.accessTTL)
	refreshExpiresAt := now.Add(s.refreshTTL)

	accessToken, err := s.sign(tokenClaims{
		AccountID: accountID,
		Username:  username,
		TokenType: domainaccount.TokenTypeAccess,
		FamilyID:  familyID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			ID:        accessJTI,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(accessExpiresAt),
		},
	})
	if err != nil {
		return nil, err
	}

	refreshToken, err := s.sign(tokenClaims{
		AccountID: accountID,
		Username:  username,
		TokenType: domainaccount.TokenTypeRefresh,
		FamilyID:  familyID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			ID:        refreshJTI,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(refreshExpiresAt),
		},
	})
	if err != nil {
		return nil, err
	}

	return &domainaccount.TokenPair{
		AccessToken:      accessToken,
		RefreshToken:     refreshToken,
		AccessExpiresAt:  accessExpiresAt,
		RefreshExpiresAt: refreshExpiresAt,
		RefreshJTI:       refreshJTI,
		FamilyID:         familyID,
	}, nil
}

// ParseAccess 解析并校验 Access Token。
func (s *JWTService) ParseAccess(token string) (*domainaccount.Claims, error) {
	return s.parse(token, domainaccount.TokenTypeAccess)
}

// ParseRefresh 解析并校验 Refresh Token。
func (s *JWTService) ParseRefresh(token string) (*domainaccount.Claims, error) {
	return s.parse(token, domainaccount.TokenTypeRefresh)
}

func (s *JWTService) sign(claims tokenClaims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.secret)
}

func (s *JWTService) parse(tokenString string, expected domainaccount.TokenType) (*domainaccount.Claims, error) {
	if s == nil || len(s.secret) == 0 || tokenString == "" {
		return nil, domainaccount.ErrTokenInvalid
	}

	claims := &tokenClaims{}
	token, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		func(token *jwt.Token) (interface{}, error) {
			if token.Method == nil || token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, errors.New("unexpected signing method")
			}
			return s.secret, nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
	)
	if err != nil || !token.Valid {
		return nil, domainaccount.ErrTokenInvalid
	}
	if claims.TokenType != expected {
		return nil, domainaccount.ErrTokenInvalid
	}
	if claims.ExpiresAt == nil || claims.AccountID == 0 || claims.ID == "" {
		return nil, domainaccount.ErrTokenInvalid
	}

	return &domainaccount.Claims{
		AccountID: claims.AccountID,
		Username:  claims.Username,
		TokenType: claims.TokenType,
		JTI:       claims.ID,
		FamilyID:  claims.FamilyID,
		ExpiresAt: claims.ExpiresAt.Time,
	}, nil
}

// newRandomID 生成 128 位随机 ID，用于 JTI 和 Token Family。
func newRandomID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand 失败属于系统级异常，使用时间戳兜底但仍保证唯一性足够高。
		return hex.EncodeToString([]byte(time.Now().Format("20060102150405.000000000")))
	}
	return hex.EncodeToString(buf)
}
