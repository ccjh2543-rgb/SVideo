package mysql

import (
	"context"
	"errors"
	"time"

	domainaccount "feedsystem_video_go/internal/domain/account"

	"gorm.io/gorm"
)

// RefreshTokenRepository 刷新令牌仓储的 MySQL 实现。
type RefreshTokenRepository struct {
	db *gorm.DB
}

// NewRefreshTokenRepository 创建刷新令牌仓储。
func NewRefreshTokenRepository(db *gorm.DB) *RefreshTokenRepository {
	return &RefreshTokenRepository{db: db}
}

// Create 保存刷新令牌记录。
func (r *RefreshTokenRepository) Create(ctx context.Context, token *domainaccount.RefreshToken) error {
	if token == nil {
		return domainaccount.ErrTokenInvalid
	}
	model := &RefreshTokenModel{
		AccountID: token.AccountID,
		JTI:       token.JTI,
		FamilyID:  token.FamilyID,
		ExpiresAt: token.ExpiresAt,
		RevokedAt: token.RevokedAt,
	}
	if token.CreatedAt.IsZero() {
		model.CreatedAt = time.Now()
	} else {
		model.CreatedAt = token.CreatedAt
	}
	if err := r.db.WithContext(ctx).Create(model).Error; err != nil {
		return err
	}
	token.ID = model.ID
	return nil
}

// FindByJTI 按 JTI 查询刷新令牌。
func (r *RefreshTokenRepository) FindByJTI(ctx context.Context, jti string) (*domainaccount.RefreshToken, error) {
	var model RefreshTokenModel
	if err := r.db.WithContext(ctx).Where("jti = ?", jti).First(&model).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainaccount.ErrTokenInvalid
		}
		return nil, err
	}
	return &domainaccount.RefreshToken{
		ID:        model.ID,
		AccountID: model.AccountID,
		JTI:       model.JTI,
		FamilyID:  model.FamilyID,
		ExpiresAt: model.ExpiresAt,
		RevokedAt: model.RevokedAt,
		CreatedAt: model.CreatedAt,
	}, nil
}

// RevokeIfActive 原子撤销未撤销的刷新令牌。
func (r *RefreshTokenRepository) RevokeIfActive(ctx context.Context, jti string) (bool, error) {
	now := time.Now()
	result := r.db.WithContext(ctx).
		Model(&RefreshTokenModel{}).
		Where("jti = ? AND revoked_at IS NULL", jti).
		Update("revoked_at", now)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// RevokeFamily 撤销一个登录会话下的全部刷新令牌。
func (r *RefreshTokenRepository) RevokeFamily(ctx context.Context, familyID string) error {
	if familyID == "" {
		return nil
	}
	return r.db.WithContext(ctx).
		Model(&RefreshTokenModel{}).
		Where("family_id = ? AND revoked_at IS NULL", familyID).
		Update("revoked_at", time.Now()).Error
}

// RevokeAllByAccount 撤销账号的全部刷新令牌。
func (r *RefreshTokenRepository) RevokeAllByAccount(ctx context.Context, accountID uint) error {
	if accountID == 0 {
		return nil
	}
	return r.db.WithContext(ctx).
		Model(&RefreshTokenModel{}).
		Where("account_id = ? AND revoked_at IS NULL", accountID).
		Update("revoked_at", time.Now()).Error
}
