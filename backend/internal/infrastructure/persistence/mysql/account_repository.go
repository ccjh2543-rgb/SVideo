package mysql

import (
	"context"
	"errors"

	domainaccount "feedsystem_video_go/internal/domain/account"
	"feedsystem_video_go/internal/util"

	"gorm.io/gorm"
)

// AccountRepository 账号仓储的 MySQL 实现。
type AccountRepository struct {
	db *gorm.DB
}

// NewAccountRepository 创建账号仓储。
func NewAccountRepository(db *gorm.DB) *AccountRepository {
	return &AccountRepository{db: db}
}

// Create 创建账号，用户名重复时返回领域错误。
func (r *AccountRepository) Create(ctx context.Context, account *domainaccount.Account) error {
	if account == nil {
		return domainaccount.ErrAccountNotFound
	}
	model := &AccountModel{
		ID:       account.ID,
		Username: account.Username,
		Password: account.PasswordHash,
	}
	if err := r.db.WithContext(ctx).Create(model).Error; err != nil {
		if util.IsDuplicateEntry(err) {
			return domainaccount.ErrUsernameTaken
		}
		return err
	}
	account.ID = model.ID
	return nil
}

// FindByID 按主键查询账号。
func (r *AccountRepository) FindByID(ctx context.Context, id uint) (*domainaccount.Account, error) {
	var model AccountModel
	if err := r.db.WithContext(ctx).First(&model, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainaccount.ErrAccountNotFound
		}
		return nil, err
	}
	return model.toDomain(), nil
}

// FindByUsername 按用户名查询账号。
func (r *AccountRepository) FindByUsername(ctx context.Context, username string) (*domainaccount.Account, error) {
	var model AccountModel
	if err := r.db.WithContext(ctx).Where("username = ?", username).First(&model).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainaccount.ErrAccountNotFound
		}
		return nil, err
	}
	return model.toDomain(), nil
}

// UpdateUsername 修改用户名，重复时返回领域错误。
func (r *AccountRepository) UpdateUsername(ctx context.Context, id uint, username string) error {
	result := r.db.WithContext(ctx).
		Model(&AccountModel{}).
		Where("id = ?", id).
		Update("username", username)
	if result.Error != nil {
		if util.IsDuplicateEntry(result.Error) {
			return domainaccount.ErrUsernameTaken
		}
		return result.Error
	}
	if result.RowsAffected == 0 {
		return domainaccount.ErrAccountNotFound
	}
	return nil
}

// UpdatePassword 更新密码哈希。
func (r *AccountRepository) UpdatePassword(ctx context.Context, id uint, passwordHash string) error {
	result := r.db.WithContext(ctx).
		Model(&AccountModel{}).
		Where("id = ?", id).
		Update("password", passwordHash)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return domainaccount.ErrAccountNotFound
	}
	return nil
}
