package account

import (
	"context"

	"gorm.io/gorm"
)

type AccountRepository struct {
	db *gorm.DB
}

// 创建账号仓库
func NewAccountRepository(db *gorm.DB) *AccountRepository {
	return &AccountRepository{db: db}
}

// 创建账号
func (ar *AccountRepository) CreateAccount(ctx context.Context, account *Account) error {
	if err := ar.db.WithContext(ctx).Create(account).Error; err != nil {
		return err
	}
	return nil
}

// 根据ID重命名账号
func (ar *AccountRepository) Rename(ctx context.Context, id uint, newUsername string) error {
	result := ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Update("username", newUsername)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// 根据ID重命名账号并更新token
func (ar *AccountRepository) RenameWithToken(ctx context.Context, id uint, newUsername string, token string) error {
	return ar.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&Account{}).Where("id = ?", id).Update("username", newUsername)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		if err := tx.Model(&Account{}).Where("id = ?", id).Update("token", token).Error; err != nil {
			return err
		}
		return nil
	})
}

// 根据ID修改密码
func (ar *AccountRepository) ChangePassword(ctx context.Context, id uint, newPassword string) error {
	if err := ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Update("password", newPassword).Error; err != nil {
		return err
	}
	return nil
}

// 根据ID查询账号
func (ar *AccountRepository) FindByID(ctx context.Context, id uint) (*Account, error) {
	var account Account
	if err := ar.db.WithContext(ctx).First(&account, id).Error; err != nil {
		return nil, err
	}
	return &account, nil
}

// 根据用户名查询账号
func (ar *AccountRepository) FindByUsername(ctx context.Context, username string) (*Account, error) {
	var account Account
	if err := ar.db.WithContext(ctx).Where("username = ?", username).First(&account).Error; err != nil {
		return nil, err
	}
	return &account, nil
}

// 登录
func (ar *AccountRepository) Login(ctx context.Context, id uint, token string) error {
	if err := ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Update("token", token).Error; err != nil {
		return err
	}
	return nil
}

// 退出
func (ar *AccountRepository) Logout(ctx context.Context, id uint) error {
	if err := ar.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Update("token", "").Error; err != nil {
		return err
	}
	return nil
}
