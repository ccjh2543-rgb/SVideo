// Package mysql 提供基于 GORM + MySQL 的仓储实现。
package mysql

import domainaccount "feedsystem_video_go/internal/domain/account"

// AccountModel 账号表的 GORM 模型。
// 与存量 accounts 表保持一致，避免在迁移期产生额外字段。
type AccountModel struct {
	ID       uint   `gorm:"primaryKey"`
	Username string `gorm:"unique"`
	Password string `gorm:"column:password"`
	Token    string `gorm:"column:token"`
}

// TableName 指定账号表名。
func (AccountModel) TableName() string {
	return "accounts"
}

// toDomain 将 GORM 模型转换为领域实体。
func (m *AccountModel) toDomain() *domainaccount.Account {
	if m == nil {
		return nil
	}
	return &domainaccount.Account{
		ID:           m.ID,
		Username:     m.Username,
		PasswordHash: m.Password,
	}
}
