package mysql

import "time"

// RefreshTokenModel 刷新令牌表的 GORM 模型。
// 数据表保存完整令牌记录，用于刷新轮换、重放检测和审计。
type RefreshTokenModel struct {
	ID        uint       `gorm:"primaryKey"`
	AccountID uint       `gorm:"index;not null"`
	JTI       string     `gorm:"column:jti;uniqueIndex;size:64;not null"`
	FamilyID  string     `gorm:"column:family_id;index;size:64;not null"`
	ExpiresAt time.Time  `gorm:"not null"`
	RevokedAt *time.Time `gorm:"index"`
	CreatedAt time.Time  `gorm:"autoCreateTime"`
}

// TableName 指定刷新令牌表名。
func (RefreshTokenModel) TableName() string {
	return "refresh_tokens"
}
