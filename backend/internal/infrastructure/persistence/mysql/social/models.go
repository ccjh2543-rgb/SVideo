// Package social 提供社交上下文的 MySQL 仓储实现。
package social

import "time"

// SocialModel 关注关系表 GORM 模型。
type SocialModel struct {
	ID         uint `gorm:"primaryKey"`
	FollowerID uint `gorm:"not null;index:idx_social_follower;uniqueIndex:idx_social_follower_vlogger"`
	VloggerID  uint `gorm:"not null;index:idx_social_vlogger;uniqueIndex:idx_social_follower_vlogger"`
}

// TableName 指定关注关系表名。
func (SocialModel) TableName() string { return "socials" }

// StatsModel 创作者统计表 GORM 模型。
type StatsModel struct {
	AuthorID        uint       `gorm:"primaryKey;autoIncrement:false"`
	FansCount       int64      `gorm:"not null;default:0"`
	IsBig           bool       `gorm:"not null;default:false"`
	LastPublishTime *time.Time `gorm:"index"`
}

// TableName 指定创作者统计表名。
func (StatsModel) TableName() string { return "creator_stats" }
