package mysql

import "time"

// CreatorStatsModel 创作者统计表。
type CreatorStatsModel struct {
	AuthorID        uint       `gorm:"primaryKey;autoIncrement:false"`
	FansCount       int64      `gorm:"not null;default:0"`
	IsBig           bool       `gorm:"not null;default:false"`
	LastPublishTime *time.Time `gorm:"index"`
}

// TableName 指定创作者统计表名。
func (CreatorStatsModel) TableName() string {
	return "creator_stats"
}
