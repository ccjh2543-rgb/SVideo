// Package video 提供视频上下文 MySQL 仓储实现。
package video

import (
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"
)

// VideoModel 视频表 GORM 模型。
type VideoModel struct {
	ID          uint      `gorm:"primaryKey"`
	AuthorID    uint      `gorm:"index;not null"`
	Username    string    `gorm:"type:varchar(255);not null"`
	Title       string    `gorm:"type:varchar(255);not null"`
	Description string    `gorm:"type:varchar(255)"`
	PlayURL     string    `gorm:"type:varchar(255);not null"`
	CoverURL    string    `gorm:"type:varchar(255);not null"`
	CreateTime  time.Time `gorm:"autoCreateTime"`
	LikesCount  int64     `gorm:"column:likes_count;not null;default:0"`
	Popularity  int64     `gorm:"column:popularity;not null;default:0"`
}

// TableName 指定视频表名。
func (VideoModel) TableName() string { return "videos" }

// LikeModel 点赞表 GORM 模型。
type LikeModel struct {
	ID        uint      `gorm:"primaryKey"`
	VideoID   uint      `gorm:"uniqueIndex:idx_like_video_account;not null"`
	AccountID uint      `gorm:"uniqueIndex:idx_like_video_account;not null"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName 指定点赞表名。
func (LikeModel) TableName() string { return "likes" }

// CommentModel 评论表 GORM 模型。
type CommentModel struct {
	ID        uint      `gorm:"primaryKey"`
	Username  string    `gorm:"index"`
	VideoID   uint      `gorm:"index"`
	AuthorID  uint      `gorm:"index"`
	Content   string    `gorm:"type:text"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName 指定评论表名。
func (CommentModel) TableName() string { return "comments" }

// OutboxModel 事务消息表 GORM 模型。
type OutboxModel struct {
	ID        uint   `gorm:"primaryKey"`
	VideoID   uint   `gorm:"index"`
	AuthorID  uint   `gorm:"index"`
	EventType string `gorm:"type:varchar(50)"`
	// 复合索引覆盖 poller / 补偿任务的共同过滤条件：status + next_retry + create_time。
	CreateTime time.Time  `gorm:"autoCreateTime;index:idx_outbox_status_retry_time,priority:3"`
	Status     string     `gorm:"type:varchar(50);index:idx_outbox_status_retry_time,priority:1"`
	RetryCount int        `gorm:"not null;default:0"`
	NextRetry  *time.Time `gorm:"index:idx_outbox_status_retry_time,priority:2"`
}

// TableName 指定事务消息表名。
func (OutboxModel) TableName() string { return "outbox_msgs" }

func toVideoDomain(model *VideoModel) *domainvideo.Video {
	if model == nil {
		return nil
	}
	return &domainvideo.Video{
		ID:          model.ID,
		AuthorID:    model.AuthorID,
		Username:    model.Username,
		Title:       model.Title,
		Description: model.Description,
		PlayURL:     model.PlayURL,
		CoverURL:    model.CoverURL,
		CreateTime:  model.CreateTime,
		LikesCount:  model.LikesCount,
		Popularity:  model.Popularity,
	}
}

func toVideoModel(video *domainvideo.Video) *VideoModel {
	if video == nil {
		return nil
	}
	return &VideoModel{
		ID:          video.ID,
		AuthorID:    video.AuthorID,
		Username:    video.Username,
		Title:       video.Title,
		Description: video.Description,
		PlayURL:     video.PlayURL,
		CoverURL:    video.CoverURL,
		CreateTime:  video.CreateTime,
		LikesCount:  video.LikesCount,
		Popularity:  video.Popularity,
	}
}

func toOutboxDomain(model *OutboxModel) *domainvideo.OutboxMessage {
	if model == nil {
		return nil
	}
	return &domainvideo.OutboxMessage{
		ID:         model.ID,
		VideoID:    model.VideoID,
		AuthorID:   model.AuthorID,
		EventType:  model.EventType,
		CreateTime: model.CreateTime,
		Status:     model.Status,
		RetryCount: model.RetryCount,
		NextRetry:  model.NextRetry,
	}
}
