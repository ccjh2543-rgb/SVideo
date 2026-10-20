// Package video 是视频、点赞、评论领域。
//
// 该包只包含领域实体、领域错误和仓储接口，不依赖 GORM、Redis、Gin。
package video

import (
	"strings"
	"time"
)

// Video 视频聚合根。
type Video struct {
	ID          uint      `json:"id"`
	AuthorID    uint      `json:"author_id"`
	Username    string    `json:"username"`
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	PlayURL     string    `json:"play_url"`
	CoverURL    string    `json:"cover_url"`
	CreateTime  time.Time `json:"create_time"`
	LikesCount  int64     `json:"likes_count"`
	Popularity  int64     `json:"popularity"`
}

// ValidateForPublish 校验并规范化发布参数。
func (v *Video) ValidateForPublish() error {
	if v == nil {
		return ErrVideoInvalid
	}
	v.Title = strings.TrimSpace(v.Title)
	v.PlayURL = strings.TrimSpace(v.PlayURL)
	v.CoverURL = strings.TrimSpace(v.CoverURL)
	if v.Title == "" {
		return ErrTitleRequired
	}
	if v.PlayURL == "" {
		return ErrPlayURLRequired
	}
	if v.CoverURL == "" {
		return ErrCoverURLRequired
	}
	return nil
}

// Like 点赞记录。
type Like struct {
	ID        uint      `json:"id"`
	VideoID   uint      `json:"video_id"`
	AccountID uint      `json:"account_id"`
	CreatedAt time.Time `json:"created_at"`
}

// Comment 评论实体。
type Comment struct {
	ID        uint      `json:"id"`
	Username  string    `json:"username"`
	VideoID   uint      `json:"video_id"`
	AuthorID  uint      `json:"author_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// Validate 校验并规范化评论内容。
func (c *Comment) Validate() error {
	if c == nil {
		return ErrCommentInvalid
	}
	c.Username = strings.TrimSpace(c.Username)
	c.Content = strings.TrimSpace(c.Content)
	if c.VideoID == 0 || c.AuthorID == 0 {
		return ErrCommentRequired
	}
	if c.Content == "" {
		return ErrCommentContentRequired
	}
	return nil
}

// OutboxMessage 视频发布事件的事务消息。
type OutboxMessage struct {
	ID         uint       `json:"id"`
	VideoID    uint       `json:"video_id"`
	AuthorID   uint       `json:"author_id"`
	EventType  string     `json:"event_type"`
	CreateTime time.Time  `json:"create_time"`
	Status     string     `json:"status"`
	RetryCount int        `json:"retry_count"`
	NextRetry  *time.Time `json:"next_retry,omitempty"`
}
