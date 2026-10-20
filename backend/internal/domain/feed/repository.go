package feed

import (
	"context"
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"
)

// LikesCountCursor 点赞数排行游标值对象。
// 点赞数相同的视频用 ID 兜底，保证翻页不重复、不遗漏。
type LikesCountCursor struct {
	LikesCount int64
	ID         uint
}

// FeedRepository Feed 流查询仓储接口。
// 只定义查询能力，SQL 实现放在 infrastructure/persistence/mysql/feed。
type FeedRepository interface {
	// ListLatest 按发布时间倒序查询，使用 (时间, ID) 复合游标分页。
	ListLatest(ctx context.Context, limit int, before time.Time, beforeID uint) ([]*domainvideo.Video, error)

	// ListLikesCountWithCursor 按点赞数倒序查询，使用 (点赞数, ID) 复合游标分页。
	ListLikesCountWithCursor(ctx context.Context, limit int, cursor *LikesCountCursor) ([]*domainvideo.Video, error)

	// ListByFollowing 查询关注作者发布的视频，用于推拉结合不可用时的降级路径。
	ListByFollowing(ctx context.Context, limit int, viewerID uint, before time.Time) ([]*domainvideo.Video, error)

	// ListByPopularity 按热度倒序查询，热度相同再按时间、ID 兜底。
	ListByPopularity(ctx context.Context, limit int, popularityBefore int64, before time.Time, beforeID uint) ([]*domainvideo.Video, error)

	// GetByIDs 批量按 ID 查询视频，是三级缓存的最后一道防线。
	GetByIDs(ctx context.Context, ids []uint) ([]*domainvideo.Video, error)
}
