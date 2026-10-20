package video

import (
	"context"

	domainvideo "feedsystem_video_go/internal/domain/video"
)

// DetailCache 视频详情缓存端口。
// 由 infrastructure/cache/redis/video 实现，应用层不感知 Redis。
type DetailCache interface {
	// Get 返回视频、是否逻辑过期、是否命中。
	Get(ctx context.Context, id uint) (*domainvideo.Video, bool, bool)
	Set(ctx context.Context, video *domainvideo.Video)
	SetNull(ctx context.Context, id uint)
	Delete(ctx context.Context, id uint)
	Lock(ctx context.Context, id uint) (token string, ok bool)
	Unlock(ctx context.Context, id uint, token string)
	BloomExists(ctx context.Context, id uint) (bool, error)
	BloomAdd(ctx context.Context, id uint)
	RebuildBloom(ctx context.Context, load func(afterID uint, limit int) ([]uint, error)) error
}

// PopularityUpdater 热度更新端口，MQ 和 Redis 缓存都可以实现。
type PopularityUpdater interface {
	Update(ctx context.Context, videoID uint, change int64) error
}

// LikeEventPublisher 点赞事件发布端口。
type LikeEventPublisher interface {
	Like(ctx context.Context, userID, videoID uint) error
	Unlike(ctx context.Context, userID, videoID uint) error
}

// CommentEventPublisher 评论事件发布端口。
type CommentEventPublisher interface {
	Publish(ctx context.Context, username string, videoID, authorID uint, content string) error
	Delete(ctx context.Context, commentID uint) error
}
