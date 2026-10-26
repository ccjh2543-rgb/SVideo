package feed

import (
	"context"

	domainsocial "feedsystem_video_go/internal/domain/social"
	domainvideo "feedsystem_video_go/internal/domain/video"
)

// VideoCache 视频实体多级缓存端口。
// L1 为进程内缓存，L2 为 Redis；具体实现位于 infrastructure/cache/redis/feed。
type VideoCache interface {
	// GetLocal 读取 L1 本地缓存。
	GetLocal(id uint) (domainvideo.Video, bool)
	// SetLocal 写入 L1 本地缓存。
	SetLocal(id uint, video domainvideo.Video)
	// MGet 批量读取 L2 Redis 缓存，未命中的 ID 不会出现在返回值中。
	MGet(ctx context.Context, ids []uint) (map[uint]domainvideo.Video, error)
	// Set 异步写入 L2 Redis 缓存。
	Set(ctx context.Context, video domainvideo.Video)
}

// RankingCache 时间线与热榜的 Redis 端口。
// 应用层只表达"取时间线尾部、重建时间线、生成热榜快照"等意图，不感知 ZSET 细节。
type RankingCache interface {
	// GlobalTimelineTail 返回全局时间线中最老一条的毫秒时间戳；ZSET 为空时 ok=false。
	GlobalTimelineTail(ctx context.Context) (score int64, ok bool, err error)
	// RebuildGlobalTimeline 使用给定视频重建全局时间线。
	RebuildGlobalTimeline(ctx context.Context, videos []*domainvideo.Video) error
	// LatestIDs 返回时间线中 score 不大于 maxScore 的视频 ID；
	// maxScore <= 0 表示不限上界（第一页）。
	LatestIDs(ctx context.Context, maxScore int64, limit int) ([]uint, error)
	// EnsureSnapshot 确保热榜合并快照存在，不存在时按权重合并小时桶并设置 TTL。
	EnsureSnapshot(ctx context.Context, dest string, keys []string, weights []float64) error
	// SnapshotIDs 按热度倒序从快照中取 [offset, offset+limit) 的视频 ID。
	SnapshotIDs(ctx context.Context, dest string, offset int, limit int) ([]uint, error)
}

// FollowingCache 关注列表缓存端口。
// 读关注流时避免每页都执行 socials JOIN creator_stats。
type FollowingCache interface {
	// Get 读取关注列表，ok=false 表示未命中。
	Get(ctx context.Context, followerID uint) ([]*domainsocial.Stats, bool)
	// Set 写入关注列表缓存。
	Set(ctx context.Context, followerID uint, stats []*domainsocial.Stats)
}

// DependencyGuard 依赖熔断端口。
// 应用层只表达“该依赖当前是否允许调用”，具体熔断算法位于 infrastructure/resilience。
type DependencyGuard interface {
	// Allow 判断指定依赖是否放行。
	Allow(name string) bool
	// Record 记录依赖调用结果，err 非空表示依赖失败。
	Record(name string, err error)
}
