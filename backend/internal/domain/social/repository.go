package social

import (
	"context"
	"time"
)

// BigVFansThreshold 大 V 进入阈值。
// 粉丝数达到该值的作者发布视频时不做全量写扩散，由粉丝读时拉取。
const BigVFansThreshold int64 = 100000

// BigVExitThreshold 大 V 退出阈值。
// 采用滞回：粉丝数跌破该值才从小 V 状态退出，避免在 10 万附近抖动。
const BigVExitThreshold int64 = 90000

// HeavyEnterThreshold 关注大 V 数达到该值时进入重度用户。
const HeavyEnterThreshold int64 = 100

// HeavyExitThreshold 关注大 V 数低于该值时退出重度用户，避免阈值抖动。
const HeavyExitThreshold int64 = 80

// Repository 关注关系仓储接口。
type Repository interface {
	// Follow 创建关注关系。
	Follow(ctx context.Context, relation *Social) error
	// FollowWithStats 在同一事务内创建关注关系并增加粉丝数。
	// 用于 MQ 不可用时的同步兜底，避免“关系写入成功但粉丝数没更新”。
	FollowWithStats(ctx context.Context, relation *Social) error
	// Unfollow 删除关注关系。
	Unfollow(ctx context.Context, relation *Social) error
	// UnfollowWithStats 在同一事务内删除关注关系并减少粉丝数。
	// 关系不存在时返回 ErrNotFollowed，调用方不应再减少粉丝数。
	UnfollowWithStats(ctx context.Context, relation *Social) error
	// IsFollowed 判断关注关系是否存在。
	IsFollowed(ctx context.Context, relation *Social) (bool, error)
	// GetFollowers 查询博主的粉丝列表。
	GetFollowers(ctx context.Context, vloggerID uint) ([]*AccountBrief, error)
	// GetFollowing 查询用户关注的博主列表。
	GetFollowing(ctx context.Context, followerID uint) ([]*AccountBrief, error)
	// ListFollowerIDsAfter 按粉丝 ID 游标分页查询粉丝，用于 Feed 写扩散。
	ListFollowerIDsAfter(ctx context.Context, vloggerID uint, afterFollowerID uint, limit int) ([]uint, error)
}

// Stats 创作者统计信息。
type Stats struct {
	AuthorID  uint
	FansCount int64
	IsBig     bool
	// WasBig 表示作者曾经是大 V 且已经跌破退出阈值。
	// 读路径仍会拉取它的 Outbox，避免大 V 变小 V 的历史内容断层。
	WasBig          bool
	LastPublishTime time.Time
}

// StatsRepository 创作者统计仓储接口。
type StatsRepository interface {
	// Get 查询作者统计，不存在时返回粉丝数为 0 的默认值。
	Get(ctx context.Context, authorID uint) (*Stats, error)
	// UpsertOnPublish 更新作者最后发布时间。
	UpsertOnPublish(ctx context.Context, authorID uint, publishTime time.Time) error
	// IncrementFans 原子增减粉丝数，并同步维护 is_big 标记。
	IncrementFans(ctx context.Context, authorID uint, delta int64) error
	// ListFollowing 查询当前用户关注的全部作者统计。
	ListFollowing(ctx context.Context, followerID uint) ([]*Stats, error)
}
