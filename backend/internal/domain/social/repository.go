package social

import (
	"context"
	"time"
)

// BigVFansThreshold 大 V 判定阈值。
// 超过该粉丝数的作者发布视频时不做写扩散，由粉丝读时拉取。
const BigVFansThreshold int64 = 100000

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
	AuthorID        uint
	FansCount       int64
	IsBig           bool
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
