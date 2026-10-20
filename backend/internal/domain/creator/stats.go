// Package creator 维护创作者维度的统计数据，供 Feed 推拉结合和热榜使用。
package creator

import (
	"context"
	"time"
)

// BigVFansThreshold 大 V 判定阈值。
// 超过该粉丝数的作者发布视频时不做写扩散，由粉丝读时拉取。
const BigVFansThreshold int64 = 100000

// Stats 创作者统计信息。
type Stats struct {
	AuthorID        uint
	FansCount       int64
	IsBig           bool
	LastPublishTime time.Time
}

// Repository 创作者统计仓储接口。
type Repository interface {
	// Get 查询作者统计，不存在时返回粉丝数为 0 的默认值。
	Get(ctx context.Context, authorID uint) (*Stats, error)
	// UpsertOnPublish 更新作者最后发布时间。
	UpsertOnPublish(ctx context.Context, authorID uint, publishTime time.Time) error
	// IncrementFans 原子增减粉丝数，并同步维护 is_big 标记。
	IncrementFans(ctx context.Context, authorID uint, delta int64) error
	// ListFollowing 查询当前用户关注的全部作者统计。
	ListFollowing(ctx context.Context, followerID uint) ([]*Stats, error)
}
