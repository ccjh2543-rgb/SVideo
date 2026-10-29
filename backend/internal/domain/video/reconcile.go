package video

import "context"

// LikesCountDrift 点赞计数差异。
// Stored 是 videos.likes_count 的当前值，Actual 是 likes 明细表的真实行数。
type LikesCountDrift struct {
	VideoID uint
	Stored  int64
	Actual  int64
}

// LikesCounterReconcileRepository 点赞计数对账仓储接口。
//
// 只做“发现差异 + 按明细修正”，不处理热度小时桶；
// Redis 热度的重建由独立任务按事件时间重放完成。
type LikesCounterReconcileRepository interface {
	// FindLikesCountDrifts 分批查询账实不符的视频。
	FindLikesCountDrifts(ctx context.Context, limit int) ([]LikesCountDrift, error)
	// FixLikesCount 把点赞数修正为明细行数。
	FixLikesCount(ctx context.Context, videoID uint, expected int64) error
}
