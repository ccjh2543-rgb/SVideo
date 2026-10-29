package social

import "context"

// FansCountDrift 粉丝计数差异。
// Stored 是 creator_stats.fans_count 的当前值，Actual 是 socials 明细表的真实行数。
type FansCountDrift struct {
	AuthorID uint
	Stored   int64
	Actual   int64
}

// FansCounterReconcileRepository 粉丝计数对账仓储接口。
type FansCounterReconcileRepository interface {
	// FindFansCountDrifts 分批查询账实不符的创作者。
	FindFansCountDrifts(ctx context.Context, limit int) ([]FansCountDrift, error)
	// FixFansCount 把粉丝数修正为明细行数，并同步刷新 is_big。
	FixFansCount(ctx context.Context, authorID uint, expected int64) error
}
