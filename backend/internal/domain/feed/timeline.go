// Package feed 定义 Feed 时间线领域的接口和值对象。
package feed

import "context"

// TimelineEntry 时间线中的一条视频记录。
// Score 使用视频发布时间的毫秒时间戳，保证按时间倒序。
type TimelineEntry struct {
	VideoID uint
	Score   int64
}

// DefaultInboxCapacity 粉丝 Inbox 的默认热窗口容量。
// Inbox 只保存浅页热数据，更深的历史由数据库游标兜底。
const DefaultInboxCapacity int64 = 300

// TimelineRepository 推拉结合时间线仓储接口。
//
// Inbox：粉丝收件箱，小 V 发布时写扩散到这里。
// Outbox：作者发件箱，所有作者都会维护，大 V 由粉丝读时拉取。
type TimelineRepository interface {
	PushInbox(ctx context.Context, followerID uint, videoID uint, score int64) error
	PushInboxBatch(ctx context.Context, followerIDs []uint, videoID uint, score int64) error
	// PushInboxEntries 批量写入带各自分数的条目，用于新关注时回填作者历史视频。
	PushInboxEntries(ctx context.Context, followerID uint, entries []TimelineEntry) error
	// PushInboxEntriesWindow 只写入能进入 Inbox 容量窗口的条目。
	// Inbox 未满时按剩余容量取最新条目；已满时只写比当前最老条目更新的条目。
	PushInboxEntriesWindow(ctx context.Context, followerID uint, entries []TimelineEntry, capacity int64) error
	PushOutbox(ctx context.Context, authorID uint, videoID uint, score int64) error

	ReadInbox(ctx context.Context, userID uint, maxScore int64, limit int) ([]TimelineEntry, error)
	ReadOutbox(ctx context.Context, authorID uint, maxScore int64, limit int) ([]TimelineEntry, error)
	ReadOutboxes(ctx context.Context, authorIDs []uint, maxScore int64, limit int) ([][]TimelineEntry, error)

	RemoveFromInbox(ctx context.Context, followerID uint, videoIDs []uint) error
}

// FollowerReader 粉丝分页读取接口，避免一次加载全部粉丝。
type FollowerReader interface {
	// ListFollowerIDsAfter 按粉丝 ID 游标分页查询粉丝 ID。
	ListFollowerIDsAfter(ctx context.Context, vloggerID uint, afterFollowerID uint, limit int) ([]uint, error)
}
