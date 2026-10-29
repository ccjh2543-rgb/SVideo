// Package redis 提供基于 Redis 的 Feed 时间线仓储实现。
package redis

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"

	goredis "github.com/redis/go-redis/v9"
)

const (
	// Inbox 与 Outbox 是热缓存而不是全量存储：
	// 超过容量的历史内容由数据库游标查询兜底，不能再从 Outbox 反查。
	inboxMaxSize  int64 = domainfeed.DefaultInboxCapacity
	outboxMaxSize int64 = 50
	timelineTTL         = 7 * 24 * time.Hour
)

// timelineMember 把 videoID 编码成固定 20 位十进制字符串。
// Redis 对相同 score 的 member 按字典序排序，未补零时 "9" > "10"，
// 会导致同毫秒视频的 ID 倒序与数字序不一致。
func timelineMember(videoID uint) string {
	return fmt.Sprintf("%020d", videoID)
}

// TimelineRepository 推拉结合时间线的 Redis 实现。
type TimelineRepository struct {
	cache *Client
}

// NewTimelineRepository 创建时间线仓储。
func NewTimelineRepository(cache *Client) *TimelineRepository {
	return &TimelineRepository{cache: cache}
}

// PushInbox 向单个粉丝收件箱写入视频。
func (r *TimelineRepository) PushInbox(ctx context.Context, followerID uint, videoID uint, score int64) error {
	return r.PushInboxBatch(ctx, []uint{followerID}, videoID, score)
}

// PushInboxBatch 批量向多个粉丝收件箱写入视频，并裁剪到固定长度。
func (r *TimelineRepository) PushInboxBatch(ctx context.Context, followerIDs []uint, videoID uint, score int64) error {
	if len(followerIDs) == 0 {
		return nil
	}
	member := timelineMember(videoID)
	entries := make(map[string][]goredis.Z, len(followerIDs))
	for _, followerID := range followerIDs {
		key := inboxKey(followerID)
		entries[key] = []goredis.Z{{Score: float64(score), Member: member}}
	}
	return r.cache.ZAddManyWithTrim(ctx, entries, 0, -inboxMaxSize-1, timelineTTL)
}

// PushInboxEntries 批量向粉丝收件箱写入带各自分数的条目，并裁剪到固定长度。
// 用于新关注时把作者最近的视频回填到粉丝 Inbox，保证首次刷新就能看到内容。
func (r *TimelineRepository) PushInboxEntries(ctx context.Context, followerID uint, entries []domainfeed.TimelineEntry) error {
	if followerID == 0 || len(entries) == 0 {
		return nil
	}
	values := make([]goredis.Z, 0, len(entries))
	for _, entry := range entries {
		if entry.VideoID == 0 {
			continue
		}
		values = append(values, goredis.Z{
			Score:  float64(entry.Score),
			Member: timelineMember(entry.VideoID),
		})
	}
	if len(values) == 0 {
		return nil
	}
	return r.cache.ZAddManyWithTrim(ctx, map[string][]goredis.Z{
		inboxKey(followerID): values,
	}, 0, -inboxMaxSize-1, timelineTTL)
}

// PushInboxEntriesWindow 只写入能进入 Inbox 容量窗口的条目。
//
// Inbox 未满时按剩余容量取最新条目；
// Inbox 已满时只写比当前最老条目更新的条目，避免回填的数据立刻被裁剪。
func (r *TimelineRepository) PushInboxEntriesWindow(
	ctx context.Context,
	followerID uint,
	entries []domainfeed.TimelineEntry,
	capacity int64,
) error {
	if followerID == 0 || len(entries) == 0 {
		return nil
	}
	if capacity <= 0 {
		capacity = inboxMaxSize
	}

	key := inboxKey(followerID)
	card, err := r.cache.ZCard(ctx, key)
	if err != nil {
		return err
	}

	var oldestScore int64
	var oldestID uint
	if card >= capacity {
		oldest, err := r.cache.ZRangeWithScores(ctx, key, 0, 0)
		if err != nil {
			return err
		}
		parsed := toEntries(oldest)
		if len(parsed) > 0 {
			oldestScore = parsed[0].Score
			oldestID = parsed[0].VideoID
		}
	}

	selected := selectInboxWindowEntries(entries, card, capacity, oldestScore, oldestID)
	if len(selected) == 0 {
		return nil
	}

	values := make([]goredis.Z, 0, len(selected))
	for _, entry := range selected {
		if entry.VideoID == 0 {
			continue
		}
		values = append(values, goredis.Z{
			Score:  float64(entry.Score),
			Member: timelineMember(entry.VideoID),
		})
	}
	if len(values) == 0 {
		return nil
	}
	return r.cache.ZAddManyWithTrim(ctx, map[string][]goredis.Z{
		key: values,
	}, 0, -inboxMaxSize-1, timelineTTL)
}

// selectInboxWindowEntries 计算哪些条目值得写入 Inbox。
// 条目按 (score, videoID) 倒序排序；Inbox 未满时取最新剩余容量条，
// 已满时只保留严格大于最老条目的记录。
func selectInboxWindowEntries(
	entries []domainfeed.TimelineEntry,
	card int64,
	capacity int64,
	oldestScore int64,
	oldestID uint,
) []domainfeed.TimelineEntry {
	if len(entries) == 0 || capacity <= 0 {
		return nil
	}
	sorted := make([]domainfeed.TimelineEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.VideoID != 0 {
			sorted = append(sorted, entry)
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Score == sorted[j].Score {
			return sorted[i].VideoID > sorted[j].VideoID
		}
		return sorted[i].Score > sorted[j].Score
	})

	if card < capacity {
		limit := capacity - card
		if int64(len(sorted)) > limit {
			sorted = sorted[:limit]
		}
		return sorted
	}

	// Inbox 已满：只有分数更高，或同分但 videoID 更大的条目才能留下。
	if oldestScore == 0 && oldestID == 0 {
		return nil
	}
	result := make([]domainfeed.TimelineEntry, 0, len(sorted))
	for _, entry := range sorted {
		if entry.Score > oldestScore || (entry.Score == oldestScore && entry.VideoID > oldestID) {
			result = append(result, entry)
		}
	}
	return result
}

// PushOutbox 向作者发件箱写入视频，并裁剪到固定长度。
func (r *TimelineRepository) PushOutbox(ctx context.Context, authorID uint, videoID uint, score int64) error {
	key := outboxKey(authorID)
	return r.cache.ZAddManyWithTrim(ctx, map[string][]goredis.Z{
		key: {{Score: float64(score), Member: timelineMember(videoID)}},
	}, 0, -outboxMaxSize-1, timelineTTL)
}

// ReadInbox 读取粉丝收件箱。
func (r *TimelineRepository) ReadInbox(ctx context.Context, userID uint, maxScore int64, limit int) ([]domainfeed.TimelineEntry, error) {
	values, err := r.cache.ZRevRangeByScoreWithScores(ctx, inboxKey(userID), scoreCursor(maxScore), "-inf", 0, int64(limit))
	if err != nil {
		return nil, err
	}
	return toEntries(values), nil
}

// ReadOutbox 读取作者发件箱。
func (r *TimelineRepository) ReadOutbox(ctx context.Context, authorID uint, maxScore int64, limit int) ([]domainfeed.TimelineEntry, error) {
	values, err := r.cache.ZRevRangeByScoreWithScores(ctx, outboxKey(authorID), scoreCursor(maxScore), "-inf", 0, int64(limit))
	if err != nil {
		return nil, err
	}
	return toEntries(values), nil
}

// ReadOutboxes 批量读取多个作者发件箱。
func (r *TimelineRepository) ReadOutboxes(ctx context.Context, authorIDs []uint, maxScore int64, limit int) ([][]domainfeed.TimelineEntry, error) {
	if len(authorIDs) == 0 {
		return [][]domainfeed.TimelineEntry{}, nil
	}
	keys := make([]string, len(authorIDs))
	for i, authorID := range authorIDs {
		keys[i] = outboxKey(authorID)
	}
	values, err := r.cache.ZRevRangeByScoreWithScoresMulti(ctx, keys, scoreCursor(maxScore), "-inf", int64(limit))
	if err != nil {
		return nil, err
	}
	result := make([][]domainfeed.TimelineEntry, len(values))
	for i, items := range values {
		result[i] = toEntries(items)
	}
	return result, nil
}

// RemoveFromInbox 从粉丝收件箱删除指定视频，用于取关后的异步清理。
func (r *TimelineRepository) RemoveFromInbox(ctx context.Context, followerID uint, videoIDs []uint) error {
	if len(videoIDs) == 0 {
		return nil
	}
	members := make([]string, len(videoIDs))
	for i, videoID := range videoIDs {
		members[i] = timelineMember(videoID)
	}
	return r.cache.ZRem(ctx, inboxKey(followerID), members...)
}

// inboxKey 粉丝收件箱 key。
func inboxKey(userID uint) string {
	return fmt.Sprintf("feed:inbox:%d", userID)
}

// outboxKey 作者发件箱 key。
func outboxKey(authorID uint) string {
	return fmt.Sprintf("feed:outbox:%d", authorID)
}

// scoreCursor 把游标转换为 Redis 分数上界。
// 上界包含游标本身：同一毫秒可能有多条视频，交给应用层按 (score, videoID) 精确过滤，
// 避免像 score-1 那样把同毫秒的其他视频整段跳过。
func scoreCursor(maxScore int64) string {
	if maxScore <= 0 {
		return "+inf"
	}
	return strconv.FormatInt(maxScore, 10)
}

// toEntries 把 Redis 结果转换为领域时间线记录。
func toEntries(values []goredis.Z) []domainfeed.TimelineEntry {
	entries := make([]domainfeed.TimelineEntry, 0, len(values))
	for _, value := range values {
		member, ok := value.Member.(string)
		if !ok {
			continue
		}
		videoID, err := strconv.ParseUint(member, 10, 64)
		if err != nil || videoID == 0 {
			continue
		}
		entries = append(entries, domainfeed.TimelineEntry{
			VideoID: uint(videoID),
			Score:   int64(value.Score),
		})
	}
	return entries
}
