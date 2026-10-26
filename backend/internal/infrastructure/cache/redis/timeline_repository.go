// Package redis 提供基于 Redis 的 Feed 时间线仓储实现。
package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"

	goredis "github.com/redis/go-redis/v9"
)

const (
	// Inbox 与 Outbox 是热缓存而不是全量存储：
	// 超过容量的历史内容由数据库游标查询兜底，不能再从 Outbox 反查。
	inboxMaxSize  int64 = 300
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
