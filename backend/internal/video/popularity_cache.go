package video

import (
	"context"
	"fmt"
	"strconv"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

// UpdatePopularityCache 更新视频的流行度缓存
// ctx: 上层上下文，用于控制整体超时
// cache: Redis 缓存客户端
// id: 视频ID
// change: 流行度变化值（增量为正，减量为负）
func UpdatePopularityCache(ctx context.Context, cache *rediscache.Client, id uint, change int64) {
	// 防御性检查：若 cache 为空、视频ID无效或变动值为0，直接返回
	if cache == nil || id == 0 || change == 0 {
		return
	}

	// -------------------- 1. 失效视频详情缓存 --------------------
	// 流行度变了，之前缓存的视频详情（可能包含热度数）应该失效。
	// 这里使用 context.Background() 而非传入的 ctx，是为了确保即使上游请求已结束，
	// 清理操作也能尽量执行，避免脏缓存残留。
	_ = cache.Del(context.Background(), fmt.Sprintf("video:detail:id=%d", id))

	// -------------------- 2. 更新小时级热度排行榜 --------------------
	// 以小时为桶，配合读取时的线性权重衰减，形成 24 小时滑动窗口。
	now := time.Now().UTC().Truncate(time.Hour)

	// 构造窗口 key，格式示例: "hot:video:1h:2026012815"
	windowKey := "hot:video:1h:" + now.Format("2006010215")

	// 将视频ID转换为字符串，作为有序集合中的成员（member）。
	member := strconv.FormatUint(uint64(id), 10)

	// 创建带超时的上下文，控制此次 Redis 操作的总耗时，避免长时间阻塞。
	// 50ms 是一个比较紧的期限，适合高频写入场景，保证不影响主流程。
	opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel() // 函数退出时释放资源

	// 对窗口有序集合执行 ZINCRBY，给 member 增加指定分数（热度变动值）。
	// 若 member 不存在，则创建并赋予该分数。
	_ = cache.ZincrBy(opCtx, windowKey, member, float64(change))

	// 小时桶保留 48 小时，足够覆盖 24 小时窗口和快照计算。
	_ = cache.Expire(opCtx, windowKey, 48*time.Hour)
}
