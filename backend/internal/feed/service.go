// Package feed 短视频推荐流 - 业务服务层
//
// 【三级缓存数据流】
//
//	命中 → 直接返回
//	  ↑
//
// 请求 → L1 本地内存缓存 (3s) → 未命中 → L2 Redis 缓存 → 未命中 → L3 MySQL 数据库
//
//	              ↓
//	结果异步回填 L2 → 回填 L1
//
// 【防击穿技术】 SingleFlight: N个并发查同一条数据，只有1个真的查数据库
package feed

import (
	"context"
	"encoding/json"
	"sort"

	domaincreator "feedsystem_video_go/internal/domain/creator"
	domainfeed "feedsystem_video_go/internal/domain/feed"
	cachelib "feedsystem_video_go/internal/middleware/cache"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	redis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// FeedService Feed流核心业务逻辑
// 这是整个系统技术含量最高的地方
type FeedService struct {
	repo         *FeedRepository // L3 MySQL查询
	likeRepo     *video.LikeRepository
	rediscache   *rediscache.Client // L2 分布式缓存
	localcache   *cache.Cache       // L1 本地内存缓存（3秒）
	timelineRepo domainfeed.TimelineRepository
	creatorRepo  domaincreator.Repository
	requestGroup singleflight.Group // 防缓存击穿神器：1000个并发查同一条，只有1个查DB
}

// CachedFeedData Redis中缓存的视频流数据结构
type CachedFeedData struct {
	PublicVideos []video.Video `json:"public_videos"`
}

// NewFeedService 创建Feed流服务（依赖注入）
func NewFeedService(
	repo *FeedRepository,
	likeRepo *video.LikeRepository,
	rediscache *rediscache.Client,
	timelineRepo domainfeed.TimelineRepository,
	creatorRepo domaincreator.Repository,
) *FeedService {
	return &FeedService{
		repo:         repo,
		likeRepo:     likeRepo,
		rediscache:   rediscache,
		localcache:   cache.New(3*time.Second, 5*time.Second), // L1缓存3秒过期
		timelineRepo: timelineRepo,
		creatorRepo:  creatorRepo,
	}
}

// -----------------------------------------------------------------------------
// 【三级缓存核心函数】 GetVideoByIDs
// 数据流追踪：
//          ┌─────────────────────────────────────────────────────┐
//          │                  1000个并发请求                      │
//          └─────────────────────────────────────────────────────┘
//                              ↓ 全部命中
//                          L1 本地缓存 → 1ms返回
//                              ↓ 未命中
//                          L2 Redis MGet → 5ms返回 → 回填L1
//                              ↓ Redis挂了/未命中
//                          SingleFlight 合并请求 → 只查1次DB
//                              ↓
//                          MySQL查询 → 回填L2和L1
// -----------------------------------------------------------------------------

// GetVideoByIDs 批量获取视频详情（三级缓存 + SingleFlight防击穿）
// 这是整个Feed流的性能基石
func (f *FeedService) GetVideoByIDs(ctx context.Context, videoIDs []uint) ([]*video.Video, error) {
	if len(videoIDs) == 0 {
		return []*video.Video{}, nil
	}

	videoMap := make(map[uint]*video.Video)

	// =========================================================================
	// 【第一级】 L1 本地内存缓存（3秒 TTL）
	// 性能目标：1ms内返回，同一个进程3秒内不用查Redis
	// =========================================================================
	var missedL1 []uint // 记录L1没命中的视频ID
	for _, id := range videoIDs {
		cacheKey := fmt.Sprintf("video:entity:%d", id)
		if f.localcache != nil {
			if v, found := f.localcache.Get(cacheKey); found {
				if data, ok := v.(video.Video); ok {
					videoMap[id] = &data
					continue // ✅ L1命中，拿下一个
				}
			}
		}
		missedL1 = append(missedL1, id) // ❌ L1没命中
	}

	// 全部命中L1，直接返回
	if len(missedL1) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	// =========================================================================
	// 【第二级】 L2 Redis 分布式缓存
	// 性能目标：5ms返回，MGet批量查，一次拿所有没命中的
	// =========================================================================
	var missedL2 []uint
	if len(missedL1) > 0 {
		// 批量生成Redis key
		cacheKeys := make([]string, len(missedL1))
		for i, id := range missedL1 {
			cacheKeys[i] = fmt.Sprintf("video:entity:%d", id)
		}

		// Redis超时保护：超过50ms直接当Redis挂了
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		results, err := f.rediscache.MGet(cacheCtx, cacheKeys...)
		cancel()

		if err == nil {
			for i, res := range results {
				id := missedL1[i]
				if res != nil {
					if str, ok := res.(string); ok {
						var v video.Video
						if err := json.Unmarshal([]byte(str), &v); err == nil {
							videoMap[id] = &v
							// ✅ 命中Redis，顺便回填L1本地缓存
							if f.localcache != nil {
								f.localcache.Set(cacheKeys[i], v, 5*time.Second)
							}
							continue
						}
					}
				}
				missedL2 = append(missedL2, id) // ❌ Redis也没命中
			}
		} else {
			// 🚨 优雅降级：Redis挂了/超时了，全部直接查数据库
			// 设计哲学：缓存可以挂，用户不能刷不出视频
			missedL2 = missedL1
			log.Printf("L2 Redis挂了，全部降级到MySQL: %v", err)
		}
	}

	// 全部命中Redis，直接返回
	if len(missedL2) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	// =========================================================================
	// 【第三级】 L3 MySQL 数据库 + SingleFlight防击穿
	// 设计亮点：1000个goroutine同时查同一个视频，只有1个真的去查数据库！
	// =========================================================================
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, id := range missedL2 {
		wg.Add(1)
		go func(videoID uint) {
			defer wg.Done()

			// SingleFlight 合并相同请求
			sfKey := fmt.Sprintf("sf:entity:%d", videoID)
			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				// ==========================================
				// 这里面的代码，同一个key同时只有一个goroutine在跑
				// ==========================================
				videoList, err := f.repo.GetByIDs(ctx, []uint{videoID})
				if err != nil || len(videoList) == 0 {
					return nil, err
				}

				safeCopy := *videoList[0]

				// ✅ 查到了，异步回填Redis缓存
				cachekey := fmt.Sprintf("video:entity:%d", safeCopy.ID)
				if b, err := json.Marshal(safeCopy); err == nil {
					go func(k string, b []byte) {
						setCtx, setCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
						defer setCancel()
						f.rediscache.SetBytes(setCtx, k, b, cachelib.Jitter(time.Hour, 0.2))
					}(cachekey, b)
				}

				return videoList[0], err
			})

			// 查到了，回填L1本地缓存
			if err == nil && v != nil {
				safeCopy := *(v.(*video.Video))
				mu.Lock()
				videoMap[videoID] = &safeCopy
				mu.Unlock()
				if f.localcache != nil {
					f.localcache.Set(fmt.Sprintf("video:entity:%d", safeCopy.ID), safeCopy, 5*time.Second)
				}
			}
		}(id)
	}

	wg.Wait()
	return buildOrderedResult(videoIDs, videoMap), nil
}

// ListLatest 查询最新的视频列表，采用冷热分离 + 游标分页的策略。
// 热数据（最近视频）存放在 Redis ZSET 中，冷数据（较旧视频）降级查询 MySQL。
// 通过游标 latestBefore 实现顺序翻页，避免传统 offset 分页的数据重复/遗漏问题。
func (f *FeedService) ListLatest(ctx context.Context, limit int, latestBefore time.Time, latestID uint, viewerAccountID uint) (ListLatestResponse, error) {
	// ========== 第一步：检查 Redis ZSET 是否为空 ==========
	// 利用 ZRangeWithScores 取出 ZSET 中分数最小的那一条（即最老的视频）。
	// 范围 0,0 表示取下标 0 到 0 的元素（即第一个元素，因为 ZSET 按分数升序排列）。
	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, "feed:global_timeline", 0, 0)
	if err != nil {
		return ListLatestResponse{}, err
	}

	isZsetEmpty := len(zsetTail) == 0

	if isZsetEmpty {
		// ----- ZSET 为空：说明缓存被清空或尚未建立 -----
		// 使用全局静态锁（无视所有用户的不同时间戳游标）重建 ZSET。
		// 锁的 key 固定为 "sf:fallback:global_timeline_rebuild"，保证同一时间只允许一个请求去 MySQL 捞数据并重建缓存。
		sfKey := "sf:fallback:global_timeline_rebuild"

		// requestGroup 是 singleflight 的实现，防止高并发下大量请求同时去打 DB。
		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			// 直接无视游标，去 MySQL 捞最新的 1000 条数据作为 ZSET 的初始数据。
			// 这里传入 time.Time{} 表示不限制时间，取全局最新。
			dbVideos, err := f.repo.ListLatest(ctx, 1000, time.Time{}, 0)
			if err != nil {
				return nil, err
			}
			// 如果数据库也是空的，返回特殊标识 "EMPTY_DB"，防止无限递归。
			if len(dbVideos) == 0 {
				return "EMPTY_DB", nil
			}

			// 重建 ZSET
			bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var zElements []redis.Z
			for _, vid := range dbVideos {
				zElements = append(zElements, redis.Z{
					Score:  float64(vid.CreateTime.UnixMilli()), // 以视频创建时间（毫秒级时间戳）作为分数
					Member: fmt.Sprintf("%d", vid.ID),           // 成员为视频 ID 字符串
				})
			}
			// 将最新 1000 条视频写入 Redis ZSET
			f.rediscache.ZAdd(bgCtx, "feed:global_timeline", zElements...)
			return "SUCCESS", nil
		})

		if err != nil {
			return ListLatestResponse{}, err
		}
		// 数据库为空时，直接返回空列表，HasMore=false。
		if v == "EMPTY_DB" {
			return ListLatestResponse{HasMore: false}, nil
		}

		// 缓存重建成功后，当前请求重新调用自身（递归一次），此时 ZSET 已有数据，将进入正常热/冷分支。
		return f.ListLatest(ctx, limit, latestBefore, latestID, viewerAccountID)
	}

	// ========== 第二步：确定冷热分界线 watermark ==========
	// ZSET 尾部元素的分数（即最老视频的时间戳）就是冷热分界线。
	// 所有分数大于 watermark 的视频在热数据 ZSET 中，小于等于 watermark 的则需要去 MySQL 查。
	watermark := int64(zsetTail[0].Score)

	// 请求时间戳 reqTime：如果客户端传了游标 latestBefore，就用它；否则取当前时间。
	reqTime := time.Now().UnixMilli()
	if !latestBefore.IsZero() {
		reqTime = latestBefore.UnixMilli()
	}

	var baseVideos []*video.Video

	// ========== 第三步：根据请求时间与分界线的关系选择数据源 ==========
	if reqTime <= watermark {
		// 情况 A：冷数据区间 —— 请求的视频时间 <= 最老热数据的时间，说明需要查 MySQL。
		// 此时使用针对个别用户的防并发锁，因为冷尾流量极小，可以用 limit+reqTime 组成锁 key。
		sfKey := fmt.Sprintf("sf:cold:listLatest:%d:%d", limit, reqTime)
		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			return f.repo.ListLatest(ctx, limit, latestBefore, latestID)
		})
		if err != nil {
			return ListLatestResponse{}, err
		}
		baseVideos = v.([]*video.Video)
		// 注意：这里不回写 ZSET，防止冷数据污染热点时间线（避免将很旧的视频刷入缓存）。

	} else {
		// 情况 B：热数据区间 —— 请求时间 > 最老热数据的时间，先从 Redis ZSET 获取。
		// 使用 reqTime 作为上界（而不是 reqTime-1），再多取一些同分数据，随后按 (时间, ID) 过滤。
		maxScore := "+inf"
		if !latestBefore.IsZero() {
			maxScore = fmt.Sprintf("%d", reqTime)
		}

		// 多取 50 条，覆盖同一毫秒发布的多条视频。
		fetchLimit := int64(limit) + 50
		videoIDsStr, err := f.rediscache.ZRevRangeByScore(ctx, "feed:global_timeline", maxScore, "-inf", 0, fetchLimit)
		if err != nil {
			return ListLatestResponse{}, err
		}

		// 将字符串 ID 转为 uint 列表
		var videoIDs []uint
		for _, idStr := range videoIDsStr {
			if id, err := strconv.ParseUint(idStr, 10, 64); err == nil {
				videoIDs = append(videoIDs, uint(id))
			}
		}

		// 如果从 Redis 拿到了 ID，则批量查询视频详情（可能会走 video 的缓存或 DB）。
		if len(videoIDs) > 0 {
			baseVideos, err = f.GetVideoByIDs(ctx, videoIDs)
			if err != nil {
				return ListLatestResponse{}, err
			}
		}

		// 同一毫秒兜底游标：时间相同则按 ID 继续倒序。
		if !latestBefore.IsZero() {
			cursorMs := latestBefore.UnixMilli()
			filtered := make([]*video.Video, 0, len(baseVideos))
			for _, item := range baseVideos {
				itemMs := item.CreateTime.UnixMilli()
				if itemMs < cursorMs || (itemMs == cursorMs && (latestID == 0 || item.ID < latestID)) {
					filtered = append(filtered, item)
				}
			}
			baseVideos = filtered
		}
		if len(baseVideos) > limit {
			baseVideos = baseVideos[:limit]
		}

		// ========== 第四步：缝合冷热边界 ==========
		// 如果从 Redis 拿到的数量不足 limit 条，说明热数据不够，需要跨入冷数据区间补全。
		// 典型场景：ZSET 最后几页刚好跨过 watermark。
		if len(baseVideos) < limit {
			remainLimit := limit - len(baseVideos) // 还需要补多少条

			// 确定冷数据的游标：取当前已拿到但未返回的最后一条视频的创建时间和 ID。
			var coldCursor time.Time
			var coldCursorID uint
			if len(baseVideos) > 0 {
				last := baseVideos[len(baseVideos)-1]
				coldCursor = last.CreateTime
				coldCursorID = last.ID
			} else {
				coldCursor = latestBefore
				coldCursorID = latestID
			}

			// 用 singleflight 防止相同补全查询的并发。
			sfKey := fmt.Sprintf("sf:stitch:listLatest:%d:%d:%d", remainLimit, coldCursor.UnixMilli(), coldCursorID)
			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				return f.repo.ListLatest(ctx, remainLimit, coldCursor, coldCursorID)
			})

			if err == nil {
				coldVideos := v.([]*video.Video)
				// 将冷数据拼接到热数据后面
				baseVideos = append(baseVideos, coldVideos...)
			}
			// 注意：这里同样不回写 ZSET，避免污染热点缓存。
		}
	}

	// ========== 第五步：构建返回值 ==========
	var nextTime int64
	var nextID *uint
	if len(baseVideos) > 0 {
		// 取本页最后一条视频的时间戳（毫秒）和 ID 作为下一次请求的游标。
		last := baseVideos[len(baseVideos)-1]
		nextTime = last.CreateTime.UnixMilli()
		id := last.ID
		nextID = &id
	}
	// 判断是否还有更多数据：如果实际返回数量等于请求的 limit，则认为有下一页；否则没有。
	hasMore := len(baseVideos) == limit

	// 构建展示用的视频列表（可能填充用户信息、点赞状态等）。
	feedVideos, err := f.buildFeedVideos(ctx, baseVideos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}

	return ListLatestResponse{
		VideoList: feedVideos,
		NextTime:  nextTime,
		NextID:    nextID,
		HasMore:   hasMore,
	}, nil
}

// ListLikesCount 按照点赞数降序分页查询视频列表，基于游标分页。
// cursor 包含上一页的最后一个视频的点赞数和ID，用于定位下一页的起点。
// limit 指定每页返回的视频数量。
// viewerAccountID 用于标记当前用户是否对视频点赞。
// 返回的视频列表按点赞数从高到低排序。
func (f *FeedService) ListLikesCount(ctx context.Context, limit int, cursor *LikesCountCursor, viewerAccountID uint) (ListLikesCountResponse, error) {
	// 从数据库获取视频列表，使用游标实现高效分页
	videos, err := f.repo.ListLikesCountWithCursor(ctx, limit, cursor)
	if err != nil {
		return ListLikesCountResponse{}, err
	}

	// 判断是否还有更多数据：如果实际返回数等于 limit，则可能有下一页
	hasMore := len(videos) == limit

	// 构建包含用户点赞状态的视频列表
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListLikesCountResponse{}, err
	}

	resp := ListLikesCountResponse{
		VideoList: feedVideos,
		HasMore:   hasMore,
	}

	// 如果返回了视频，将最后一个视频的点赞数和ID作为下一页的游标返回给客户端
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextLikesCountBefore := last.LikesCount
		nextIDBefore := last.ID
		resp.NextLikesCountBefore = &nextLikesCountBefore
		resp.NextIDBefore = &nextIDBefore
	}

	return resp, nil
}

// ListByFollowing 获取当前用户关注的人发布的视频，按时间倒序分页。
//
// 优先走推拉结合时间线：
//   - 读自己的 Inbox（小 V 写扩散的内容）
//   - 批量读关注的大 V Outbox
//   - 合并自己的 Outbox，保证发布后自己能看到
//
// Redis 不可用或时间线为空时降级到 MySQL 子查询。
func (f *FeedService) ListByFollowing(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListByFollowingResponse, error) {
	if viewerAccountID == 0 {
		return ListByFollowingResponse{}, nil
	}

	if f.timelineRepo != nil && f.creatorRepo != nil {
		resp, err := f.listByFollowingHybrid(ctx, limit, latestBefore, viewerAccountID)
		if err == nil {
			return resp, nil
		}
		log.Printf("hybrid following feed failed, fallback to MySQL: %v", err)
	}

	return f.listByFollowingFromDB(ctx, limit, latestBefore, viewerAccountID)
}

// listByFollowingHybrid 合并 Inbox、大 V Outbox 和自己的 Outbox。
func (f *FeedService) listByFollowingHybrid(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListByFollowingResponse, error) {
	cursorMs := int64(0)
	if !latestBefore.IsZero() {
		cursorMs = latestBefore.UnixMilli()
	}

	stats, err := f.creatorRepo.ListFollowing(ctx, viewerAccountID)
	if err != nil {
		return ListByFollowingResponse{}, err
	}

	bigVAuthorIDs := make([]uint, 0)
	for _, item := range stats {
		if item.IsBig || item.FansCount >= domaincreator.BigVFansThreshold {
			bigVAuthorIDs = append(bigVAuthorIDs, item.AuthorID)
		}
	}
	// 限制单次合并的大 V 数量，避免关注几百个大 V 时读放大失控。
	const maxBigVAuthors = 100
	if len(bigVAuthorIDs) > maxBigVAuthors {
		bigVAuthorIDs = bigVAuthorIDs[:maxBigVAuthors]
	}

	entries, err := f.timelineRepo.ReadInbox(ctx, viewerAccountID, cursorMs, limit)
	if err != nil {
		return ListByFollowingResponse{}, err
	}

	if len(bigVAuthorIDs) > 0 {
		groups, err := f.timelineRepo.ReadOutboxes(ctx, bigVAuthorIDs, cursorMs, limit)
		if err != nil {
			return ListByFollowingResponse{}, err
		}
		for _, group := range groups {
			entries = append(entries, group...)
		}
	}

	// 合并自己的发件箱，保证作者发布后能立刻在自己的关注流看到。
	ownEntries, err := f.timelineRepo.ReadOutbox(ctx, viewerAccountID, cursorMs, limit)
	if err != nil {
		return ListByFollowingResponse{}, err
	}
	entries = append(entries, ownEntries...)

	merged := mergeTimelineEntries(entries, limit)
	if len(merged) == 0 {
		// 第一页没有缓存时回源数据库，兼顾新用户和历史数据。
		if latestBefore.IsZero() {
			return f.listByFollowingFromDB(ctx, limit, latestBefore, viewerAccountID)
		}
		return ListByFollowingResponse{VideoList: []FeedVideoItem{}, HasMore: false}, nil
	}

	videoIDs := make([]uint, 0, len(merged))
	for _, entry := range merged {
		videoIDs = append(videoIDs, entry.VideoID)
	}
	videos, err := f.GetVideoByIDs(ctx, videoIDs)
	if err != nil {
		return ListByFollowingResponse{}, err
	}
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListByFollowingResponse{}, err
	}

	var nextTime int64
	if len(feedVideos) > 0 {
		nextTime = feedVideos[len(feedVideos)-1].CreateTime
	}
	return ListByFollowingResponse{
		VideoList: feedVideos,
		NextTime:  nextTime,
		HasMore:   len(feedVideos) == limit,
	}, nil
}

// listByFollowingFromDB 是推拉结合不可用时的降级路径。
func (f *FeedService) listByFollowingFromDB(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListByFollowingResponse, error) {
	videos, err := f.repo.ListByFollowing(ctx, limit, viewerAccountID, latestBefore)
	if err != nil {
		return ListByFollowingResponse{}, err
	}

	var nextTime int64
	if len(videos) > 0 {
		nextTime = videos[len(videos)-1].CreateTime.Unix()
	}
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListByFollowingResponse{}, err
	}
	return ListByFollowingResponse{
		VideoList: feedVideos,
		NextTime:  nextTime,
		HasMore:   len(videos) == limit,
	}, nil
}

// mergeTimelineEntries 对多个来源的时间线去重并按时间倒序排序。
func mergeTimelineEntries(entries []domainfeed.TimelineEntry, limit int) []domainfeed.TimelineEntry {
	byVideoID := make(map[uint]int64, len(entries))
	for _, entry := range entries {
		if entry.VideoID == 0 {
			continue
		}
		if existing, ok := byVideoID[entry.VideoID]; !ok || entry.Score > existing {
			byVideoID[entry.VideoID] = entry.Score
		}
	}

	merged := make([]domainfeed.TimelineEntry, 0, len(byVideoID))
	for videoID, score := range byVideoID {
		merged = append(merged, domainfeed.TimelineEntry{VideoID: videoID, Score: score})
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Score == merged[j].Score {
			return merged[i].VideoID > merged[j].VideoID
		}
		return merged[i].Score > merged[j].Score
	})
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

// ListByPopularity 按热度（Popularity）降序查询视频列表，支持分页。
//
// 热度采用 24 个小时桶 + 线性权重衰减：
//   - 当前小时权重 1.0
//   - 越早的小时权重越低，最老小时权重 1/24
//   - 合并结果写入快照，保证同一 as_of 下翻页稳定
//
// Redis 不可用或数据不存在时回退到数据库三字段游标分页。
func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, latestPopularity int64, latestBefore time.Time, latestIDBefore uint) (ListByPopularityResponse, error) {
	// 优先读取 Redis 热榜
	if f.rediscache != nil {
		// 按小时截断，提供稳定的翻页视图。
		asOf := time.Now().UTC().Truncate(time.Hour)
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Hour)
		}

		const windowHours = 24
		keys := hotWindowKeys(asOf, windowHours)
		weights := linearDecayWeights(windowHours)

		// 合并快照键，避免每次翻页都重新 ZUnionStore。
		dest := "hot:video:merge:1h:" + asOf.Format("2006010215")
		opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()

		exists, _ := f.rediscache.Exists(opCtx, dest)
		if !exists {
			_ = f.rediscache.ZUnionStoreWeighted(opCtx, dest, keys, weights, "SUM")
			_ = f.rediscache.Expire(opCtx, dest, 10*time.Minute)
		}

		// 从合并结果中按热度倒序分页获取视频ID
		start := int64(offset)
		stop := start + int64(limit) - 1
		members, err := f.rediscache.ZRevRange(opCtx, dest, start, stop)
		// 如果偏移量大于0但结果为空，说明数据已被翻完，直接返回空列表
		if err == nil && len(members) == 0 {
			if offset > 0 {
				return ListByPopularityResponse{
					VideoList:  []FeedVideoItem{},
					AsOf:       asOf.Unix(),
					NextOffset: offset,
					HasMore:    false,
				}, nil
			}
		}
		// 成功获取到ID列表，按顺序查询视频详细信息
		if err == nil && len(members) > 0 {
			ids := make([]uint, 0, len(members))
			for _, m := range members {
				u, err := strconv.ParseUint(m, 10, 64)
				if err == nil && u > 0 {
					ids = append(ids, uint(u))
				}
			}

			videos, err := f.repo.GetByIDs(ctx, ids)
			if err == nil {
				// 构建ID到视频的映射，并按 Redis 返回的顺序排列结果
				byID := make(map[uint]*video.Video, len(videos))
				for _, v := range videos {
					byID[v.ID] = v
				}
				ordered := make([]*video.Video, 0, len(ids))
				for _, id := range ids {
					if v := byID[id]; v != nil {
						ordered = append(ordered, v)
					}
				}

				// 补充点赞信息
				items, err := f.buildFeedVideos(ctx, ordered, viewerAccountID)
				if err != nil {
					return ListByPopularityResponse{}, err
				}

				resp := ListByPopularityResponse{
					VideoList:  items,
					AsOf:       asOf.Unix(),         // 返回本次快照的时间戳，便于客户端续翻
					NextOffset: offset + len(items), // 下一页的偏移量
					HasMore:    len(items) == limit,
				}
				// 设置数据库游标字段，即使主要分页用 offset，也返回这些以兼容旧客户端或回退
				if len(ordered) > 0 {
					last := ordered[len(ordered)-1]
					nextPopularity := last.Popularity
					nextBefore := last.CreateTime
					nextID := last.ID
					resp.NextLatestPopularity = &nextPopularity
					resp.NextLatestBefore = &nextBefore
					resp.NextLatestIDBefore = &nextID
				}
				return resp, nil
			}
		}
	}

	// Redis 数据不可用或处理失败，回退到数据库查询（使用传统游标分页）
	videos, err := f.repo.ListByPopularity(ctx, limit, latestPopularity, latestBefore, latestIDBefore)
	if err != nil {
		return ListByPopularityResponse{}, err
	}

	items, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListByPopularityResponse{}, err
	}

	resp := ListByPopularityResponse{
		VideoList:  items,
		AsOf:       0, // 标记为数据库直接查询
		NextOffset: 0, // 数据库分页不使用 offset
		HasMore:    len(items) == limit,
	}
	// 返回下一页的游标
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextPopularity := last.Popularity
		nextBefore := last.CreateTime
		nextID := last.ID
		resp.NextLatestPopularity = &nextPopularity
		resp.NextLatestBefore = &nextBefore
		resp.NextLatestIDBefore = &nextID
	}
	return resp, nil
}

// hotWindowKeys 生成从 asOf 往前共 hours 个小时桶的 key。
func hotWindowKeys(asOf time.Time, hours int) []string {
	if hours <= 0 {
		return nil
	}
	keys := make([]string, 0, hours)
	for i := 0; i < hours; i++ {
		keys = append(keys, "hot:video:1h:"+asOf.Add(-time.Duration(i)*time.Hour).Format("2006010215"))
	}
	return keys
}

// linearDecayWeights 生成线性衰减权重：最新小时为 1.0，最老小时为 1/hours。
func linearDecayWeights(hours int) []float64 {
	if hours <= 0 {
		return nil
	}
	weights := make([]float64, hours)
	for i := 0; i < hours; i++ {
		weights[i] = float64(hours-i) / float64(hours)
	}
	return weights
}

// buildFeedVideos 根据视频实体列表构建前端使用的 FeedVideoItem 列表，
// 会批量查询当前用户是否点赞了这些视频，并填充点赞数等信息。
func (f *FeedService) buildFeedVideos(ctx context.Context, videos []*video.Video, viewerAccountID uint) ([]FeedVideoItem, error) {
	feedVideos := make([]FeedVideoItem, 0, len(videos))

	// 收集所有视频的ID，用于批量查询点赞状态
	videoIDs := make([]uint, len(videos))
	for i, v := range videos {
		videoIDs[i] = v.ID
	}

	// 批量获取当前用户对这些视频的点赞状态
	likedMap, err := f.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
	if err != nil {
		return nil, err
	}

	// 组装最终的返回结构
	for _, video := range videos {
		feedVideos = append(feedVideos, FeedVideoItem{
			ID:          video.ID,
			Author:      FeedAuthor{ID: video.AuthorID, Username: video.Username},
			Title:       video.Title,
			Description: video.Description,
			PlayURL:     video.PlayURL,
			CoverURL:    video.CoverURL,
			CreateTime:  video.CreateTime.Unix(),
			LikesCount:  video.LikesCount,
			IsLiked:     likedMap[video.ID], // 使用批量查询的结果标记是否已点赞
		})
	}
	return feedVideos, nil
}

// buildOrderedResult 按照 orderedIDs 的顺序从 dataMap 中提取视频实体，
// 确保结果顺序与给定的ID列表一致，忽略在 map 中不存在的ID。
func buildOrderedResult(orderedIDs []uint, dataMap map[uint]*video.Video) []*video.Video {
	res := make([]*video.Video, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if v, exits := dataMap[id]; exits && v != nil {
			res = append(res, v)
		}
	}
	return res
}
