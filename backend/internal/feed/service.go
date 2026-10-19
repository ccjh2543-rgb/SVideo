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
	cacheTTL     time.Duration      // 缓存过期时间
	requestGroup singleflight.Group // 防缓存击穿神器：1000个并发查同一条，只有1个查DB
}

// CachedFeedData Redis中缓存的视频流数据结构
type CachedFeedData struct {
	PublicVideos []video.Video `json:"public_videos"`
}

// NewFeedService 创建Feed流服务（依赖注入）
func NewFeedService(repo *FeedRepository, likeRepo *video.LikeRepository, rediscache *rediscache.Client) *FeedService {
	return &FeedService{
		repo:       repo,
		likeRepo:   likeRepo,
		rediscache: rediscache,
		localcache: cache.New(3*time.Second, 5*time.Second), // L1缓存3秒过期
		cacheTTL:   24 * time.Hour,
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
						f.rediscache.SetBytes(setCtx, k, b, time.Hour)
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
func (f *FeedService) ListLatest(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListLatestResponse, error) {
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
			dbVideos, err := f.repo.ListLatest(ctx, 1000, time.Time{})
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
		return f.ListLatest(ctx, limit, latestBefore, viewerAccountID)
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
			return f.repo.ListLatest(ctx, limit, latestBefore)
		})
		if err != nil {
			return ListLatestResponse{}, err
		}
		baseVideos = v.([]*video.Video)
		// 注意：这里不回写 ZSET，防止冷数据污染热点时间线（避免将很旧的视频刷入缓存）。

	} else {
		// 情况 B：热数据区间 —— 请求时间 > 最老热数据的时间，先从 Redis ZSET 获取。
		// 如果客户端传了游标，maxScore 设为 reqTime-1，避免重复返回游标时间点的那条视频。
		maxScore := "+inf"
		if !latestBefore.IsZero() {
			maxScore = fmt.Sprintf("%d", reqTime-1)
		}

		// ZRevRangeByScore 按分数从高到低（最新到最老）返回 limit 条视频 ID。
		videoIDsStr, err := f.rediscache.ZRevRangeByScore(ctx, "feed:global_timeline", maxScore, "-inf", 0, int64(limit))
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

		// ========== 第四步：缝合冷热边界 ==========
		// 如果从 Redis 拿到的数量不足 limit 条，说明热数据不够，需要跨入冷数据区间补全。
		// 典型场景：ZSET 最后几页刚好跨过 watermark。
		if len(baseVideos) < limit {
			remainLimit := limit - len(baseVideos) // 还需要补多少条

			// 确定冷数据的游标：取当前已拿到但未返回的最后一条视频的创建时间作为下次查询的起点。
			var coldCursor time.Time
			if len(baseVideos) > 0 {
				coldCursor = baseVideos[len(baseVideos)-1].CreateTime
			} else {
				coldCursor = latestBefore
			}

			// 用 singleflight 防止相同补全查询的并发。
			sfKey := fmt.Sprintf("sf:stitch:listLatest:%d:%d", remainLimit, coldCursor.UnixMilli())
			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				return f.repo.ListLatest(ctx, remainLimit, coldCursor)
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
	if len(baseVideos) > 0 {
		// 取本页最后一条视频的时间戳（毫秒）作为下一次请求的游标（NextTime）。
		nextTime = baseVideos[len(baseVideos)-1].CreateTime.UnixMilli()
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
// limit 指定每页数量。
// latestBefore 用于游标分页，表示上一页最后一条视频的创建时间，本页返回此时间之前的视频。
// viewerAccountID 用于确定关注列表以及标记点赞状态。
// 此方法包含 Redis 缓存逻辑，以加速热门查询。
func (f *FeedService) ListByFollowing(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListByFollowingResponse, error) {
	// 实际从数据库查询并构建响应的函数，避免重复代码
	doListByFollowingFromDB := func() (ListByFollowingResponse, error) {
		videos, err := f.repo.ListByFollowing(ctx, limit, viewerAccountID, latestBefore)
		if err != nil {
			return ListByFollowingResponse{}, err
		}

		// 计算下一页的游标：最后一条视频的创建时间戳
		var nextTime int64
		if len(videos) > 0 {
			nextTime = videos[len(videos)-1].CreateTime.Unix()
		} else {
			nextTime = 0
		}

		hasMore := len(videos) == limit

		// 为视频列表补充点赞信息
		feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
		if err != nil {
			return ListByFollowingResponse{}, err
		}

		return ListByFollowingResponse{
			VideoList: feedVideos,
			NextTime:  nextTime,
			HasMore:   hasMore,
		}, nil
	}

	// 缓存逻辑：仅当用户已登录且缓存可用时尝试读取缓存
	var cacheKey string
	if viewerAccountID != 0 && f.rediscache != nil {
		before := int64(0)
		if !latestBefore.IsZero() {
			before = latestBefore.Unix()
		}
		// 缓存键由 limit、用户ID和游标组成，保证缓存命中正确
		cacheKey = fmt.Sprintf("feed:listByFollowing:limit=%d:accountID=%d:before=%d", limit, viewerAccountID, before)
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		// 尝试从缓存直接获取
		b, err := f.rediscache.GetBytes(cacheCtx, cacheKey)
		if err == nil {
			var cached ListByFollowingResponse
			if err := json.Unmarshal(b, &cached); err == nil {
				return cached, nil // 缓存命中，直接返回
			}
		} else if rediscache.IsMiss(err) { // 缓存未命中，需要防缓存击穿
			lockKey := "lock:" + cacheKey
			// 尝试获取分布式锁，锁的超时时间为500ms
			token, locked, _ := f.rediscache.Lock(cacheCtx, lockKey, 500*time.Millisecond)
			if locked {
				// 获得锁后，再次检查缓存（可能其他协程已写）
				defer func() { _ = f.rediscache.Unlock(context.Background(), lockKey, token) }()
				if b, err := f.rediscache.GetBytes(cacheCtx, cacheKey); err == nil {
					var cached ListByFollowingResponse
					if err := json.Unmarshal(b, &cached); err == nil {
						return cached, nil
					}
				} else { // 确认缓存确实未命中，从数据库加载并回写缓存
					resp, err := doListByFollowingFromDB()
					if err != nil {
						return ListByFollowingResponse{}, err
					}
					if b, err := json.Marshal(resp); err == nil {
						_ = f.rediscache.SetBytes(cacheCtx, cacheKey, b, f.cacheTTL)
					}
					return resp, nil
				}
			} else {
				// 未获取锁，等待一小段时间并重试读取缓存（最多5次）
				for i := 0; i < 5; i++ {
					time.Sleep(20 * time.Millisecond)
					if b, err := f.rediscache.GetBytes(cacheCtx, cacheKey); err == nil {
						var cached ListByFollowingResponse
						if err := json.Unmarshal(b, &cached); err == nil {
							return cached, nil
						}
					}
				}
			}
		}
		// 如果缓存存在但解码失败或其它错误，继续走数据库查询
	}

	// 无缓存或缓存未命中/出错，从数据库查询
	resp, err := doListByFollowingFromDB()
	if err != nil {
		return ListByFollowingResponse{}, err
	}

	// 如果启用了缓存，则将数据库结果写入缓存
	if cacheKey != "" {
		if b, err := json.Marshal(resp); err == nil {
			cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			_ = f.rediscache.SetBytes(cacheCtx, cacheKey, b, f.cacheTTL)
		}
	}
	return resp, nil
}

// ListByPopularity 按热度（Popularity）降序查询视频列表，支持分页。
// 该方法优先使用 Redis 合并缓存的热度排行榜（1分钟粒度，60分钟窗口），
// 如果 Redis 不可用或数据不存在则回退到数据库查询。
// reqAsOf：请求的时间锚点，用于确定 Redis 快照版本，提供稳定翻页体验。
// offset：Redis 分页时的起始偏移量。
// latestPopularity, latestBefore, latestIDBefore：用于数据库分页的游标（三位一体），确保准确翻页。
func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, latestPopularity int64, latestBefore time.Time, latestIDBefore uint) (ListByPopularityResponse, error) {
	// 优先读取 Redis 热榜
	if f.rediscache != nil {
		// 确定快照时间：按分钟截断，以提供稳定的翻页视图
		asOf := time.Now().UTC().Truncate(time.Minute)
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Minute)
		}

		// 构建最近60分钟的热度窗口（60个1分钟ZSet的键）
		const win = 60
		keys := make([]string, 0, win)
		for i := 0; i < win; i++ {
			keys = append(keys, "hot:video:1m:"+asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504"))
		}

		// 合并的快照键，用于缓存合并结果，避免频繁 ZUnionStore
		dest := "hot:video:merge:1m:" + asOf.Format("200601021504")
		opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()

		// 检查合并快照是否存在，若不存在则执行合并并设置过期时间
		exists, _ := f.rediscache.Exists(opCtx, dest)
		if !exists {
			_ = f.rediscache.ZUnionStore(opCtx, dest, keys, "SUM")
			_ = f.rediscache.Expire(opCtx, dest, 2*time.Minute) // 为后续翻页保留足够长的时间
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
