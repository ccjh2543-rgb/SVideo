// Package feed 是 Feed 上下文应用层，负责最新流、点赞排行、关注流和热度流的用例编排。
package feed

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainsocial "feedsystem_video_go/internal/domain/social"
	domainvideo "feedsystem_video_go/internal/domain/video"

	"golang.org/x/sync/singleflight"
)

const (
	// followingHybridBudget 关注流读 Redis 的预算，超时直接走数据库降级。
	followingHybridBudget = 120 * time.Millisecond
	// followingDBBudget 数据库降级查询预算，避免慢查询拖垮请求。
	followingDBBudget = 300 * time.Millisecond
	// dbFallbackConcurrency 数据库降级路径的最大并发数，保护 MySQL。
	dbFallbackConcurrency = 50
	// breakerRedisFeed 时间线/最新流 Redis 熔断器名称。
	breakerRedisFeed = "redis-feed"
	// breakerRedisPopularity 热榜 Redis 熔断器名称。
	breakerRedisPopularity = "redis-popularity"
	// defaultBigVBatchSize 大 V Outbox 单批读取数量。
	defaultBigVBatchSize = 32
	// defaultBigVMergeBudget 大 V 归并的时间预算。
	defaultBigVMergeBudget = 50 * time.Millisecond
	// heavyMigrationBackfillLimit 存量重度用户懒迁移时每个大 V 回填的条数。
	heavyMigrationBackfillLimit = 20
	// heavyMigrationTimeout 存量重度用户懒迁移的总预算，避免后台任务无限挂起。
	heavyMigrationTimeout = 2 * time.Minute
)

// errTimelineEmpty 表示时间线中没有任何可用数据，需要回源数据库。
var errTimelineEmpty = errors.New("following timeline is empty")

// errCircuitOpen 表示依赖熔断已打开。
var errCircuitOpen = errors.New("dependency circuit is open")

// FeedService Feed 流应用服务。
//
// 三级缓存策略：
//
//	L1 本地内存（3s）→ L2 Redis → L3 MySQL（SingleFlight 防击穿）
type FeedService struct {
	repo            domainfeed.FeedRepository
	likeRepo        domainvideo.LikeRepository
	videoCache      VideoCache
	ranking         RankingCache
	timelineRepo    domainfeed.TimelineRepository
	creatorRepo     domainsocial.StatsRepository
	followingCache  FollowingCache
	heavyUserCache  HeavyUserCache
	guard           DependencyGuard
	dbFallback      chan struct{}
	bigVBatchSize   int
	bigVMergeBudget time.Duration
	requestGroup    singleflight.Group
	heavyMigrating  sync.Map
}

// NewFeedService 创建 Feed 应用服务。
func NewFeedService(
	repo domainfeed.FeedRepository,
	likeRepo domainvideo.LikeRepository,
	videoCache VideoCache,
	ranking RankingCache,
	timelineRepo domainfeed.TimelineRepository,
	creatorRepo domainsocial.StatsRepository,
	followingCache FollowingCache,
	heavyUserCache HeavyUserCache,
	guard DependencyGuard,
) *FeedService {
	return &FeedService{
		repo:            repo,
		likeRepo:        likeRepo,
		videoCache:      videoCache,
		ranking:         ranking,
		timelineRepo:    timelineRepo,
		creatorRepo:     creatorRepo,
		followingCache:  followingCache,
		heavyUserCache:  heavyUserCache,
		guard:           guard,
		dbFallback:      make(chan struct{}, dbFallbackConcurrency),
		bigVBatchSize:   defaultBigVBatchSize,
		bigVMergeBudget: defaultBigVMergeBudget,
	}
}

// getVideoByIDs 批量获取视频实体，依次经过 L1、L2、L3，并在回源时合并并发请求。
func (s *FeedService) getVideoByIDs(ctx context.Context, videoIDs []uint) ([]*domainvideo.Video, error) {
	if len(videoIDs) == 0 {
		return []*domainvideo.Video{}, nil
	}

	videoMap := make(map[uint]*domainvideo.Video)

	// L1 本地缓存
	missedL1 := make([]uint, 0, len(videoIDs))
	for _, id := range videoIDs {
		if s.videoCache != nil {
			if v, ok := s.videoCache.GetLocal(id); ok {
				vv := v
				videoMap[id] = &vv
				continue
			}
		}
		missedL1 = append(missedL1, id)
	}
	if len(missedL1) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	// L2 Redis 批量缓存，Redis 异常时整体降级到 MySQL
	missedL2 := make([]uint, 0, len(missedL1))
	if s.videoCache != nil {
		hits, err := s.videoCache.MGet(ctx, missedL1)
		if err == nil {
			for _, id := range missedL1 {
				if v, ok := hits[id]; ok {
					vv := v
					videoMap[id] = &vv
					s.videoCache.SetLocal(id, vv)
					continue
				}
				missedL2 = append(missedL2, id)
			}
		} else {
			log.Printf("L2 Redis 不可用，视频详情降级到 MySQL: %v", err)
			missedL2 = missedL1
		}
	} else {
		missedL2 = missedL1
	}
	if len(missedL2) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	// L3 MySQL + SingleFlight
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, id := range missedL2 {
		wg.Add(1)
		go func(videoID uint) {
			defer wg.Done()

			sfKey := fmt.Sprintf("sf:entity:%d", videoID)
			v, err, _ := s.requestGroup.Do(sfKey, func() (interface{}, error) {
				videoList, err := s.repo.GetByIDs(ctx, []uint{videoID})
				if err != nil || len(videoList) == 0 {
					return nil, err
				}
				safeCopy := *videoList[0]
				if s.videoCache != nil {
					// 异步回填 L2，避免阻塞当前请求
					s.videoCache.Set(ctx, safeCopy)
				}
				return videoList[0], nil
			})
			if err != nil || v == nil {
				return
			}

			safeCopy := *(v.(*domainvideo.Video))
			mu.Lock()
			videoMap[videoID] = &safeCopy
			mu.Unlock()
			if s.videoCache != nil {
				s.videoCache.SetLocal(safeCopy.ID, safeCopy)
			}
		}(id)
	}
	wg.Wait()
	return buildOrderedResult(videoIDs, videoMap), nil
}

// ListLatest 查询最新视频流，采用 Redis 热数据 + MySQL 冷数据缝合的游标分页策略。
func (s *FeedService) ListLatest(ctx context.Context, limit int, latestBefore time.Time, latestID uint, viewerAccountID uint) (LatestResult, error) {
	return s.listLatest(ctx, limit, latestBefore, latestID, viewerAccountID, true)
}

// listLatest 是最新流的内部实现。
// allowRebuild 为 false 表示已经尝试过重建时间线，此时 Redis 仍不可用则直接查 MySQL，
// 避免在缓存禁用时无限递归。
func (s *FeedService) listLatest(ctx context.Context, limit int, latestBefore time.Time, latestID uint, viewerAccountID uint, allowRebuild bool) (LatestResult, error) {
	// Redis 熔断或不可用时直接查库，避免每个请求都等待超时。
	if s.ranking == nil || (s.guard != nil && !s.guard.Allow(breakerRedisFeed)) {
		return s.listLatestFromDB(ctx, limit, latestBefore, latestID, viewerAccountID)
	}

	watermark, hasTimeline, err := s.ranking.GlobalTimelineTail(ctx)
	if err != nil {
		s.recordDependency(breakerRedisFeed, err)
		log.Printf("global timeline tail failed, fallback to MySQL: %v", err)
		return s.listLatestFromDB(ctx, limit, latestBefore, latestID, viewerAccountID)
	}
	s.recordDependency(breakerRedisFeed, nil)

	if !hasTimeline && !allowRebuild {
		return s.listLatestFromDB(ctx, limit, latestBefore, latestID, viewerAccountID)
	}

	if !hasTimeline {
		// 时间线为空时只允许一个请求回源重建
		sfKey := "sf:fallback:global_timeline_rebuild"
		v, err, _ := s.requestGroup.Do(sfKey, func() (interface{}, error) {
			dbVideos, err := s.repo.ListLatest(ctx, 1000, time.Time{}, 0)
			if err != nil {
				return nil, err
			}
			if len(dbVideos) == 0 {
				return "EMPTY_DB", nil
			}
			bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := s.ranking.RebuildGlobalTimeline(bgCtx, dbVideos); err != nil {
				return "REBUILD_FAILED", nil
			}
			return "SUCCESS", nil
		})
		if err != nil {
			return s.listLatestFromDB(ctx, limit, latestBefore, latestID, viewerAccountID)
		}
		if v == "EMPTY_DB" {
			return LatestResult{HasMore: false}, nil
		}
		if v == "REBUILD_FAILED" {
			return s.listLatestFromDB(ctx, limit, latestBefore, latestID, viewerAccountID)
		}
		// 重建成功后重新进入正常冷热分支；再次为空则降级到 MySQL
		return s.listLatest(ctx, limit, latestBefore, latestID, viewerAccountID, false)
	}

	reqTime := time.Now().UnixMilli()
	if !latestBefore.IsZero() {
		reqTime = latestBefore.UnixMilli()
	}

	var baseVideos []*domainvideo.Video

	if reqTime <= watermark {
		// 冷数据区间：直接查 MySQL，不回写时间线，避免污染热数据
		sfKey := fmt.Sprintf("sf:cold:listLatest:%d:%d", limit, reqTime)
		v, err, _ := s.requestGroup.Do(sfKey, func() (interface{}, error) {
			return s.repo.ListLatest(ctx, limit, latestBefore, latestID)
		})
		if err != nil {
			return LatestResult{}, err
		}
		baseVideos = v.([]*domainvideo.Video)
	} else {
		// 热数据区间：先从 ZSET 多取一些 ID，覆盖同毫秒发布的情况
		maxScore := int64(0) // 0 表示 +inf
		if !latestBefore.IsZero() {
			maxScore = reqTime
		}
		videoIDs, err := s.ranking.LatestIDs(ctx, maxScore, limit+50)
		if err != nil {
			s.recordDependency(breakerRedisFeed, err)
			log.Printf("latest timeline read failed, fallback to MySQL: %v", err)
			return s.listLatestFromDB(ctx, limit, latestBefore, latestID, viewerAccountID)
		}
		s.recordDependency(breakerRedisFeed, nil)
		if len(videoIDs) > 0 {
			baseVideos, err = s.getVideoByIDs(ctx, videoIDs)
			if err != nil {
				return LatestResult{}, err
			}
		}

		// 同一毫秒用 ID 继续倒序过滤
		if !latestBefore.IsZero() {
			filtered := make([]*domainvideo.Video, 0, len(baseVideos))
			for _, item := range baseVideos {
				itemMs := item.CreateTime.UnixMilli()
				if itemMs < reqTime || (itemMs == reqTime && (latestID == 0 || item.ID < latestID)) {
					filtered = append(filtered, item)
				}
			}
			baseVideos = filtered
		}
		if len(baseVideos) > limit {
			baseVideos = baseVideos[:limit]
		}

		// 热数据不足时跨入冷数据区间补全
		if len(baseVideos) < limit {
			remainLimit := limit - len(baseVideos)
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

			sfKey := fmt.Sprintf("sf:stitch:listLatest:%d:%d:%d", remainLimit, coldCursor.UnixMilli(), coldCursorID)
			v, err, _ := s.requestGroup.Do(sfKey, func() (interface{}, error) {
				return s.repo.ListLatest(ctx, remainLimit, coldCursor, coldCursorID)
			})
			if err == nil {
				baseVideos = append(baseVideos, v.([]*domainvideo.Video)...)
			}
		}
	}

	return s.buildLatestResult(ctx, baseVideos, limit, viewerAccountID)
}

// listLatestFromDB 最新流的数据库降级路径。
func (s *FeedService) listLatestFromDB(ctx context.Context, limit int, latestBefore time.Time, latestID uint, viewerAccountID uint) (LatestResult, error) {
	videos, err := s.repo.ListLatest(ctx, limit, latestBefore, latestID)
	if err != nil {
		return LatestResult{}, err
	}
	return s.buildLatestResult(ctx, videos, limit, viewerAccountID)
}

// buildLatestResult 组装最新流返回值，游标取本页最后一条视频的毫秒时间和 ID。
func (s *FeedService) buildLatestResult(ctx context.Context, videos []*domainvideo.Video, limit int, viewerAccountID uint) (LatestResult, error) {
	var nextTime int64
	var nextID *uint
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextTime = last.CreateTime.UnixMilli()
		id := last.ID
		nextID = &id
	}

	items, err := s.buildFeedItems(ctx, videos, viewerAccountID)
	if err != nil {
		return LatestResult{}, err
	}
	return LatestResult{
		VideoList: items,
		NextTime:  nextTime,
		NextID:    nextID,
		HasMore:   len(videos) == limit,
	}, nil
}

// ListLikesCount 按点赞数倒序分页查询视频列表。
func (s *FeedService) ListLikesCount(ctx context.Context, limit int, cursor *domainfeed.LikesCountCursor, viewerAccountID uint) (LikesCountResult, error) {
	videos, err := s.repo.ListLikesCountWithCursor(ctx, limit, cursor)
	if err != nil {
		return LikesCountResult{}, err
	}

	items, err := s.buildFeedItems(ctx, videos, viewerAccountID)
	if err != nil {
		return LikesCountResult{}, err
	}

	resp := LikesCountResult{
		VideoList: items,
		HasMore:   len(videos) == limit,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextLikesCountBefore := last.LikesCount
		nextIDBefore := last.ID
		resp.NextLikesCountBefore = &nextLikesCountBefore
		resp.NextIDBefore = &nextIDBefore
	}
	return resp, nil
}

// ListByFollowing 获取关注流。
// 优先读推拉结合时间线（Inbox + 大 V Outbox + 自己 Outbox），失败时降级到 MySQL。
// latestID 与 latestBefore 组成 (毫秒时间, 视频ID) 双游标，保证同一毫秒内的视频翻页不重不漏。
func (s *FeedService) ListByFollowing(ctx context.Context, limit int, latestBefore time.Time, latestID uint, viewerAccountID uint) (FollowingResult, error) {
	if viewerAccountID == 0 {
		return FollowingResult{}, nil
	}

	if s.timelineRepo != nil && s.creatorRepo != nil {
		// 重度用户的大 V 内容已经通过选择性推进入 Inbox，读路径可以跳过 Outbox 拉取。
		heavy := false
		if s.heavyUserCache != nil {
			value, err := s.heavyUserCache.IsHeavy(ctx, viewerAccountID)
			if err != nil {
				log.Printf("following feed: read heavy flag failed user=%d: %v", viewerAccountID, err)
			} else {
				heavy = value
			}
			// 重度标记已存在但历史内容还没回填完时，继续走普通推拉路径，
			// 避免读路径提前跳过已登记大 V 的 Outbox 造成内容断层。
			if heavy {
				backfilled, err := s.heavyUserCache.IsBackfilled(ctx, viewerAccountID)
				if err != nil {
					log.Printf("following feed: read heavy backfill failed user=%d: %v", viewerAccountID, err)
					heavy = false
				} else if !backfilled {
					heavy = false
				}
			}
		}

		// Redis 阶段只给有限预算，超时后立刻走数据库降级，避免拖长整个请求。
		hybridCtx, cancel := context.WithTimeout(ctx, followingHybridBudget)
		resp, err := s.listByFollowingHybrid(hybridCtx, limit, latestBefore, latestID, viewerAccountID, heavy)
		cancel()
		if err == nil {
			return resp, nil
		}
		log.Printf("hybrid following feed failed, fallback to MySQL: %v", err)
	}
	return s.listByFollowingFromDBGuarded(ctx, limit, latestBefore, latestID, viewerAccountID)
}

// listByFollowingFromDBGuarded 带并发限制与 SingleFlight 的数据库降级路径。
// Redis 故障时所有请求都会走这里，必须防止把 MySQL 打穿。
func (s *FeedService) listByFollowingFromDBGuarded(ctx context.Context, limit int, latestBefore time.Time, latestID uint, viewerAccountID uint) (FollowingResult, error) {
	if s.guard != nil && !s.guard.Allow("mysql-feed") {
		return FollowingResult{}, errCircuitOpen
	}

	select {
	case s.dbFallback <- struct{}{}:
		defer func() { <-s.dbFallback }()
	case <-ctx.Done():
		return FollowingResult{}, ctx.Err()
	}

	key := fmt.Sprintf("sf:following:db:%d:%d:%d", viewerAccountID, latestBefore.UnixMilli(), latestID)
	// 用独立 context，避免首个请求取消导致所有等待者一起失败；同时限制执行时间。
	leaderCtx, cancel := context.WithTimeout(context.Background(), followingDBBudget)
	defer cancel()

	value, err, _ := s.requestGroup.Do(key, func() (interface{}, error) {
		return s.listByFollowingFromDB(leaderCtx, limit, latestBefore, latestID, viewerAccountID)
	})
	if err != nil {
		s.recordDependency("mysql-feed", err)
		return FollowingResult{}, err
	}
	s.recordDependency("mysql-feed", nil)

	result, ok := value.(FollowingResult)
	if !ok {
		return FollowingResult{}, fmt.Errorf("unexpected following fallback result")
	}
	return result, nil
}

// startHeavyMigration 懒迁移存量重度用户。
//
// 触发条件：用户关注的大 V 数已达到重度阈值，但还没有重度标记或回填未完成。
// 迁移在后台执行，当前请求继续走普通推拉路径；完成后下一次请求才会走重度路径。
func (s *FeedService) startHeavyMigration(uid uint, stats []*domainsocial.Stats) {
	if uid == 0 || s.heavyUserCache == nil || s.timelineRepo == nil || len(stats) == 0 {
		return
	}
	bigVIDs := make([]uint, 0, len(stats))
	for _, item := range stats {
		if item == nil || item.AuthorID == 0 {
			continue
		}
		// 只登记当前仍是大 V 的作者；WasBig 已变小 V，历史内容继续走 Outbox。
		if item.IsBig || item.FansCount >= domainsocial.BigVFansThreshold {
			bigVIDs = append(bigVIDs, item.AuthorID)
		}
	}
	if len(bigVIDs) < int(domainsocial.HeavyEnterThreshold) {
		return
	}
	if _, loaded := s.heavyMigrating.LoadOrStore(uid, struct{}{}); loaded {
		return
	}

	go func() {
		defer s.heavyMigrating.Delete(uid)
		ctx, cancel := context.WithTimeout(context.Background(), heavyMigrationTimeout)
		defer cancel()

		started, err := s.heavyUserCache.TryBeginBackfill(ctx, uid)
		if err != nil || !started {
			return
		}
		if _, err := s.heavyUserCache.EnsureHeavy(ctx, uid, bigVIDs); err != nil {
			log.Printf("heavy migration: ensure heavy failed user=%d: %v", uid, err)
			return
		}
		members, err := s.heavyUserCache.ListHeavyFollowing(ctx, uid)
		if err != nil {
			log.Printf("heavy migration: list heavy following failed user=%d: %v", uid, err)
			return
		}
		for _, authorID := range members {
			entries, err := s.timelineRepo.ReadOutbox(ctx, authorID, 0, heavyMigrationBackfillLimit)
			if err != nil {
				log.Printf("heavy migration: read outbox failed user=%d author=%d: %v", uid, authorID, err)
				return
			}
			if len(entries) == 0 {
				continue
			}
			if err := s.timelineRepo.PushInboxEntriesWindow(ctx, uid, entries, domainfeed.DefaultInboxCapacity); err != nil {
				log.Printf("heavy migration: push inbox failed user=%d author=%d: %v", uid, authorID, err)
				return
			}
		}
		if err := s.heavyUserCache.MarkBackfilled(ctx, uid); err != nil {
			log.Printf("heavy migration: mark backfilled failed user=%d: %v", uid, err)
		}
	}()
}

// listByFollowingHybrid 合并 Inbox、大 V Outbox 和自己的 Outbox。
// heavy=true 表示选择性推已覆盖大 V 内容，只读 Inbox + 自己 Outbox。
func (s *FeedService) listByFollowingHybrid(ctx context.Context, limit int, latestBefore time.Time, latestID uint, viewerAccountID uint, heavy bool) (FollowingResult, error) {
	cursorMs := int64(0)
	if !latestBefore.IsZero() {
		cursorMs = latestBefore.UnixMilli()
	}

	// 熔断打开时直接跳过 Redis，避免每个请求都先等一次超时。
	if s.guard != nil && !s.guard.Allow(breakerRedisFeed) {
		return FollowingResult{}, errCircuitOpen
	}

	stats, err := s.listFollowingStats(ctx, viewerAccountID)
	if err != nil {
		return FollowingResult{}, err
	}

	// allowedAuthors 既用于挑选大 V，也用于读时过滤：
	// 取关后 Inbox 里可能残留旧视频，按当前关注集合过滤可以立即生效。
	allowedAuthors := make(map[uint]struct{}, len(stats)+1)
	allowedAuthors[viewerAccountID] = struct{}{}
	bigVIDs := make([]uint, 0)
	for _, item := range stats {
		if item == nil || item.AuthorID == 0 {
			continue
		}
		allowedAuthors[item.AuthorID] = struct{}{}
		if item.IsBig || item.WasBig || item.FansCount >= domainsocial.BigVFansThreshold {
			bigVIDs = append(bigVIDs, item.AuthorID)
		}
	}

	// 存量用户的重度索引由后台懒迁移建立；迁移完成前继续走普通推拉路径，
	// 避免“刚标记重度但 Inbox 还没回填”导致大 V 内容缺失。
	pushedBigV := make(map[uint]struct{}, len(bigVIDs))
	if heavy && s.heavyUserCache != nil {
		registered, err := s.heavyUserCache.ListHeavyFollowing(ctx, viewerAccountID)
		if err != nil {
			log.Printf("following feed: list heavy following failed user=%d: %v", viewerAccountID, err)
			heavy = false
		} else {
			for _, id := range registered {
				pushedBigV[id] = struct{}{}
			}
		}
	}

	bigVStats := make([]*domainsocial.Stats, 0)
	for _, item := range stats {
		if item == nil || item.AuthorID == 0 {
			continue
		}
		if item.IsBig || item.WasBig || item.FansCount >= domainsocial.BigVFansThreshold {
			// 只有“当前仍是大 V”且已登记到反向索引的作者才能跳过 Outbox。
			// WasBig 的作者已经变小 V，历史内容只在 Outbox，必须继续拉取或依赖降级回填。
			if heavy && (item.IsBig || item.FansCount >= domainsocial.BigVFansThreshold) {
				if _, pushed := pushedBigV[item.AuthorID]; pushed {
					continue
				}
			}
			bigVStats = append(bigVStats, item)
		}
	}

	// 存量用户没有重度索引时，读路径懒触发一次后台迁移：
	// 建立双向索引并回填大 V 最近内容，完成前继续走普通推拉路径。
	if !heavy && len(bigVStats) >= int(domainsocial.HeavyEnterThreshold) {
		s.startHeavyMigration(viewerAccountID, bigVStats)
	}

	// 多取 50 条，覆盖同一毫秒发布多条视频的情况
	readLimit := limit + 50
	entries, err := s.timelineRepo.ReadInbox(ctx, viewerAccountID, cursorMs, readLimit)
	if err != nil {
		s.recordDependency(breakerRedisFeed, err)
		return FollowingResult{}, err
	}

	ownEntries, err := s.timelineRepo.ReadOutbox(ctx, viewerAccountID, cursorMs, readLimit)
	if err != nil {
		s.recordDependency(breakerRedisFeed, err)
		return FollowingResult{}, err
	}
	entries = append(entries, ownEntries...)

	bigVEntries := []domainfeed.TimelineEntry(nil)
	unreadUpper := int64(-1)
	partial := false
	if len(bigVStats) > 0 {
		// 大 V 数量不设上限：按上界排序、分批读取、预算用尽时用水位线保证不跨过未读数据。
		bigVEntries, unreadUpper, partial, err = s.mergeBigVOutboxes(ctx, bigVStats, cursorMs, latestID, limit)
		if err != nil {
			s.recordDependency(breakerRedisFeed, err)
			return FollowingResult{}, err
		}
	}
	s.recordDependency(breakerRedisFeed, nil)
	entries = append(entries, bigVEntries...)

	if partial {
		entries = filterEntriesAbove(entries, unreadUpper)
	}
	entries = filterTimelineEntries(entries, cursorMs, latestID)
	// 保留全部候选而不是先截到 limit：前几条视频可能已被删除/取关，
	// 先截断会导致本页变短；这里按候选顺序继续向后取，直到凑满一页。
	merged := mergeTimelineEntries(entries, 0)
	if len(merged) == 0 {
		// 第一页为空可能是新用户或缓存被清空；
		// 非第一页为空可能是 Inbox 已被裁剪。
		// 交给外层回源数据库，避免静默返回“没有更多”。
		return FollowingResult{}, errTimelineEmpty
	}

	candidateIDs := make([]uint, 0, len(merged))
	for _, entry := range merged {
		candidateIDs = append(candidateIDs, entry.VideoID)
	}

	// 按页大小分批取视频：只有命中已删除/已取关内容时才继续向后取，
	// 正常路径仍然只查 limit 条，避免一次把几百个 ID 全部查出。
	videos := make([]*domainvideo.Video, 0, limit)
	consumed := 0
	for consumed < len(candidateIDs) && len(videos) < limit {
		batchSize := limit - len(videos)
		if batchSize < 1 {
			batchSize = 1
		}
		end := consumed + batchSize
		if end > len(candidateIDs) {
			end = len(candidateIDs)
		}
		batchVideos, err := s.getVideoByIDs(ctx, candidateIDs[consumed:end])
		if err != nil {
			return FollowingResult{}, err
		}
		videos = append(videos, filterVideosByAuthors(batchVideos, allowedAuthors)...)
		consumed = end
	}
	if len(videos) == 0 {
		// 命中的条目全部来自已取关作者或已被删除，回源数据库重新取一页。
		return FollowingResult{}, errTimelineEmpty
	}
	videos, hasMoreCandidates := assembleFollowingPage(videos, consumed, len(candidateIDs), limit)
	items, err := s.buildFeedItems(ctx, videos, viewerAccountID)
	if err != nil {
		return FollowingResult{}, err
	}

	// has_more 的语义是“还有候选”，而不是“本页刚好满”。
	// 预算截断或候选耗尽且本页不足时，用 DB 继续补页。
	hasMore := hasMoreCandidates || partial
	if len(items) < limit {
		items, hasMore, err = s.fillFollowingFromDB(ctx, items, limit, viewerAccountID)
		if err != nil {
			return FollowingResult{}, err
		}
	}

	var nextTime int64
	var nextID *uint
	if len(items) > 0 {
		// 游标取本页最后一条视频的毫秒发布时间与 ID，前端下次原样回传。
		last := items[len(items)-1]
		nextTime = last.CreateTimeMs
		id := last.ID
		nextID = &id
	}
	return FollowingResult{
		VideoList: items,
		NextTime:  nextTime,
		NextID:    nextID,
		HasMore:   hasMore,
		Partial:   partial,
	}, nil
}

// mergeBigVOutboxes 按 (score, videoID) 归并所有大 V 的 Outbox。
//
// 返回：
//   - entries：读到的大 V 视频；
//   - unreadUpper：未读作者中最大的上界，-1 表示没有未读作者；
//   - partial：是否因时间预算提前结束。
//
// 只要 partial=true，调用方必须丢弃 score <= unreadUpper 的结果，
// 否则这些结果可能被未读作者顶掉，导致游标跨过未读数据。
func (s *FeedService) mergeBigVOutboxes(
	ctx context.Context,
	stats []*domainsocial.Stats,
	cursorMs int64,
	cursorID uint,
	limit int,
) ([]domainfeed.TimelineEntry, int64, bool, error) {
	if len(stats) == 0 || limit <= 0 {
		return nil, -1, false, nil
	}

	type authorBound struct {
		authorID uint
		upper    int64
	}
	authors := make([]authorBound, 0, len(stats))
	for _, item := range stats {
		if item == nil || item.AuthorID == 0 {
			continue
		}
		// 上界取 min(最后发布时间, 当前游标)；未知发布时间按 +inf 处理，强制读取。
		upper := int64(math.MaxInt64)
		if !item.LastPublishTime.IsZero() {
			upper = item.LastPublishTime.UnixMilli()
		}
		if cursorMs > 0 && upper > cursorMs {
			upper = cursorMs
		}
		authors = append(authors, authorBound{authorID: item.AuthorID, upper: upper})
	}
	if len(authors) == 0 {
		return nil, -1, false, nil
	}
	sort.Slice(authors, func(i, j int) bool {
		if authors[i].upper == authors[j].upper {
			return authors[i].authorID > authors[j].authorID
		}
		return authors[i].upper > authors[j].upper
	})

	batchSize := s.bigVBatchSize
	if batchSize <= 0 {
		batchSize = defaultBigVBatchSize
	}
	budget := s.bigVMergeBudget
	if budget <= 0 {
		budget = defaultBigVMergeBudget
	}
	deadline := time.Now().Add(budget)

	h := &timelineEntryHeap{}
	heap.Init(h)
	// 每个作者最多读 limit+50 条，覆盖同毫秒分组；堆容量仍是 limit。
	perAuthorLimit := limit + 50

	for start := 0; start < len(authors); {
		end := start + batchSize
		if end > len(authors) {
			end = len(authors)
		}
		authorIDs := make([]uint, 0, end-start)
		for _, author := range authors[start:end] {
			authorIDs = append(authorIDs, author.authorID)
		}

		groups, err := s.timelineRepo.ReadOutboxes(ctx, authorIDs, cursorMs, perAuthorLimit)
		if err != nil {
			if ctx.Err() != nil {
				return heapEntries(h), authors[start].upper, true, nil
			}
			return nil, -1, false, err
		}
		for _, group := range groups {
			for _, entry := range group {
				if !entryAfterCursor(entry, cursorMs, cursorID) {
					continue
				}
				heapPushLimited(h, entry, limit)
			}
		}
		start = end

		nextUpper := int64(-1)
		if start < len(authors) {
			nextUpper = authors[start].upper
		}
		// 严格小于才能剪枝：上界相同还要比较 videoID。
		if h.Len() == limit && nextUpper >= 0 && nextUpper < h.topScore() {
			return heapEntries(h), -1, false, nil
		}
		if time.Now().After(deadline) {
			if start < len(authors) {
				return heapEntries(h), authors[start].upper, true, nil
			}
			return heapEntries(h), -1, false, nil
		}
	}
	return heapEntries(h), -1, false, nil
}

// fillFollowingFromDB 用数据库补齐预算截断后的页面。
func (s *FeedService) fillFollowingFromDB(ctx context.Context, items []FeedItem, limit int, viewerAccountID uint) ([]FeedItem, bool, error) {
	if len(items) >= limit {
		return items, true, nil
	}

	var before time.Time
	var beforeID uint
	if len(items) > 0 {
		last := items[len(items)-1]
		if last.CreateTimeMs > 0 {
			before = time.UnixMilli(last.CreateTimeMs)
		}
		beforeID = last.ID
	}

	result, err := s.listByFollowingFromDBGuarded(ctx, limit-len(items), before, beforeID, viewerAccountID)
	if err != nil {
		return nil, false, err
	}

	seen := make(map[uint]struct{}, len(items)+len(result.VideoList))
	for _, item := range items {
		seen[item.ID] = struct{}{}
	}
	for _, item := range result.VideoList {
		if _, ok := seen[item.ID]; ok {
			continue
		}
		items = append(items, item)
		if len(items) >= limit {
			break
		}
	}
	hasMore := result.HasMore || len(items) >= limit
	return items, hasMore, nil
}

// entryAfterCursor 判断时间线条目是否严格位于双游标之后。
func entryAfterCursor(entry domainfeed.TimelineEntry, cursorMs int64, cursorID uint) bool {
	if cursorMs <= 0 {
		return true
	}
	if entry.Score < cursorMs {
		return true
	}
	return entry.Score == cursorMs && cursorID > 0 && entry.VideoID < cursorID
}

// filterEntriesAbove 只保留 score 严格大于水位线的条目。
// 水位线以下的条目可能被未读作者顶掉，不能安全返回。
func filterEntriesAbove(entries []domainfeed.TimelineEntry, watermark int64) []domainfeed.TimelineEntry {
	if watermark < 0 {
		return entries
	}
	filtered := make([]domainfeed.TimelineEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Score > watermark {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

// timelineEntryHeap 小顶堆：堆顶是当前前 limit 名中最差的一条。
type timelineEntryHeap []domainfeed.TimelineEntry

// Len 返回堆大小。
func (h timelineEntryHeap) Len() int { return len(h) }

// Less 按 (score, videoID) 升序比较，堆顶最小。
func (h timelineEntryHeap) Less(i, j int) bool {
	if h[i].Score == h[j].Score {
		return h[i].VideoID < h[j].VideoID
	}
	return h[i].Score < h[j].Score
}

// Swap 交换堆元素。
func (h timelineEntryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push 入堆。
func (h *timelineEntryHeap) Push(value interface{}) {
	*h = append(*h, value.(domainfeed.TimelineEntry))
}

// Pop 出堆。
func (h *timelineEntryHeap) Pop() interface{} {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// topScore 返回堆顶 score。
func (h *timelineEntryHeap) topScore() int64 {
	if len(*h) == 0 {
		return math.MinInt64
	}
	return (*h)[0].Score
}

// heapPushLimited 维持容量为 limit 的小顶堆。
func heapPushLimited(h *timelineEntryHeap, entry domainfeed.TimelineEntry, limit int) {
	heap.Push(h, entry)
	if h.Len() > limit {
		heap.Pop(h)
	}
}

// heapEntries 返回堆内条目副本。
func heapEntries(h *timelineEntryHeap) []domainfeed.TimelineEntry {
	if h == nil || len(*h) == 0 {
		return nil
	}
	result := make([]domainfeed.TimelineEntry, len(*h))
	copy(result, *h)
	return result
}

// recordDependency 记录依赖调用结果，供熔断器统计。
func (s *FeedService) recordDependency(name string, err error) {
	if s.guard != nil {
		s.guard.Record(name, err)
	}
}

// listFollowingStats 读取关注列表：优先缓存，未命中回源数据库并回填缓存。
func (s *FeedService) listFollowingStats(ctx context.Context, viewerAccountID uint) ([]*domainsocial.Stats, error) {
	if s.followingCache != nil {
		if stats, ok := s.followingCache.Get(ctx, viewerAccountID); ok {
			return stats, nil
		}
	}
	stats, err := s.creatorRepo.ListFollowing(ctx, viewerAccountID)
	if err != nil {
		return nil, err
	}
	if s.followingCache != nil {
		s.followingCache.Set(ctx, viewerAccountID, stats)
	}
	return stats, nil
}

// listByFollowingFromDB 是推拉结合不可用时的降级路径。
func (s *FeedService) listByFollowingFromDB(ctx context.Context, limit int, latestBefore time.Time, latestID uint, viewerAccountID uint) (FollowingResult, error) {
	videos, err := s.repo.ListByFollowing(ctx, limit, viewerAccountID, latestBefore, latestID)
	if err != nil {
		return FollowingResult{}, err
	}

	var nextTime int64
	var nextID *uint
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextTime = last.CreateTime.UnixMilli()
		id := last.ID
		nextID = &id
	}
	items, err := s.buildFeedItems(ctx, videos, viewerAccountID)
	if err != nil {
		return FollowingResult{}, err
	}
	return FollowingResult{
		VideoList: items,
		NextTime:  nextTime,
		NextID:    nextID,
		HasMore:   len(videos) == limit,
	}, nil
}

// filterTimelineEntries 按 (score, videoID) 双游标过滤时间线记录。
// 当客户端只传了时间、没有传 ID 时，丢弃与游标同毫秒的记录，避免重复返回。
func filterTimelineEntries(entries []domainfeed.TimelineEntry, cursorMs int64, cursorID uint) []domainfeed.TimelineEntry {
	if cursorMs <= 0 {
		return entries
	}
	filtered := make([]domainfeed.TimelineEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Score < cursorMs || (entry.Score == cursorMs && cursorID > 0 && entry.VideoID < cursorID) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

// filterVideosByAuthors 只保留当前关注作者（以及自己）的视频。
func filterVideosByAuthors(videos []*domainvideo.Video, allowed map[uint]struct{}) []*domainvideo.Video {
	filtered := make([]*domainvideo.Video, 0, len(videos))
	for _, video := range videos {
		if video == nil {
			continue
		}
		if _, ok := allowed[video.AuthorID]; ok {
			filtered = append(filtered, video)
		}
	}
	return filtered
}

// assembleFollowingPage 从候选视频中截取一页，并判断是否还有未消费的候选。
//
// consumed 是已经取过视频的候选条目数，candidateCount 是全部候选数。
// 只有“还有候选没取”或“本页超过 limit”才说明后面还有内容；
// 候选已取完且视频不足一页时交给 DB 补页，避免误报 has_more。
func assembleFollowingPage(videos []*domainvideo.Video, consumed, candidateCount, limit int) ([]*domainvideo.Video, bool) {
	if limit <= 0 {
		return nil, false
	}
	hasMore := consumed < candidateCount || len(videos) > limit
	if len(videos) > limit {
		videos = videos[:limit]
	}
	return videos, hasMore
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

// ListByPopularity 按热度降序查询推荐流。
// 正常路径使用 24 小时桶指数衰减合并快照；Redis 不可用时回退数据库三字段游标。
func (s *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, latestPopularity int64, latestBefore time.Time, latestIDBefore uint) (PopularityResult, error) {
	if s.ranking != nil && (s.guard == nil || s.guard.Allow(breakerRedisPopularity)) {
		asOf := time.Now().UTC().Truncate(time.Hour)
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Hour)
		}

		dest := domainfeed.HotSnapshotKey(asOf)

		// 快照由离线任务预生成，请求路径只读，绝不做 ZUNIONSTORE。
		readCtx, readCancel := context.WithTimeout(ctx, 80*time.Millisecond)
		ids, readErr := s.ranking.SnapshotIDs(readCtx, dest, offset, limit)
		readCancel()
		if readErr != nil {
			s.recordDependency(breakerRedisPopularity, readErr)
		} else {
			s.recordDependency(breakerRedisPopularity, nil)
		}

		if readErr == nil && len(ids) == 0 && offset > 0 {
			return PopularityResult{
				VideoList:  []FeedItem{},
				AsOf:       asOf.Unix(),
				NextOffset: offset,
				HasMore:    false,
			}, nil
		}
		if readErr == nil && len(ids) > 0 {
			videos, err := s.repo.GetByIDs(ctx, ids)
			if err == nil {
				byID := make(map[uint]*domainvideo.Video, len(videos))
				for _, v := range videos {
					byID[v.ID] = v
				}
				ordered := make([]*domainvideo.Video, 0, len(ids))
				for _, id := range ids {
					if v := byID[id]; v != nil {
						ordered = append(ordered, v)
					}
				}

				items, err := s.buildFeedItems(ctx, ordered, viewerAccountID)
				if err != nil {
					return PopularityResult{}, err
				}

				resp := PopularityResult{
					VideoList:  items,
					AsOf:       asOf.Unix(),
					NextOffset: offset + len(items),
					HasMore:    len(items) == limit,
				}
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

	// 降级路径：数据库三字段游标分页
	videos, err := s.repo.ListByPopularity(ctx, limit, latestPopularity, latestBefore, latestIDBefore)
	if err != nil {
		return PopularityResult{}, err
	}

	items, err := s.buildFeedItems(ctx, videos, viewerAccountID)
	if err != nil {
		return PopularityResult{}, err
	}

	resp := PopularityResult{
		VideoList:  items,
		AsOf:       0, // 0 表示数据库直查
		NextOffset: 0,
		HasMore:    len(items) == limit,
	}
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

// HotWindowKeys 导出给离线快照任务复用，保证在线与离线的时间窗口一致。
func HotWindowKeys(asOf time.Time, hours int) []string {
	return domainfeed.HotTopWindowKeys(asOf, hours)
}

// ExponentialDecayWeights 导出给离线快照任务复用。
func ExponentialDecayWeights(asOf time.Time, hours int, halfLife time.Duration) []float64 {
	return domainfeed.HotDecayWeights(asOf, hours, halfLife)
}

// buildFeedItems 组装展示模型，并批量填充当前用户的点赞状态。
func (s *FeedService) buildFeedItems(ctx context.Context, videos []*domainvideo.Video, viewerAccountID uint) ([]FeedItem, error) {
	items := make([]FeedItem, 0, len(videos))
	videoIDs := make([]uint, len(videos))
	for i, v := range videos {
		videoIDs[i] = v.ID
	}

	likedMap, err := s.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
	if err != nil {
		return nil, err
	}

	for _, video := range videos {
		items = append(items, FeedItem{
			ID:           video.ID,
			AuthorID:     video.AuthorID,
			AuthorName:   video.Username,
			Title:        video.Title,
			Description:  video.Description,
			PlayURL:      video.PlayURL,
			CoverURL:     video.CoverURL,
			CreateTime:   video.CreateTime.Unix(),
			CreateTimeMs: video.CreateTime.UnixMilli(),
			LikesCount:   video.LikesCount,
			IsLiked:      likedMap[video.ID],
		})
	}
	return items, nil
}

// buildOrderedResult 按 orderedIDs 的顺序提取视频，忽略未命中的 ID。
func buildOrderedResult(orderedIDs []uint, dataMap map[uint]*domainvideo.Video) []*domainvideo.Video {
	res := make([]*domainvideo.Video, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if v, exists := dataMap[id]; exists && v != nil {
			res = append(res, v)
		}
	}
	return res
}
