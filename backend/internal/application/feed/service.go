// Package feed 是 Feed 上下文应用层，负责最新流、点赞排行、关注流和热度流的用例编排。
package feed

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	domaincreator "feedsystem_video_go/internal/domain/creator"
	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainvideo "feedsystem_video_go/internal/domain/video"

	"golang.org/x/sync/singleflight"
)

// FeedService Feed 流应用服务。
//
// 三级缓存策略：
//
//	L1 本地内存（3s）→ L2 Redis → L3 MySQL（SingleFlight 防击穿）
type FeedService struct {
	repo         domainfeed.FeedRepository
	likeRepo     domainvideo.LikeRepository
	videoCache   VideoCache
	ranking      RankingCache
	timelineRepo domainfeed.TimelineRepository
	creatorRepo  domaincreator.Repository
	requestGroup singleflight.Group
}

// NewFeedService 创建 Feed 应用服务。
func NewFeedService(
	repo domainfeed.FeedRepository,
	likeRepo domainvideo.LikeRepository,
	videoCache VideoCache,
	ranking RankingCache,
	timelineRepo domainfeed.TimelineRepository,
	creatorRepo domaincreator.Repository,
) *FeedService {
	return &FeedService{
		repo:         repo,
		likeRepo:     likeRepo,
		videoCache:   videoCache,
		ranking:      ranking,
		timelineRepo: timelineRepo,
		creatorRepo:  creatorRepo,
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
	watermark, hasTimeline, err := s.ranking.GlobalTimelineTail(ctx)
	if err != nil {
		return LatestResult{}, err
	}

	if !hasTimeline && !allowRebuild {
		videos, err := s.repo.ListLatest(ctx, limit, latestBefore, latestID)
		if err != nil {
			return LatestResult{}, err
		}
		return s.buildLatestResult(ctx, videos, limit, viewerAccountID)
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
				return nil, err
			}
			return "SUCCESS", nil
		})
		if err != nil {
			return LatestResult{}, err
		}
		if v == "EMPTY_DB" {
			return LatestResult{HasMore: false}, nil
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
			return LatestResult{}, err
		}
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
func (s *FeedService) ListByFollowing(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (FollowingResult, error) {
	if viewerAccountID == 0 {
		return FollowingResult{}, nil
	}

	if s.timelineRepo != nil && s.creatorRepo != nil {
		resp, err := s.listByFollowingHybrid(ctx, limit, latestBefore, viewerAccountID)
		if err == nil {
			return resp, nil
		}
		log.Printf("hybrid following feed failed, fallback to MySQL: %v", err)
	}
	return s.listByFollowingFromDB(ctx, limit, latestBefore, viewerAccountID)
}

// listByFollowingHybrid 合并 Inbox、大 V Outbox 和自己的 Outbox。
func (s *FeedService) listByFollowingHybrid(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (FollowingResult, error) {
	cursorMs := int64(0)
	if !latestBefore.IsZero() {
		cursorMs = latestBefore.UnixMilli()
	}

	stats, err := s.creatorRepo.ListFollowing(ctx, viewerAccountID)
	if err != nil {
		return FollowingResult{}, err
	}

	bigVAuthorIDs := make([]uint, 0)
	for _, item := range stats {
		if item.IsBig || item.FansCount >= domaincreator.BigVFansThreshold {
			bigVAuthorIDs = append(bigVAuthorIDs, item.AuthorID)
		}
	}
	// 限制单次合并的大 V 数量，避免关注几百个大 V 时读放大失控
	const maxBigVAuthors = 100
	if len(bigVAuthorIDs) > maxBigVAuthors {
		bigVAuthorIDs = bigVAuthorIDs[:maxBigVAuthors]
	}

	entries, err := s.timelineRepo.ReadInbox(ctx, viewerAccountID, cursorMs, limit)
	if err != nil {
		return FollowingResult{}, err
	}
	if len(bigVAuthorIDs) > 0 {
		groups, err := s.timelineRepo.ReadOutboxes(ctx, bigVAuthorIDs, cursorMs, limit)
		if err != nil {
			return FollowingResult{}, err
		}
		for _, group := range groups {
			entries = append(entries, group...)
		}
	}

	ownEntries, err := s.timelineRepo.ReadOutbox(ctx, viewerAccountID, cursorMs, limit)
	if err != nil {
		return FollowingResult{}, err
	}
	entries = append(entries, ownEntries...)

	merged := mergeTimelineEntries(entries, limit)
	if len(merged) == 0 {
		// 第一页没有缓存时回源数据库，兼顾新用户和历史数据
		if latestBefore.IsZero() {
			return s.listByFollowingFromDB(ctx, limit, latestBefore, viewerAccountID)
		}
		return FollowingResult{VideoList: []FeedItem{}, HasMore: false}, nil
	}

	videoIDs := make([]uint, 0, len(merged))
	for _, entry := range merged {
		videoIDs = append(videoIDs, entry.VideoID)
	}
	videos, err := s.getVideoByIDs(ctx, videoIDs)
	if err != nil {
		return FollowingResult{}, err
	}
	items, err := s.buildFeedItems(ctx, videos, viewerAccountID)
	if err != nil {
		return FollowingResult{}, err
	}

	var nextTime int64
	if len(items) > 0 {
		nextTime = items[len(items)-1].CreateTime
	}
	return FollowingResult{
		VideoList: items,
		NextTime:  nextTime,
		HasMore:   len(items) == limit,
	}, nil
}

// listByFollowingFromDB 是推拉结合不可用时的降级路径。
func (s *FeedService) listByFollowingFromDB(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (FollowingResult, error) {
	videos, err := s.repo.ListByFollowing(ctx, limit, viewerAccountID, latestBefore)
	if err != nil {
		return FollowingResult{}, err
	}

	var nextTime int64
	if len(videos) > 0 {
		nextTime = videos[len(videos)-1].CreateTime.Unix()
	}
	items, err := s.buildFeedItems(ctx, videos, viewerAccountID)
	if err != nil {
		return FollowingResult{}, err
	}
	return FollowingResult{
		VideoList: items,
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

// ListByPopularity 按热度降序查询推荐流。
// 正常路径使用 24 小时桶线性衰减合并快照；Redis 不可用时回退数据库三字段游标。
func (s *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, latestPopularity int64, latestBefore time.Time, latestIDBefore uint) (PopularityResult, error) {
	if s.ranking != nil {
		asOf := time.Now().UTC().Truncate(time.Hour)
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Hour)
		}

		const windowHours = 24
		keys := hotWindowKeys(asOf, windowHours)
		weights := linearDecayWeights(windowHours)
		dest := "hot:video:merge:1h:" + asOf.Format("2006010215")

		buildCtx, buildCancel := context.WithTimeout(ctx, 80*time.Millisecond)
		_ = s.ranking.EnsureSnapshot(buildCtx, dest, keys, weights)
		buildCancel()

		readCtx, readCancel := context.WithTimeout(ctx, 80*time.Millisecond)
		ids, readErr := s.ranking.SnapshotIDs(readCtx, dest, offset, limit)
		readCancel()

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
			ID:          video.ID,
			AuthorID:    video.AuthorID,
			AuthorName:  video.Username,
			Title:       video.Title,
			Description: video.Description,
			PlayURL:     video.PlayURL,
			CoverURL:    video.CoverURL,
			CreateTime:  video.CreateTime.Unix(),
			LikesCount:  video.LikesCount,
			IsLiked:     likedMap[video.ID],
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
