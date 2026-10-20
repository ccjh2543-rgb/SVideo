package video

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	cachelib "feedsystem_video_go/internal/middleware/cache"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"gorm.io/gorm"
)

type VideoService struct {
	repo         *VideoRepository
	cache        *rediscache.Client
	cacheTTL     time.Duration
	popularityMQ *rabbitmq.PopularityMQ
	bloom        *cachelib.BloomFilter
}

func NewVideoService(
	repo *VideoRepository,
	cache *rediscache.Client,
	popularityMQ *rabbitmq.PopularityMQ,
	bloom *cachelib.BloomFilter,
) *VideoService {
	return &VideoService{
		repo:         repo,
		cache:        cache,
		cacheTTL:     5 * time.Minute,
		popularityMQ: popularityMQ,
		bloom:        bloom,
	}
}

func (vs *VideoService) Publish(ctx context.Context, video *Video) error {
	if video == nil {
		return errors.New("video is nil")
	}
	video.Title = strings.TrimSpace(video.Title)
	video.PlayURL = strings.TrimSpace(video.PlayURL)
	video.CoverURL = strings.TrimSpace(video.CoverURL)

	if video.Title == "" {
		return errors.New("title is required")
	}
	if video.PlayURL == "" {
		return errors.New("play url is required")
	}
	if video.CoverURL == "" {
		return errors.New("cover url is required")
	}

	// 在数据库事务中原子性地执行视频记录与本地消息表的写入
	err := vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// --- 第一步：写入视频主表 ---
		// tx.Create(video) 会将 video 结构体插入到对应的视频表中
		// 如果数据库表设置了自增主键，执行成功后 video.ID 会被回填为实际生成的 ID
		if err := tx.Create(video).Error; err != nil {
			// 插入失败，直接返回错误，事务会自动回滚
			// 此时视频表和消息表都不会有任何新数据
			return err
		}

		// --- 第二步：构造本地消息表记录 ---
		// 使用刚生成的 video.ID 关联消息，保证事件与业务数据的对应关系
		msg := OutboxMsg{
			VideoID:    video.ID,          // 关联的视频主键，必须在上一步 Create 后才能拿到
			AuthorID:   video.AuthorID,    // 作者 ID，供 Fan-out Worker 使用
			EventType:  "video_published", // 事件类型，后续消息分发器会据此路由
			Status:     "pending",         // 初始状态为 pending，表示待发送
			CreateTime: video.CreateTime,  // 沿用视频的创建时间，保持时间一致性
		}

		// --- 第三步：写入本地消息表 ---
		// 将消息记录插入 outbox 表，与视频写入在同一个事务中
		if err := tx.Create(&msg).Error; err != nil {
			// 消息写入失败，同样返回错误让事务回滚
			// 这样可以避免“视频已入库但事件未记录”的数据不一致问题
			return err
		}

		// --- 提交事务 ---
		// 返回 nil 表示事务内所有操作均成功，GORM 会自动提交事务
		// 提交后，视频和消息记录才会真正持久化到磁盘
		return nil
	})
	if err != nil {
		return err
	}

	// 写入布隆过滤器，降低不存在视频 ID 的穿透压力。
	if vs.bloom != nil {
		_ = vs.bloom.Add(ctx, uint64(video.ID))
	}
	return nil
}

func (vs *VideoService) Delete(ctx context.Context, id uint, authorID uint) error {
	video, err := vs.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if video == nil {
		return errors.New("video not found")
	}
	if video.AuthorID != authorID {
		return errors.New("unauthorized")
	}
	if err := vs.repo.DeleteVideo(ctx, id); err != nil {
		return err
	}
	if vs.cache != nil {
		cacheKey := fmt.Sprintf("video:detail:id=%d", id)
		_ = vs.cache.Del(context.Background(), cacheKey)
	}
	return nil
}

func (vs *VideoService) ListByAuthorID(ctx context.Context, authorID uint) ([]Video, error) {
	videos, err := vs.repo.ListByAuthorID(ctx, int64(authorID))
	if err != nil {
		return nil, err
	}
	return videos, nil
}

// GetDetail 查询视频详情。
//
// 缓存策略：
//   - 布隆过滤器拦截不存在的 ID
//   - 空值缓存兜底布隆误判
//   - 逻辑过期：过期后先返回旧值，再异步重建
//   - 分布式锁防止缓存击穿
func (vs *VideoService) GetDetail(ctx context.Context, id uint) (*Video, error) {
	if id == 0 {
		return nil, errors.New("video id is required")
	}
	cacheKey := fmt.Sprintf("video:detail:id=%d", id)
	nullKey := fmt.Sprintf("video:null:id=%d", id)

	if vs.cache != nil {
		if b, err := vs.cache.GetBytes(ctx, nullKey); err == nil && len(b) > 0 {
			return nil, errors.New("video not found")
		}
	}

	if vs.bloom != nil {
		exists, err := vs.bloom.Exists(ctx, uint64(id))
		if err == nil && !exists {
			return nil, errors.New("video not found")
		}
	}

	if vs.cache != nil {
		if video, expired, found := vs.getDetailCache(ctx, cacheKey); found {
			if expired {
				go vs.refreshDetail(id)
			}
			return video, nil
		}

		lockKey := "lock:" + cacheKey
		lockCtx, lockCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		token, locked, lockErr := vs.cache.Lock(lockCtx, lockKey, 2*time.Second)
		lockCancel()

		if lockErr == nil && locked {
			defer func() { _ = vs.cache.Unlock(context.Background(), lockKey, token) }()

			if video, _, found := vs.getDetailCache(ctx, cacheKey); found {
				return video, nil
			}
			video, err := vs.repo.GetByID(ctx, id)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					vs.setNullCache(id)
					return nil, errors.New("video not found")
				}
				return nil, err
			}
			vs.setDetailCache(ctx, video)
			return video, nil
		}

		for i := 0; i < 5; i++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			if video, _, found := vs.getDetailCache(ctx, cacheKey); found {
				return video, nil
			}
		}
	}

	video, err := vs.repo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			vs.setNullCache(id)
			return nil, errors.New("video not found")
		}
		return nil, err
	}
	if vs.cache != nil {
		vs.setDetailCache(ctx, video)
	}
	return video, nil
}

// getDetailCache 读取逻辑过期缓存。
func (vs *VideoService) getDetailCache(ctx context.Context, cacheKey string) (*Video, bool, bool) {
	data, expired, found, err := cachelib.GetLogical(ctx, vs.cache, cacheKey)
	if err != nil || !found {
		return nil, false, false
	}
	var video Video
	if err := json.Unmarshal(data, &video); err != nil {
		return nil, false, false
	}
	return &video, expired, true
}

// setDetailCache 写入逻辑过期缓存。
func (vs *VideoService) setDetailCache(ctx context.Context, video *Video) {
	if vs.cache == nil || video == nil {
		return
	}
	data, err := json.Marshal(video)
	if err != nil {
		return
	}
	physicalTTL := cachelib.Jitter(30*time.Minute, 0.2)
	_ = cachelib.SetLogical(ctx, vs.cache, fmt.Sprintf("video:detail:id=%d", video.ID), data, vs.cacheTTL, physicalTTL)
}

// setNullCache 写入空值缓存，防止布隆误判后持续穿透数据库。
func (vs *VideoService) setNullCache(id uint) {
	if vs.cache == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = vs.cache.SetBytes(ctx, fmt.Sprintf("video:null:id=%d", id), []byte("1"), cachelib.Jitter(time.Minute, 0.2))
}

// refreshDetail 异步重建逻辑过期缓存。
func (vs *VideoService) refreshDetail(id uint) {
	if vs.cache == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cacheKey := fmt.Sprintf("video:detail:id=%d", id)
	lockKey := "lock:" + cacheKey
	token, locked, err := vs.cache.Lock(ctx, lockKey, 2*time.Second)
	if err != nil || !locked {
		return
	}
	defer func() { _ = vs.cache.Unlock(context.Background(), lockKey, token) }()

	// 双重检查：其他请求可能已经完成重建。
	if _, expired, found := vs.getDetailCache(ctx, cacheKey); found && !expired {
		return
	}
	video, err := vs.repo.GetByID(ctx, id)
	if err != nil {
		return
	}
	vs.setDetailCache(ctx, video)
}

// RebuildBloom 从数据库加载全部视频 ID 重建布隆过滤器。
func (vs *VideoService) RebuildBloom(ctx context.Context) error {
	if vs.bloom == nil {
		return nil
	}
	return vs.bloom.Rebuild(ctx, func(afterID uint, limit int) ([]uint, error) {
		return vs.repo.ListIDsAfter(ctx, afterID, limit)
	})
}

func (vs *VideoService) UpdateLikesCount(ctx context.Context, id uint, likesCount int64) error {
	if err := vs.repo.UpdateLikesCount(ctx, id, likesCount); err != nil {
		return err
	}
	return nil
}

func (vs *VideoService) UpdatePopularity(ctx context.Context, id uint, change int64) error {
	if err := vs.repo.UpdatePopularity(ctx, id, change); err != nil {
		return err
	}

	// 发送 MQ 消息，由 PopularityWorker 异步更新 Redis 缓存（详情缓存失效 + 热榜 ZSET）
	if vs.popularityMQ != nil {
		_ = vs.popularityMQ.Update(ctx, id, change)
	}
	return nil
}
