package video

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"gorm.io/gorm"
)

type VideoService struct {
	repo         *VideoRepository
	cache        *rediscache.Client
	cacheTTL     time.Duration
	popularityMQ *rabbitmq.PopularityMQ
}

func NewVideoService(repo *VideoRepository, cache *rediscache.Client, popularityMQ *rabbitmq.PopularityMQ) *VideoService {
	return &VideoService{repo: repo, cache: cache, cacheTTL: 5 * time.Minute, popularityMQ: popularityMQ}
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

	return err
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

func (vs *VideoService) GetDetail(ctx context.Context, id uint) (*Video, error) {
	cacheKey := fmt.Sprintf("video:detail:id=%d", id)

	getCached := func() (*Video, bool) {
		opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		b, err := vs.cache.GetBytes(opCtx, cacheKey)
		if err != nil {
			return nil, false
		}
		var cached Video
		if err := json.Unmarshal(b, &cached); err != nil {
			return nil, false
		}
		return &cached, true
	}

	setCached := func(video *Video) {
		b, err := json.Marshal(video)
		if err != nil {
			return
		}
		opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		_ = vs.cache.SetBytes(opCtx, cacheKey, b, vs.cacheTTL)
	}

	if vs.cache != nil {
		if v, ok := getCached(); ok {
			return v, nil
		}

		// 再次尝试读取缓存，确认确实未命中（防止竞态）
		if v, ok := getCached(); ok {
			return v, nil
		}

		// 缓存未命中（Redis 返回 nil），尝试获取分布式锁
		lockKey := "lock:" + cacheKey

		lockCtx, lockCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		token, locked, lockErr := vs.cache.Lock(lockCtx, lockKey, 2*time.Second)
		lockCancel()

		if lockErr == nil && locked {
			defer func() { _ = vs.cache.Unlock(context.Background(), lockKey, token) }()

			if v, ok := getCached(); ok {
				return v, nil
			}

			video, err := vs.repo.GetByID(ctx, id)
			if err != nil {
				return nil, err
			}
			setCached(video)
			return video, nil
		}

		// 没拿到锁：等待别人回填缓存
		for i := 0; i < 5; i++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			if v, ok := getCached(); ok {
				return v, nil
			}
		}
	}

	video, err := vs.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if vs.cache != nil {
		setCached(video)
	}
	return video, nil
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
