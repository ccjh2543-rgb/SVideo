package video

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/util"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LikeService struct {
	repo         *LikeRepository
	VideoRepo    *VideoRepository
	cache        *rediscache.Client
	likeMQ       *rabbitmq.LikeMQ
	popularityMQ *rabbitmq.PopularityMQ
}

func NewLikeService(repo *LikeRepository, videoRepo *VideoRepository, cache *rediscache.Client, likeMQ *rabbitmq.LikeMQ, popularityMQ *rabbitmq.PopularityMQ) *LikeService {
	return &LikeService{repo: repo, VideoRepo: videoRepo, cache: cache, likeMQ: likeMQ, popularityMQ: popularityMQ}
}

// Like 处理用户点赞请求，包含以下关键流程：
// 1. 参数校验与业务规则检查（视频存在性、是否重复点赞）。
// 2. 优先使用消息队列异步写入 MySQL 和 Redis，实现快速响应。
// 3. 若任一消息队列投递失败，则回退到同步直接写入，保证数据最终写入。
func (s *LikeService) Like(ctx context.Context, like *Like) error {
	// --- 参数基础校验 ---
	if like == nil {
		return errors.New("like is nil")
	}
	if like.VideoID == 0 || like.AccountID == 0 {
		return errors.New("video_id and account_id are required")
	}

	// --- 视频存在性检查（如果有 VideoRepo 则先确认视频存在）---
	if s.VideoRepo != nil {
		ok, err := s.VideoRepo.IsExist(ctx, like.VideoID)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("video not found")
		}
	}

	// --- 重复点赞检查：如果已有点赞记录则直接拒绝 ---
	isLiked, err := s.repo.IsLiked(ctx, like.VideoID, like.AccountID)
	if err != nil {
		return err
	}
	if isLiked {
		return errors.New("user has liked this video")
	}

	// 设置点赞时间
	like.CreatedAt = time.Now()

	// --- 异步路径：通过消息队列发布事件 ---
	mysqlEnqueued := false // 标记 MySQL 写入事件是否成功入队
	redisEnqueued := false // 标记 Redis 热度更新事件是否成功入队

	// 投递 MySQL 持久化消息（包含插入点赞记录并更新计数）
	if s.likeMQ != nil {
		if err := s.likeMQ.Like(ctx, like.AccountID, like.VideoID); err == nil {
			mysqlEnqueued = true
		}
	}

	// 投递 Redis 热度更新消息
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, like.VideoID, 1); err == nil {
			redisEnqueued = true
		}
	}

	// 两个 MQ 都成功发布，直接返回成功（异步消费者负责最终写入）
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	// --- 降级路径：MQ 投递失败时，同步写入确保可靠性 ---

	// 如果 MySQL 消息未成功入队，则启动数据库事务进行同步写入
	if !mysqlEnqueued {
		err := s.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// 事务内再次校验视频是否存在（使用 SELECT ... FOR UPDATE 避免幻读）
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&Video{}, like.VideoID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return errors.New("video not found")
				}
				return err
			}

			// 插入点赞记录，处理重复键错误（并发下可能出现）
			if err := tx.Create(like).Error; err != nil {
				if util.IsDuplicateEntry(err) {
					return errors.New("user has liked this video")
				}
				return err
			}

			// 原子更新 likes_count + 1
			if err := tx.Model(&Video{}).Where("id = ?", like.VideoID).
				UpdateColumn("likes_count", gorm.Expr("likes_count + 1")).Error; err != nil {
				return err
			}

			// 更新热度值 popularity + 1
			return tx.Model(&Video{}).Where("id = ?", like.VideoID).
				UpdateColumn("popularity", gorm.Expr("popularity + 1")).Error
		})
		if err != nil {
			return err
		}
	}

	// 如果 Redis 消息未成功入队，直接调用缓存更新函数同步写入
	if !redisEnqueued {
		UpdatePopularityCache(ctx, s.cache, like.VideoID, 1)
	}

	return nil
}

func (s *LikeService) Unlike(ctx context.Context, like *Like) error {
	if like == nil {
		return errors.New("like is nil")
	}
	if like.VideoID == 0 || like.AccountID == 0 {
		return errors.New("video_id and account_id are required")
	}

	if s.VideoRepo != nil {
		ok, err := s.VideoRepo.IsExist(ctx, like.VideoID)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("video not found")
		}
	}

	isLiked, err := s.repo.IsLiked(ctx, like.VideoID, like.AccountID)
	if err != nil {
		return err
	}
	if !isLiked {
		return errors.New("user has not liked this video")
	}

	mysqlEnqueued := false
	redisEnqueued := false
	if s.likeMQ != nil {
		if err := s.likeMQ.Unlike(ctx, like.AccountID, like.VideoID); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, like.VideoID, -1); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	// Fallback: direct MySQL write when like MQ publish fails.
	if !mysqlEnqueued {
		err := s.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			del := tx.Where("video_id = ? AND account_id = ?", like.VideoID, like.AccountID).Delete(&Like{})
			if del.Error != nil {
				return del.Error
			}
			if del.RowsAffected == 0 {
				return errors.New("user has not liked this video")
			}

			if err := tx.Model(&Video{}).Where("id = ?", like.VideoID).
				UpdateColumn("likes_count", gorm.Expr("GREATEST(likes_count - 1, 0)")).Error; err != nil {
				return err
			}
			return tx.Model(&Video{}).Where("id = ?", like.VideoID).
				UpdateColumn("popularity", gorm.Expr("GREATEST(popularity - 1, 0)")).Error
		})
		if err != nil {
			return err
		}
	}

	// Fallback: direct Redis update when popularity MQ publish fails.
	if !redisEnqueued {
		UpdatePopularityCache(ctx, s.cache, like.VideoID, -1)
	}
	return nil
}

func (s *LikeService) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	return s.repo.IsLiked(ctx, videoID, accountID)
}

func (s *LikeService) ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error) {
	return s.repo.ListLikedVideos(ctx, accountID)
}
