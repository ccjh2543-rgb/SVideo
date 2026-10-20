package video

import (
	"context"
	"errors"
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"
	"feedsystem_video_go/internal/util"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// LikeRepository 点赞仓储的 MySQL 实现。
type LikeRepository struct {
	db *gorm.DB
}

// NewLikeRepository 创建点赞仓储。
func NewLikeRepository(db *gorm.DB) *LikeRepository {
	return &LikeRepository{db: db}
}

// Like 插入点赞记录。
func (r *LikeRepository) Like(ctx context.Context, like *domainvideo.Like) error {
	return r.db.WithContext(ctx).Create(&LikeModel{
		VideoID:   like.VideoID,
		AccountID: like.AccountID,
		CreatedAt: like.CreatedAt,
	}).Error
}

// Unlike 删除点赞记录。
func (r *LikeRepository) Unlike(ctx context.Context, like *domainvideo.Like) error {
	return r.db.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", like.VideoID, like.AccountID).
		Delete(&LikeModel{}).Error
}

// LikeIgnoreDuplicate 插入点赞，重复时静默忽略。
func (r *LikeRepository) LikeIgnoreDuplicate(ctx context.Context, like *domainvideo.Like) (bool, error) {
	if like == nil || like.VideoID == 0 || like.AccountID == 0 {
		return false, nil
	}
	err := r.db.WithContext(ctx).Create(&LikeModel{
		VideoID:   like.VideoID,
		AccountID: like.AccountID,
		CreatedAt: time.Now(),
	}).Error
	if err == nil {
		return true, nil
	}
	if util.IsDuplicateEntry(err) {
		return false, nil
	}
	return false, err
}

// DeleteByVideoAndAccount 删除指定点赞关系。
func (r *LikeRepository) DeleteByVideoAndAccount(ctx context.Context, videoID, accountID uint) (bool, error) {
	if videoID == 0 || accountID == 0 {
		return false, nil
	}
	result := r.db.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Delete(&LikeModel{})
	return result.RowsAffected > 0, result.Error
}

// IsLiked 判断是否已点赞。
func (r *LikeRepository) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&LikeModel{}).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// BatchGetLiked 批量获取点赞状态。
func (r *LikeRepository) BatchGetLiked(ctx context.Context, videoIDs []uint, accountID uint) (map[uint]bool, error) {
	liked := make(map[uint]bool)
	if len(videoIDs) == 0 || accountID == 0 {
		return liked, nil
	}
	var models []LikeModel
	if err := r.db.WithContext(ctx).Model(&LikeModel{}).
		Where("video_id IN ? AND account_id = ?", videoIDs, accountID).
		Find(&models).Error; err != nil {
		return nil, err
	}
	for _, model := range models {
		liked[model.VideoID] = true
	}
	return liked, nil
}

// ListLikedVideos 查询用户点赞过的视频。
func (r *LikeRepository) ListLikedVideos(ctx context.Context, accountID uint) ([]domainvideo.Video, error) {
	if accountID == 0 {
		return []domainvideo.Video{}, nil
	}
	var models []VideoModel
	if err := r.db.WithContext(ctx).
		Model(&VideoModel{}).
		Joins("JOIN likes ON likes.video_id = videos.id").
		Where("likes.account_id = ?", accountID).
		Order("likes.created_at DESC").
		Find(&models).Error; err != nil {
		return nil, err
	}
	videos := make([]domainvideo.Video, 0, len(models))
	for i := range models {
		videos = append(videos, *toVideoDomain(&models[i]))
	}
	return videos, nil
}

// LikeWithCounters 在事务中完成点赞和计数更新。
func (r *LikeRepository) LikeWithCounters(ctx context.Context, videoID, accountID uint) (bool, error) {
	created := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id").First(&VideoModel{}, videoID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return domainvideo.ErrVideoNotFound
			}
			return err
		}
		if err := tx.Create(&LikeModel{
			VideoID:   videoID,
			AccountID: accountID,
			CreatedAt: time.Now(),
		}).Error; err != nil {
			if util.IsDuplicateEntry(err) {
				return domainvideo.ErrAlreadyLiked
			}
			return err
		}
		if err := tx.Model(&VideoModel{}).Where("id = ?", videoID).
			UpdateColumn("likes_count", gorm.Expr("likes_count + 1")).Error; err != nil {
			return err
		}
		if err := tx.Model(&VideoModel{}).Where("id = ?", videoID).
			UpdateColumn("popularity", gorm.Expr("popularity + 1")).Error; err != nil {
			return err
		}
		created = true
		return nil
	})
	return created, err
}

// UnlikeWithCounters 在事务中完成取消点赞和计数更新。
func (r *LikeRepository) UnlikeWithCounters(ctx context.Context, videoID, accountID uint) (bool, error) {
	deleted := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Where("video_id = ? AND account_id = ?", videoID, accountID).Delete(&LikeModel{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return domainvideo.ErrNotLiked
		}
		if err := tx.Model(&VideoModel{}).Where("id = ?", videoID).
			UpdateColumn("likes_count", gorm.Expr("GREATEST(likes_count - 1, 0)")).Error; err != nil {
			return err
		}
		if err := tx.Model(&VideoModel{}).Where("id = ?", videoID).
			UpdateColumn("popularity", gorm.Expr("GREATEST(popularity - 1, 0)")).Error; err != nil {
			return err
		}
		deleted = true
		return nil
	})
	return deleted, err
}
