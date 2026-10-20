package video

import (
	"context"
	"errors"

	domainvideo "feedsystem_video_go/internal/domain/video"

	"gorm.io/gorm"
)

// VideoRepository 视频仓储的 MySQL 实现。
type VideoRepository struct {
	db *gorm.DB
}

// NewVideoRepository 创建视频仓储。
func NewVideoRepository(db *gorm.DB) *VideoRepository {
	return &VideoRepository{db: db}
}

// CreateWithOutbox 在同一事务中写入视频和事务消息。
func (r *VideoRepository) CreateWithOutbox(ctx context.Context, video *domainvideo.Video, outbox *domainvideo.OutboxMessage) error {
	if video == nil || outbox == nil {
		return domainvideo.ErrVideoInvalid
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		videoModel := toVideoModel(video)
		if err := tx.Create(videoModel).Error; err != nil {
			return err
		}
		video.ID = videoModel.ID

		outboxModel := &OutboxModel{
			VideoID:    video.ID,
			AuthorID:   outbox.AuthorID,
			EventType:  outbox.EventType,
			CreateTime: outbox.CreateTime,
			Status:     outbox.Status,
		}
		if err := tx.Create(outboxModel).Error; err != nil {
			return err
		}
		outbox.ID = outboxModel.ID
		return nil
	})
}

// GetByID 按主键查询视频。
func (r *VideoRepository) GetByID(ctx context.Context, id uint) (*domainvideo.Video, error) {
	var model VideoModel
	if err := r.db.WithContext(ctx).First(&model, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domainvideo.ErrVideoNotFound
		}
		return nil, err
	}
	return toVideoDomain(&model), nil
}

// Delete 删除视频。
func (r *VideoRepository) Delete(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).Delete(&VideoModel{}, id).Error
}

// ListByAuthorID 查询作者视频。
func (r *VideoRepository) ListByAuthorID(ctx context.Context, authorID uint) ([]domainvideo.Video, error) {
	var models []VideoModel
	if err := r.db.WithContext(ctx).
		Where("author_id = ?", authorID).
		Order("create_time DESC").
		Find(&models).Error; err != nil {
		return nil, err
	}
	videos := make([]domainvideo.Video, 0, len(models))
	for i := range models {
		videos = append(videos, *toVideoDomain(&models[i]))
	}
	return videos, nil
}

// ListIDsAfter 按主键游标分页查询视频 ID。
func (r *VideoRepository) ListIDsAfter(ctx context.Context, afterID uint, limit int) ([]uint, error) {
	if limit <= 0 {
		return nil, nil
	}
	var ids []uint
	query := r.db.WithContext(ctx).Model(&VideoModel{})
	if afterID > 0 {
		query = query.Where("id > ?", afterID)
	}
	if err := query.Order("id ASC").Limit(limit).Pluck("id", &ids).Error; err != nil {
		return nil, err
	}
	return ids, nil
}

// IsExist 判断视频是否存在。
func (r *VideoRepository) IsExist(ctx context.Context, id uint) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&VideoModel{}).Where("id = ?", id).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// UpdateLikesCount 直接设置点赞数。
func (r *VideoRepository) UpdateLikesCount(ctx context.Context, id uint, likesCount int64) error {
	return r.db.WithContext(ctx).Model(&VideoModel{}).
		Where("id = ?", id).
		Update("likes_count", likesCount).Error
}

// ChangeLikesCount 原子增减点赞数。
func (r *VideoRepository) ChangeLikesCount(ctx context.Context, id uint, change int64) error {
	return r.db.WithContext(ctx).Model(&VideoModel{}).
		Where("id = ?", id).
		UpdateColumn("likes_count", gorm.Expr("GREATEST(likes_count + ?, 0)", change)).Error
}

// ChangePopularity 原子增减热度。
func (r *VideoRepository) ChangePopularity(ctx context.Context, id uint, change int64) error {
	return r.db.WithContext(ctx).Model(&VideoModel{}).
		Where("id = ?", id).
		UpdateColumn("popularity", gorm.Expr("GREATEST(popularity + ?, 0)", change)).Error
}
