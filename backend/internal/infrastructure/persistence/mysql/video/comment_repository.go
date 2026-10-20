package video

import (
	"context"
	"errors"

	domainvideo "feedsystem_video_go/internal/domain/video"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CommentRepository 评论仓储的 MySQL 实现。
type CommentRepository struct {
	db *gorm.DB
}

// NewCommentRepository 创建评论仓储。
func NewCommentRepository(db *gorm.DB) *CommentRepository {
	return &CommentRepository{db: db}
}

// Create 插入评论。
func (r *CommentRepository) Create(ctx context.Context, comment *domainvideo.Comment) error {
	model := &CommentModel{
		Username: comment.Username,
		VideoID:  comment.VideoID,
		AuthorID: comment.AuthorID,
		Content:  comment.Content,
	}
	if err := r.db.WithContext(ctx).Create(model).Error; err != nil {
		return err
	}
	comment.ID = model.ID
	comment.CreatedAt = model.CreatedAt
	return nil
}

// CreateWithPopularity 在事务中写评论并增加视频热度。
func (r *CommentRepository) CreateWithPopularity(ctx context.Context, comment *domainvideo.Comment) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id").First(&VideoModel{}, comment.VideoID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return domainvideo.ErrVideoNotFound
			}
			return err
		}
		model := &CommentModel{
			Username: comment.Username,
			VideoID:  comment.VideoID,
			AuthorID: comment.AuthorID,
			Content:  comment.Content,
		}
		if err := tx.Create(model).Error; err != nil {
			return err
		}
		comment.ID = model.ID
		comment.CreatedAt = model.CreatedAt
		return tx.Model(&VideoModel{}).Where("id = ?", comment.VideoID).
			UpdateColumn("popularity", gorm.Expr("popularity + 1")).Error
	})
}

// Delete 删除评论。
func (r *CommentRepository) Delete(ctx context.Context, comment *domainvideo.Comment) error {
	return r.db.WithContext(ctx).Delete(&CommentModel{}, comment.ID).Error
}

// GetAll 查询视频的全部评论。
func (r *CommentRepository) GetAll(ctx context.Context, videoID uint) ([]domainvideo.Comment, error) {
	var models []CommentModel
	if err := r.db.WithContext(ctx).
		Where("video_id = ?", videoID).
		Find(&models).Error; err != nil {
		return nil, err
	}
	comments := make([]domainvideo.Comment, 0, len(models))
	for _, model := range models {
		comments = append(comments, domainvideo.Comment{
			ID:        model.ID,
			Username:  model.Username,
			VideoID:   model.VideoID,
			AuthorID:  model.AuthorID,
			Content:   model.Content,
			CreatedAt: model.CreatedAt,
		})
	}
	return comments, nil
}

// IsExist 判断评论是否存在。
func (r *CommentRepository) IsExist(ctx context.Context, id uint) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&CommentModel{}).Where("id = ?", id).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetByID 按 ID 查询评论，不存在时返回 nil。
func (r *CommentRepository) GetByID(ctx context.Context, id uint) (*domainvideo.Comment, error) {
	var model CommentModel
	if err := r.db.WithContext(ctx).First(&model, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &domainvideo.Comment{
		ID:        model.ID,
		Username:  model.Username,
		VideoID:   model.VideoID,
		AuthorID:  model.AuthorID,
		Content:   model.Content,
		CreatedAt: model.CreatedAt,
	}, nil
}
