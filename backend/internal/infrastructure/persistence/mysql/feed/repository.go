// Package feed 提供 Feed 上下文的 MySQL 查询实现。
package feed

import (
	"context"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainvideo "feedsystem_video_go/internal/domain/video"
	videomysql "feedsystem_video_go/internal/infrastructure/persistence/mysql/video"

	"gorm.io/gorm"
)

// Repository Feed 流查询仓储实现。
// 所有查询均使用游标分页，避免 OFFSET 深翻页导致的全表扫描。
type Repository struct {
	db *gorm.DB
}

// NewRepository 创建 Feed 查询仓储。
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// ListLatest 按发布时间倒序查询，使用 (create_time, id) 复合游标。
func (r *Repository) ListLatest(ctx context.Context, limit int, before time.Time, beforeID uint) ([]*domainvideo.Video, error) {
	query := r.db.WithContext(ctx).Model(&videomysql.VideoModel{}).Order("create_time DESC")

	if !before.IsZero() {
		if beforeID > 0 {
			query = query.Where(
				"(create_time < ?) OR (create_time = ? AND id < ?)",
				before, before, beforeID,
			)
		} else {
			query = query.Where("create_time < ?", before)
		}
	}
	return r.find(query, limit)
}

// ListLikesCountWithCursor 按点赞数倒序查询，点赞数相同按 ID 兜底。
func (r *Repository) ListLikesCountWithCursor(ctx context.Context, limit int, cursor *domainfeed.LikesCountCursor) ([]*domainvideo.Video, error) {
	query := r.db.WithContext(ctx).Model(&videomysql.VideoModel{}).
		Order("likes_count DESC, id DESC")

	if cursor != nil {
		query = query.Where(
			"(likes_count < ?) OR (likes_count = ? AND id < ?)",
			cursor.LikesCount, cursor.LikesCount, cursor.ID,
		)
	}
	return r.find(query, limit)
}

// ListByFollowing 查询关注作者发布的视频。
// 使用子查询先取关注列表，再查视频，避免大表 JOIN 放大。
func (r *Repository) ListByFollowing(ctx context.Context, limit int, viewerID uint, before time.Time) ([]*domainvideo.Video, error) {
	query := r.db.WithContext(ctx).Model(&videomysql.VideoModel{}).Order("create_time DESC")

	if viewerID > 0 {
		followingSubQuery := r.db.WithContext(ctx).
			Table("socials").
			Select("vlogger_id").
			Where("follower_id = ?", viewerID)
		query = query.Where("author_id IN (?)", followingSubQuery)
	}

	if !before.IsZero() {
		query = query.Where("create_time < ?", before)
	}
	return r.find(query, limit)
}

// ListByPopularity 按热度倒序查询，热度相同再按时间和 ID 兜底。
func (r *Repository) ListByPopularity(ctx context.Context, limit int, popularityBefore int64, before time.Time, beforeID uint) ([]*domainvideo.Video, error) {
	query := r.db.WithContext(ctx).Model(&videomysql.VideoModel{}).
		Order("popularity DESC, create_time DESC, id DESC")

	if !before.IsZero() && beforeID > 0 {
		query = query.Where(
			"(popularity < ?) OR (popularity = ? AND create_time < ?) OR (popularity = ? AND create_time = ? AND id < ?)",
			popularityBefore,
			popularityBefore, before,
			popularityBefore, before, beforeID,
		)
	}
	return r.find(query, limit)
}

// GetByIDs 批量按 ID 查询视频。
func (r *Repository) GetByIDs(ctx context.Context, ids []uint) ([]*domainvideo.Video, error) {
	if len(ids) == 0 {
		return []*domainvideo.Video{}, nil
	}
	var models []videomysql.VideoModel
	if err := r.db.WithContext(ctx).
		Model(&videomysql.VideoModel{}).
		Where("id IN ?", ids).
		Find(&models).Error; err != nil {
		return nil, err
	}
	return toDomainVideos(models), nil
}

// find 执行查询并转换为领域实体。
func (r *Repository) find(query *gorm.DB, limit int) ([]*domainvideo.Video, error) {
	var models []videomysql.VideoModel
	if err := query.Limit(limit).Find(&models).Error; err != nil {
		return nil, err
	}
	return toDomainVideos(models), nil
}

// toDomainVideos 把 MySQL 模型转换为视频领域实体。
func toDomainVideos(models []videomysql.VideoModel) []*domainvideo.Video {
	videos := make([]*domainvideo.Video, 0, len(models))
	for _, model := range models {
		videos = append(videos, &domainvideo.Video{
			ID:          model.ID,
			AuthorID:    model.AuthorID,
			Username:    model.Username,
			Title:       model.Title,
			Description: model.Description,
			PlayURL:     model.PlayURL,
			CoverURL:    model.CoverURL,
			CreateTime:  model.CreateTime,
			LikesCount:  model.LikesCount,
			Popularity:  model.Popularity,
		})
	}
	return videos
}
