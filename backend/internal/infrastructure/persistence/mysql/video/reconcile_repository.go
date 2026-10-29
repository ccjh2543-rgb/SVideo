package video

import (
	"context"

	domainvideo "feedsystem_video_go/internal/domain/video"

	"gorm.io/gorm"
)

// ReconcileRepository 计数对账仓储的 MySQL 实现。
type ReconcileRepository struct {
	db *gorm.DB
}

// NewReconcileRepository 创建计数对账仓储。
func NewReconcileRepository(db *gorm.DB) *ReconcileRepository {
	return &ReconcileRepository{db: db}
}

// FindLikesCountDrifts 查询点赞数与明细行数不一致的视频。
// 使用 LEFT JOIN + HAVING，单批 limit 条，避免一次扫描全表。
func (r *ReconcileRepository) FindLikesCountDrifts(ctx context.Context, limit int) ([]domainvideo.LikesCountDrift, error) {
	if limit <= 0 {
		return []domainvideo.LikesCountDrift{}, nil
	}
	var rows []struct {
		VideoID uint  `gorm:"column:video_id"`
		Stored  int64 `gorm:"column:stored"`
		Actual  int64 `gorm:"column:actual"`
	}
	if err := r.db.WithContext(ctx).Raw(
		`SELECT v.id AS video_id, v.likes_count AS stored, COUNT(l.id) AS actual
		 FROM videos v
		 LEFT JOIN likes l ON l.video_id = v.id
		 GROUP BY v.id, v.likes_count
		 HAVING stored <> actual
		 LIMIT ?`,
		limit,
	).Scan(&rows).Error; err != nil {
		return nil, err
	}
	drifts := make([]domainvideo.LikesCountDrift, 0, len(rows))
	for _, row := range rows {
		drifts = append(drifts, domainvideo.LikesCountDrift{
			VideoID: row.VideoID,
			Stored:  row.Stored,
			Actual:  row.Actual,
		})
	}
	return drifts, nil
}

// FixLikesCount 把点赞数修正为明细行数。
func (r *ReconcileRepository) FixLikesCount(ctx context.Context, videoID uint, expected int64) error {
	return r.db.WithContext(ctx).
		Model(&VideoModel{}).
		Where("id = ?", videoID).
		UpdateColumn("likes_count", expected).Error
}
