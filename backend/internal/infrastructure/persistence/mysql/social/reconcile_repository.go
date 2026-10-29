package social

import (
	"context"

	domainsocial "feedsystem_video_go/internal/domain/social"

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

// FindFansCountDrifts 查询粉丝数与关注明细不一致的创作者。
func (r *ReconcileRepository) FindFansCountDrifts(ctx context.Context, limit int) ([]domainsocial.FansCountDrift, error) {
	if limit <= 0 {
		return []domainsocial.FansCountDrift{}, nil
	}
	var rows []struct {
		AuthorID uint  `gorm:"column:author_id"`
		Stored   int64 `gorm:"column:stored"`
		Actual   int64 `gorm:"column:actual"`
	}
	if err := r.db.WithContext(ctx).Raw(
		`SELECT cs.author_id AS author_id, cs.fans_count AS stored, COUNT(s.id) AS actual
		 FROM creator_stats cs
		 LEFT JOIN socials s ON s.vlogger_id = cs.author_id
		 GROUP BY cs.author_id, cs.fans_count
		 HAVING stored <> actual
		 LIMIT ?`,
		limit,
	).Scan(&rows).Error; err != nil {
		return nil, err
	}
	drifts := make([]domainsocial.FansCountDrift, 0, len(rows))
	for _, row := range rows {
		drifts = append(drifts, domainsocial.FansCountDrift{
			AuthorID: row.AuthorID,
			Stored:   row.Stored,
			Actual:   row.Actual,
		})
	}
	return drifts, nil
}

// FixFansCount 把粉丝数修正为明细行数，并同步刷新 is_big。
// 对账同样可能触发“大 V → 小 V”，因此需要写入 demote_pending，
// 让后台任务把 Outbox 历史批量回填到粉丝 Inbox。
func (r *ReconcileRepository) FixFansCount(ctx context.Context, authorID uint, expected int64) error {
	return r.db.WithContext(ctx).Exec(
		`UPDATE creator_stats
		 SET was_big = CASE
		         WHEN is_big = 1 AND ? < ? THEN 1
		         ELSE was_big
		     END,
		     demote_pending = CASE
		         WHEN ? >= ? THEN 0
		         WHEN is_big = 1 AND ? < ? THEN 1
		         ELSE demote_pending
		     END,
		     fans_count = ?,
		     is_big = (? >= ?)
		 WHERE author_id = ?`,
		expected, domainsocial.BigVExitThreshold,
		expected, domainsocial.BigVFansThreshold,
		expected, domainsocial.BigVExitThreshold,
		expected,
		expected, domainsocial.BigVFansThreshold,
		authorID,
	).Error
}
