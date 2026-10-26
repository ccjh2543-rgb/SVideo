package social

import (
	"context"
	"database/sql"
	"errors"
	"time"

	domainsocial "feedsystem_video_go/internal/domain/social"

	"gorm.io/gorm"
)

// StatsRepository 创作者统计仓储的 MySQL 实现。
type StatsRepository struct {
	db *gorm.DB
}

// NewStatsRepository 创建创作者统计仓储。
func NewStatsRepository(db *gorm.DB) *StatsRepository {
	return &StatsRepository{db: db}
}

// Get 查询作者统计，不存在时返回默认值。
func (r *StatsRepository) Get(ctx context.Context, authorID uint) (*domainsocial.Stats, error) {
	var model StatsModel
	if err := r.db.WithContext(ctx).First(&model, authorID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &domainsocial.Stats{AuthorID: authorID}, nil
		}
		return nil, err
	}
	stats := &domainsocial.Stats{
		AuthorID:  model.AuthorID,
		FansCount: model.FansCount,
		IsBig:     model.IsBig,
	}
	if model.LastPublishTime != nil {
		stats.LastPublishTime = *model.LastPublishTime
	}
	return stats, nil
}

// UpsertOnPublish 更新作者最后发布时间。
func (r *StatsRepository) UpsertOnPublish(ctx context.Context, authorID uint, publishTime time.Time) error {
	return r.db.WithContext(ctx).Exec(
		`INSERT INTO creator_stats (author_id, fans_count, is_big, last_publish_time)
		 VALUES (?, 0, 0, ?)
		 ON DUPLICATE KEY UPDATE last_publish_time = VALUES(last_publish_time)`,
		authorID, publishTime,
	).Error
}

// IncrementFans 原子增减粉丝数，并同步维护 is_big 标记。
func (r *StatsRepository) IncrementFans(ctx context.Context, authorID uint, delta int64) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return upsertFans(tx, authorID, delta)
	})
}

// upsertFans 在给定事务内增减粉丝数并刷新 is_big。
// 供关注关系的事务化写入复用，保证 socials 与 creator_stats 同时生效。
func upsertFans(tx *gorm.DB, authorID uint, delta int64) error {
	if err := tx.Exec(
		`INSERT INTO creator_stats (author_id, fans_count, is_big)
		 VALUES (?, GREATEST(?, 0), ?)
		 ON DUPLICATE KEY UPDATE fans_count = GREATEST(fans_count + ?, 0)`,
		authorID, delta, delta >= domainsocial.BigVFansThreshold, delta,
	).Error; err != nil {
		return err
	}
	return tx.Exec(
		`UPDATE creator_stats
		 SET is_big = (fans_count >= ?)
		 WHERE author_id = ?`,
		domainsocial.BigVFansThreshold, authorID,
	).Error
}

// ListFollowing 查询当前用户关注的全部作者统计。
func (r *StatsRepository) ListFollowing(ctx context.Context, followerID uint) ([]*domainsocial.Stats, error) {
	var rows []struct {
		AuthorID        uint         `gorm:"column:author_id"`
		FansCount       int64        `gorm:"column:fans_count"`
		IsBig           bool         `gorm:"column:is_big"`
		LastPublishTime sql.NullTime `gorm:"column:last_publish_time"`
	}
	if err := r.db.WithContext(ctx).Raw(
		`SELECT s.vlogger_id AS author_id,
		        COALESCE(cs.fans_count, 0) AS fans_count,
		        COALESCE(cs.is_big, 0) AS is_big,
		        cs.last_publish_time
		 FROM socials s
		 LEFT JOIN creator_stats cs ON cs.author_id = s.vlogger_id
		 WHERE s.follower_id = ?`,
		followerID,
	).Scan(&rows).Error; err != nil {
		return nil, err
	}

	stats := make([]*domainsocial.Stats, 0, len(rows))
	for _, row := range rows {
		item := &domainsocial.Stats{
			AuthorID:  row.AuthorID,
			FansCount: row.FansCount,
			IsBig:     row.IsBig,
		}
		if row.LastPublishTime.Valid {
			item.LastPublishTime = row.LastPublishTime.Time
		}
		stats = append(stats, item)
	}
	return stats, nil
}
