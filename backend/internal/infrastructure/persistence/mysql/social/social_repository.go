package social

import (
	"context"

	domainsocial "feedsystem_video_go/internal/domain/social"
	"feedsystem_video_go/internal/util"

	"gorm.io/gorm"
)

// Repository 关注关系仓储的 MySQL 实现。
type Repository struct {
	db *gorm.DB
}

// NewRepository 创建关注关系仓储。
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// Follow 创建关注关系。
func (r *Repository) Follow(ctx context.Context, relation *domainsocial.Social) error {
	return r.db.WithContext(ctx).Create(&SocialModel{
		FollowerID: relation.FollowerID,
		VloggerID:  relation.VloggerID,
	}).Error
}

// Unfollow 删除关注关系。
func (r *Repository) Unfollow(ctx context.Context, relation *domainsocial.Social) error {
	return r.db.WithContext(ctx).
		Where("follower_id = ? AND vlogger_id = ?", relation.FollowerID, relation.VloggerID).
		Delete(&SocialModel{}).Error
}

// FollowWithStats 在同一事务内创建关注关系并增加粉丝数。
// 并发或重复关注由 socials 的唯一索引兜底，重复时返回数据库错误，调用方按幂等处理。
func (r *Repository) FollowWithStats(ctx context.Context, relation *domainsocial.Social) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&SocialModel{
			FollowerID: relation.FollowerID,
			VloggerID:  relation.VloggerID,
		}).Error; err != nil {
			if util.IsDuplicateEntry(err) {
				return domainsocial.ErrAlreadyFollowed
			}
			return err
		}
		return upsertFans(tx, relation.VloggerID, 1)
	})
}

// UnfollowWithStats 在同一事务内删除关注关系并减少粉丝数。
// 关系不存在时返回 ErrNotFollowed，避免“没删到关系却仍然扣粉丝数”。
func (r *Repository) UnfollowWithStats(ctx context.Context, relation *domainsocial.Social) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Where("follower_id = ? AND vlogger_id = ?", relation.FollowerID, relation.VloggerID).
			Delete(&SocialModel{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return domainsocial.ErrNotFollowed
		}
		return upsertFans(tx, relation.VloggerID, -1)
	})
}

// IsFollowed 判断关注关系是否存在。
func (r *Repository) IsFollowed(ctx context.Context, relation *domainsocial.Social) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&SocialModel{}).
		Where("follower_id = ? AND vlogger_id = ?", relation.FollowerID, relation.VloggerID).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetFollowers 查询博主的粉丝列表。
func (r *Repository) GetFollowers(ctx context.Context, vloggerID uint) ([]*domainsocial.AccountBrief, error) {
	var relations []SocialModel
	if err := r.db.WithContext(ctx).
		Model(&SocialModel{}).
		Where("vlogger_id = ?", vloggerID).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	accountIDs := make([]uint, 0, len(relations))
	for _, relation := range relations {
		accountIDs = append(accountIDs, relation.FollowerID)
	}
	return r.findAccounts(ctx, accountIDs)
}

// GetFollowing 查询用户关注的博主列表。
func (r *Repository) GetFollowing(ctx context.Context, followerID uint) ([]*domainsocial.AccountBrief, error) {
	var relations []SocialModel
	if err := r.db.WithContext(ctx).
		Model(&SocialModel{}).
		Where("follower_id = ?", followerID).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	accountIDs := make([]uint, 0, len(relations))
	for _, relation := range relations {
		accountIDs = append(accountIDs, relation.VloggerID)
	}
	return r.findAccounts(ctx, accountIDs)
}

// ListFollowerIDsAfter 按粉丝 ID 游标分页查询粉丝，用于 Feed 写扩散。
func (r *Repository) ListFollowerIDsAfter(ctx context.Context, vloggerID uint, afterFollowerID uint, limit int) ([]uint, error) {
	if limit <= 0 {
		return nil, nil
	}
	var followerIDs []uint
	query := r.db.WithContext(ctx).
		Model(&SocialModel{}).
		Where("vlogger_id = ?", vloggerID)
	if afterFollowerID > 0 {
		query = query.Where("follower_id > ?", afterFollowerID)
	}
	if err := query.
		Order("follower_id ASC").
		Limit(limit).
		Pluck("follower_id", &followerIDs).Error; err != nil {
		return nil, err
	}
	return followerIDs, nil
}

// findAccounts 批量查询账号摘要，保持传入顺序。
func (r *Repository) findAccounts(ctx context.Context, accountIDs []uint) ([]*domainsocial.AccountBrief, error) {
	if len(accountIDs) == 0 {
		return []*domainsocial.AccountBrief{}, nil
	}

	var accounts []*domainsocial.AccountBrief
	if err := r.db.WithContext(ctx).
		Table("accounts").
		Select("id, username").
		Where("id IN ?", accountIDs).
		Find(&accounts).Error; err != nil {
		return nil, err
	}
	return accounts, nil
}
