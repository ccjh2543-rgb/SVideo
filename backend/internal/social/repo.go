package social

import (
	"context"

	"gorm.io/gorm"
)

type SocialRepository struct {
	db *gorm.DB
}

func NewSocialRepository(db *gorm.DB) *SocialRepository {
	return &SocialRepository{db: db}
}

func (r *SocialRepository) Follow(ctx context.Context, social *Social) error {
	return r.db.WithContext(ctx).Create(social).Error
}

func (r *SocialRepository) Unfollow(ctx context.Context, social *Social) error {
	return r.db.WithContext(ctx).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).
		Delete(&Social{}).Error
}

func (r *SocialRepository) GetAllFollowers(ctx context.Context, VloggerID uint) ([]*AccountInfo, error) {
	var relations []Social
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("vlogger_id = ?", VloggerID).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	followerIDs := make([]uint, 0, len(relations))
	for _, rel := range relations {
		followerIDs = append(followerIDs, rel.FollowerID)
	}
	if len(followerIDs) == 0 {
		return []*AccountInfo{}, nil
	}

	var followers []*AccountInfo
	if err := r.db.WithContext(ctx).
		Table("accounts").
		Select("id, username").
		Where("id IN ?", followerIDs).
		Find(&followers).Error; err != nil {
		return nil, err
	}
	return followers, nil
}

// ListFollowerIDsAfter 按粉丝 ID 游标分页查询粉丝，用于 Feed 写扩散。
func (r *SocialRepository) ListFollowerIDsAfter(ctx context.Context, vloggerID uint, afterFollowerID uint, limit int) ([]uint, error) {
	if limit <= 0 {
		return nil, nil
	}
	var followerIDs []uint
	query := r.db.WithContext(ctx).
		Model(&Social{}).
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

func (r *SocialRepository) GetAllVloggers(ctx context.Context, FollowerID uint) ([]*AccountInfo, error) {
	var relations []Social
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("follower_id = ?", FollowerID).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	vloggerIDs := make([]uint, 0, len(relations))
	for _, rel := range relations {
		vloggerIDs = append(vloggerIDs, rel.VloggerID)
	}
	if len(vloggerIDs) == 0 {
		return []*AccountInfo{}, nil
	}

	var vloggers []*AccountInfo
	if err := r.db.WithContext(ctx).
		Table("accounts").
		Select("id, username").
		Where("id IN ?", vloggerIDs).
		Find(&vloggers).Error; err != nil {
		return nil, err
	}
	return vloggers, nil
}

func (r *SocialRepository) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}
