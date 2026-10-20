package social

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
)

// AccountExister 账号存在性接口，由账号上下文实现。
// social 只关心账号是否存在，不依赖账号实体和密码等细节。
type AccountExister interface {
	Exists(ctx context.Context, id uint) (bool, error)
}

type SocialService struct {
	repo        *SocialRepository
	accountRepo AccountExister
	socialMQ    *rabbitmq.SocialMQ
}

func NewSocialService(repo *SocialRepository, accountRepo AccountExister, socialMQ *rabbitmq.SocialMQ) *SocialService {
	return &SocialService{repo: repo, accountRepo: accountRepo, socialMQ: socialMQ}
}

// ensureAccountsExist 校验关注双方账号都存在。
func (s *SocialService) ensureAccountsExist(ctx context.Context, followerID, vloggerID uint) error {
	ok, err := s.accountRepo.Exists(ctx, followerID)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("follower account not found")
	}
	ok, err = s.accountRepo.Exists(ctx, vloggerID)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("vlogger account not found")
	}
	return nil
}

func (s *SocialService) Follow(ctx context.Context, social *Social) error {
	if err := s.ensureAccountsExist(ctx, social.FollowerID, social.VloggerID); err != nil {
		return err
	}
	if social.FollowerID == social.VloggerID {
		return errors.New("can not follow self")
	}
	isFollowed, err := s.repo.IsFollowed(ctx, social)
	if err != nil {
		return err
	}
	if isFollowed {
		return errors.New("already followed")
	}
	if s.socialMQ != nil {
		if err := s.socialMQ.Follow(ctx, social.FollowerID, social.VloggerID); err == nil {
			return nil
		}
	}
	return s.repo.Follow(ctx, social)
}

func (s *SocialService) Unfollow(ctx context.Context, social *Social) error {
	if err := s.ensureAccountsExist(ctx, social.FollowerID, social.VloggerID); err != nil {
		return err
	}
	isFollowed, err := s.repo.IsFollowed(ctx, social)
	if err != nil {
		return err
	}
	if !isFollowed {
		return errors.New("not followed")
	}
	if s.socialMQ != nil {
		if err := s.socialMQ.UnFollow(ctx, social.FollowerID, social.VloggerID); err == nil {
			return nil
		}
	}
	return s.repo.Unfollow(ctx, social)
}

func (s *SocialService) GetAllFollowers(ctx context.Context, VloggerID uint) ([]*AccountInfo, error) {
	ok, err := s.accountRepo.Exists(ctx, VloggerID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("vlogger account not found")
	}
	return s.repo.GetAllFollowers(ctx, VloggerID)
}

func (s *SocialService) GetAllVloggers(ctx context.Context, FollowerID uint) ([]*AccountInfo, error) {
	ok, err := s.accountRepo.Exists(ctx, FollowerID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("follower account not found")
	}
	return s.repo.GetAllVloggers(ctx, FollowerID)
}

func (s *SocialService) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	if err := s.ensureAccountsExist(ctx, social.FollowerID, social.VloggerID); err != nil {
		return false, err
	}
	return s.repo.IsFollowed(ctx, social)
}
