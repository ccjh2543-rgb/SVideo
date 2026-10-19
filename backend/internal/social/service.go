package social

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/middleware/rabbitmq"
)

// AccountFinder 定义查找用户账号的接口，由 account 包实现。
// 在 social 包中定义接口而非直接依赖具体类型，遵循 Go 依赖反转原则。
type AccountFinder interface {
	FindByID(ctx context.Context, id uint) (*account.Account, error)
}

type SocialService struct {
	repo        *SocialRepository
	accountRepo AccountFinder
	socialMQ    *rabbitmq.SocialMQ
}

func NewSocialService(repo *SocialRepository, accountRepo *account.AccountRepository, socialMQ *rabbitmq.SocialMQ) *SocialService {
	return &SocialService{repo: repo, accountRepo: accountRepo, socialMQ: socialMQ}
}

func (s *SocialService) Follow(ctx context.Context, social *Social) error {
	_, err := s.accountRepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return err
	}
	_, err = s.accountRepo.FindByID(ctx, social.VloggerID)
	if err != nil {
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
	_, err := s.accountRepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return err
	}
	_, err = s.accountRepo.FindByID(ctx, social.VloggerID)
	if err != nil {
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

func (s *SocialService) GetAllFollowers(ctx context.Context, VloggerID uint) ([]*account.Account, error) {
	_, err := s.accountRepo.FindByID(ctx, VloggerID)
	if err != nil {
		return nil, err
	}
	return s.repo.GetAllFollowers(ctx, VloggerID)
}

func (s *SocialService) GetAllVloggers(ctx context.Context, FollowerID uint) ([]*account.Account, error) {
	_, err := s.accountRepo.FindByID(ctx, FollowerID)
	if err != nil {
		return nil, err
	}
	return s.repo.GetAllVloggers(ctx, FollowerID)
}

func (s *SocialService) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	_, err := s.accountRepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return false, err
	}
	_, err = s.accountRepo.FindByID(ctx, social.VloggerID)
	if err != nil {
		return false, err
	}
	return s.repo.IsFollowed(ctx, social)
}
