// Package social 是社交上下文应用层，负责关注、取关和列表查询的用例编排。
package social

import (
	"context"

	domainsocial "feedsystem_video_go/internal/domain/social"
)

// AccountExister 账号存在性端口，由账号上下文实现。
type AccountExister interface {
	Exists(ctx context.Context, id uint) (bool, error)
}

// EventPublisher 关注事件发布端口。
// 由 infrastructure/messaging/rabbitmq 的 SocialMQ 实现，应用层不感知 MQ。
type EventPublisher interface {
	Follow(ctx context.Context, followerID, vloggerID uint) error
	UnFollow(ctx context.Context, followerID, vloggerID uint) error
}

// SocialService 社交应用服务。
type SocialService struct {
	repo     domainsocial.Repository
	accounts AccountExister
	events   EventPublisher
}

// NewSocialService 创建社交应用服务。
func NewSocialService(repo domainsocial.Repository, accounts AccountExister, events EventPublisher) *SocialService {
	return &SocialService{repo: repo, accounts: accounts, events: events}
}

// ensureAccountsExist 校验关注双方账号都存在。
func (s *SocialService) ensureAccountsExist(ctx context.Context, followerID, vloggerID uint) error {
	ok, err := s.accounts.Exists(ctx, followerID)
	if err != nil {
		return err
	}
	if !ok {
		return domainsocial.ErrFollowerNotFound
	}
	ok, err = s.accounts.Exists(ctx, vloggerID)
	if err != nil {
		return err
	}
	if !ok {
		return domainsocial.ErrVloggerNotFound
	}
	return nil
}

// Follow 关注博主；MQ 发布成功时由消费者异步落库，失败时同步落库兜底。
func (s *SocialService) Follow(ctx context.Context, relation *domainsocial.Social) error {
	if err := relation.Validate(); err != nil {
		return err
	}
	if err := s.ensureAccountsExist(ctx, relation.FollowerID, relation.VloggerID); err != nil {
		return err
	}

	isFollowed, err := s.repo.IsFollowed(ctx, relation)
	if err != nil {
		return err
	}
	if isFollowed {
		return domainsocial.ErrAlreadyFollowed
	}

	if s.events != nil {
		if err := s.events.Follow(ctx, relation.FollowerID, relation.VloggerID); err == nil {
			return nil
		}
	}
	return s.repo.Follow(ctx, relation)
}

// Unfollow 取消关注。
func (s *SocialService) Unfollow(ctx context.Context, relation *domainsocial.Social) error {
	if relation == nil || relation.FollowerID == 0 || relation.VloggerID == 0 {
		return domainsocial.ErrInvalidRelation
	}
	if err := s.ensureAccountsExist(ctx, relation.FollowerID, relation.VloggerID); err != nil {
		return err
	}

	isFollowed, err := s.repo.IsFollowed(ctx, relation)
	if err != nil {
		return err
	}
	if !isFollowed {
		return domainsocial.ErrNotFollowed
	}

	if s.events != nil {
		if err := s.events.UnFollow(ctx, relation.FollowerID, relation.VloggerID); err == nil {
			return nil
		}
	}
	return s.repo.Unfollow(ctx, relation)
}

// GetFollowers 查询博主粉丝列表。
func (s *SocialService) GetFollowers(ctx context.Context, vloggerID uint) ([]*domainsocial.AccountBrief, error) {
	ok, err := s.accounts.Exists(ctx, vloggerID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, domainsocial.ErrVloggerNotFound
	}
	return s.repo.GetFollowers(ctx, vloggerID)
}

// GetFollowing 查询用户关注的博主列表。
func (s *SocialService) GetFollowing(ctx context.Context, followerID uint) ([]*domainsocial.AccountBrief, error) {
	ok, err := s.accounts.Exists(ctx, followerID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, domainsocial.ErrFollowerNotFound
	}
	return s.repo.GetFollowing(ctx, followerID)
}

// IsFollowed 判断关注关系是否存在。
func (s *SocialService) IsFollowed(ctx context.Context, relation *domainsocial.Social) (bool, error) {
	if err := s.ensureAccountsExist(ctx, relation.FollowerID, relation.VloggerID); err != nil {
		return false, err
	}
	return s.repo.IsFollowed(ctx, relation)
}
