// Package social 是社交上下文应用层，负责关注、取关和列表查询的用例编排。
package social

import (
	"context"
	"log"

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

// FollowingCacheInvalidator 关注列表缓存失效端口。
// 关注关系变化后需要立即清理缓存，避免关注流读到旧数据。
type FollowingCacheInvalidator interface {
	Invalidate(ctx context.Context, followerID uint) error
}

// HeavyRegistry 重度用户索引维护端口。
type HeavyRegistry interface {
	OnFollowBigV(ctx context.Context, uid, bigV uint) (becameHeavy bool, isHeavy bool, err error)
	OnUnfollowBigV(ctx context.Context, uid, bigV uint) error
}

// SocialService 社交应用服务。
type SocialService struct {
	repo           domainsocial.Repository
	accounts       AccountExister
	stats          domainsocial.StatsRepository
	events         EventPublisher
	followingCache FollowingCacheInvalidator
	heavy          HeavyRegistry
}

// NewSocialService 创建社交应用服务。
func NewSocialService(
	repo domainsocial.Repository,
	accounts AccountExister,
	stats domainsocial.StatsRepository,
	events EventPublisher,
	followingCache FollowingCacheInvalidator,
	heavy HeavyRegistry,
) *SocialService {
	return &SocialService{
		repo:           repo,
		accounts:       accounts,
		stats:          stats,
		events:         events,
		followingCache: followingCache,
		heavy:          heavy,
	}
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
			s.invalidateFollowing(ctx, relation.FollowerID)
			return nil
		}
	}
	if err := s.repo.FollowWithStats(ctx, relation); err != nil {
		return err
	}
	s.updateHeavyOnFollow(ctx, relation.FollowerID, relation.VloggerID)
	s.invalidateFollowing(ctx, relation.FollowerID)
	return nil
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
			s.invalidateFollowing(ctx, relation.FollowerID)
			return nil
		}
	}
	if err := s.repo.UnfollowWithStats(ctx, relation); err != nil {
		return err
	}
	s.updateHeavyOnUnfollow(ctx, relation.FollowerID, relation.VloggerID)
	s.invalidateFollowing(ctx, relation.FollowerID)
	return nil
}

// updateHeavyOnFollow 在 MQ 不可用、同步落库时维护重度用户索引。
func (s *SocialService) updateHeavyOnFollow(ctx context.Context, followerID, vloggerID uint) {
	if s.heavy == nil || s.stats == nil {
		return
	}
	stats, err := s.stats.Get(ctx, vloggerID)
	if err != nil {
		log.Printf("social service: heavy stats failed vlogger=%d: %v", vloggerID, err)
		return
	}
	if stats == nil || (!stats.IsBig && stats.FansCount < domainsocial.BigVFansThreshold) {
		return
	}
	if _, _, err := s.heavy.OnFollowBigV(ctx, followerID, vloggerID); err != nil {
		log.Printf("social service: heavy follow failed follower=%d vlogger=%d: %v", followerID, vloggerID, err)
	}
}

// updateHeavyOnUnfollow 在 MQ 不可用、同步落库时维护重度用户索引。
func (s *SocialService) updateHeavyOnUnfollow(ctx context.Context, followerID, vloggerID uint) {
	if s.heavy == nil {
		return
	}
	if err := s.heavy.OnUnfollowBigV(ctx, followerID, vloggerID); err != nil {
		log.Printf("social service: heavy unfollow failed follower=%d vlogger=%d: %v", followerID, vloggerID, err)
	}
}

// invalidateFollowing 清理关注列表缓存。
// 缓存失效失败不影响关注结果，缓存 TTL 会兜底。
func (s *SocialService) invalidateFollowing(ctx context.Context, followerID uint) {
	if s.followingCache == nil {
		return
	}
	_ = s.followingCache.Invalidate(ctx, followerID) // 失败由 TTL 兜底，不影响主流程
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
