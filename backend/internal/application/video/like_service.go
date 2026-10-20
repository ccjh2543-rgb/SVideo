package video

import (
	"context"
	"errors"
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"
)

// LikeService 点赞应用服务。
type LikeService struct {
	likes            domainvideo.LikeRepository
	videos           domainvideo.VideoRepository
	likeEvents       LikeEventPublisher
	popularityEvents PopularityUpdater
	popularityCache  PopularityUpdater
}

// NewLikeService 创建点赞应用服务。
func NewLikeService(
	likes domainvideo.LikeRepository,
	videos domainvideo.VideoRepository,
	likeEvents LikeEventPublisher,
	popularityEvents PopularityUpdater,
	popularityCache PopularityUpdater,
) *LikeService {
	return &LikeService{
		likes:            likes,
		videos:           videos,
		likeEvents:       likeEvents,
		popularityEvents: popularityEvents,
		popularityCache:  popularityCache,
	}
}

// Like 处理点赞请求，优先走 MQ，失败时同步降级。
func (s *LikeService) Like(ctx context.Context, like *domainvideo.Like) error {
	if like == nil || like.VideoID == 0 || like.AccountID == 0 {
		return errors.New("video_id and account_id are required")
	}
	ok, err := s.videos.IsExist(ctx, like.VideoID)
	if err != nil {
		return err
	}
	if !ok {
		return domainvideo.ErrVideoNotFound
	}
	liked, err := s.likes.IsLiked(ctx, like.VideoID, like.AccountID)
	if err != nil {
		return err
	}
	if liked {
		return domainvideo.ErrAlreadyLiked
	}
	like.CreatedAt = time.Now()

	mysqlEnqueued := false
	redisEnqueued := false
	if s.likeEvents != nil {
		if err := s.likeEvents.Like(ctx, like.AccountID, like.VideoID); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityEvents != nil {
		if err := s.popularityEvents.Update(ctx, like.VideoID, 1); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	if !mysqlEnqueued {
		if _, err := s.likes.LikeWithCounters(ctx, like.VideoID, like.AccountID); err != nil {
			return err
		}
	}
	if !redisEnqueued && s.popularityCache != nil {
		_ = s.popularityCache.Update(ctx, like.VideoID, 1)
	}
	return nil
}

// Unlike 处理取消点赞请求。
func (s *LikeService) Unlike(ctx context.Context, like *domainvideo.Like) error {
	if like == nil || like.VideoID == 0 || like.AccountID == 0 {
		return errors.New("video_id and account_id are required")
	}
	ok, err := s.videos.IsExist(ctx, like.VideoID)
	if err != nil {
		return err
	}
	if !ok {
		return domainvideo.ErrVideoNotFound
	}
	liked, err := s.likes.IsLiked(ctx, like.VideoID, like.AccountID)
	if err != nil {
		return err
	}
	if !liked {
		return domainvideo.ErrNotLiked
	}

	mysqlEnqueued := false
	redisEnqueued := false
	if s.likeEvents != nil {
		if err := s.likeEvents.Unlike(ctx, like.AccountID, like.VideoID); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityEvents != nil {
		if err := s.popularityEvents.Update(ctx, like.VideoID, -1); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	if !mysqlEnqueued {
		if _, err := s.likes.UnlikeWithCounters(ctx, like.VideoID, like.AccountID); err != nil {
			return err
		}
	}
	if !redisEnqueued && s.popularityCache != nil {
		_ = s.popularityCache.Update(ctx, like.VideoID, -1)
	}
	return nil
}

// IsLiked 查询点赞状态。
func (s *LikeService) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	return s.likes.IsLiked(ctx, videoID, accountID)
}

// ListLikedVideos 查询用户点赞过的视频。
func (s *LikeService) ListLikedVideos(ctx context.Context, accountID uint) ([]domainvideo.Video, error) {
	return s.likes.ListLikedVideos(ctx, accountID)
}
