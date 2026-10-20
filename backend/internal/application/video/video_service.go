// Package video 是视频上下文应用层，负责发布、详情、点赞和评论用例编排。
package video

import (
	"context"
	"errors"
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"
)

// VideoService 视频应用服务。
type VideoService struct {
	videos domainvideo.VideoRepository
	cache  DetailCache
}

// NewVideoService 创建视频应用服务。
func NewVideoService(videos domainvideo.VideoRepository, cache DetailCache) *VideoService {
	return &VideoService{videos: videos, cache: cache}
}

// Publish 发布视频，并在同一事务中写入 Outbox 消息。
func (s *VideoService) Publish(ctx context.Context, video *domainvideo.Video) error {
	if err := video.ValidateForPublish(); err != nil {
		return err
	}
	if video.CreateTime.IsZero() {
		video.CreateTime = time.Now()
	}
	outbox := &domainvideo.OutboxMessage{
		AuthorID:   video.AuthorID,
		EventType:  "video_published",
		CreateTime: video.CreateTime,
		Status:     "pending",
	}
	if err := s.videos.CreateWithOutbox(ctx, video, outbox); err != nil {
		return err
	}
	if s.cache != nil {
		s.cache.BloomAdd(ctx, video.ID)
	}
	return nil
}

// Delete 删除视频。
func (s *VideoService) Delete(ctx context.Context, id uint, authorID uint) error {
	video, err := s.videos.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if video.AuthorID != authorID {
		return domainvideo.ErrUnauthorized
	}
	if err := s.videos.Delete(ctx, id); err != nil {
		return err
	}
	if s.cache != nil {
		s.cache.Delete(ctx, id)
	}
	return nil
}

// ListByAuthorID 查询作者的视频列表。
func (s *VideoService) ListByAuthorID(ctx context.Context, authorID uint) ([]domainvideo.Video, error) {
	return s.videos.ListByAuthorID(ctx, authorID)
}

// GetDetail 查询视频详情。
//
// 缓存策略：布隆过滤器、空值缓存、逻辑过期、分布式锁。
func (s *VideoService) GetDetail(ctx context.Context, id uint) (*domainvideo.Video, error) {
	if id == 0 {
		return nil, domainvideo.ErrVideoInvalid
	}
	if s.cache != nil {
		if exists, err := s.cache.BloomExists(ctx, id); err == nil && !exists {
			return nil, domainvideo.ErrVideoNotFound
		}
		if video, expired, found := s.cache.Get(ctx, id); found {
			if video == nil {
				return nil, domainvideo.ErrVideoNotFound
			}
			if expired {
				go s.refreshDetail(id)
			}
			return video, nil
		}

		if token, locked := s.cache.Lock(ctx, id); locked {
			defer s.cache.Unlock(context.Background(), id, token)
			if video, _, found := s.cache.Get(ctx, id); found {
				if video == nil {
					return nil, domainvideo.ErrVideoNotFound
				}
				return video, nil
			}
			video, err := s.videos.GetByID(ctx, id)
			if err != nil {
				if errors.Is(err, domainvideo.ErrVideoNotFound) {
					s.cache.SetNull(ctx, id)
				}
				return nil, err
			}
			s.cache.Set(ctx, video)
			return video, nil
		}

		for i := 0; i < 5; i++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			if video, _, found := s.cache.Get(ctx, id); found {
				if video == nil {
					return nil, domainvideo.ErrVideoNotFound
				}
				return video, nil
			}
		}
	}

	video, err := s.videos.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, domainvideo.ErrVideoNotFound) && s.cache != nil {
			s.cache.SetNull(ctx, id)
		}
		return nil, err
	}
	if s.cache != nil {
		s.cache.Set(ctx, video)
	}
	return video, nil
}

func (s *VideoService) refreshDetail(id uint) {
	if s.cache == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	token, locked := s.cache.Lock(ctx, id)
	if !locked {
		return
	}
	defer s.cache.Unlock(context.Background(), id, token)

	if _, expired, found := s.cache.Get(ctx, id); found && !expired {
		return
	}
	video, err := s.videos.GetByID(ctx, id)
	if err != nil {
		return
	}
	s.cache.Set(ctx, video)
}

// RebuildBloom 重建视频布隆过滤器。
func (s *VideoService) RebuildBloom(ctx context.Context) error {
	if s.cache == nil {
		return nil
	}
	return s.cache.RebuildBloom(ctx, func(afterID uint, limit int) ([]uint, error) {
		return s.videos.ListIDsAfter(ctx, afterID, limit)
	})
}
