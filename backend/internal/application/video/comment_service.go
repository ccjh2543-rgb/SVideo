package video

import (
	"context"

	domainvideo "feedsystem_video_go/internal/domain/video"
)

// CommentService 评论应用服务。
type CommentService struct {
	comments         domainvideo.CommentRepository
	videos           domainvideo.VideoRepository
	commentEvents    CommentEventPublisher
	popularityEvents PopularityUpdater
	popularityCache  PopularityUpdater
}

// NewCommentService 创建评论应用服务。
func NewCommentService(
	comments domainvideo.CommentRepository,
	videos domainvideo.VideoRepository,
	commentEvents CommentEventPublisher,
	popularityEvents PopularityUpdater,
	popularityCache PopularityUpdater,
) *CommentService {
	return &CommentService{
		comments:         comments,
		videos:           videos,
		commentEvents:    commentEvents,
		popularityEvents: popularityEvents,
		popularityCache:  popularityCache,
	}
}

// Publish 发布评论，优先走 MQ，失败时同步降级。
func (s *CommentService) Publish(ctx context.Context, comment *domainvideo.Comment) error {
	if err := comment.Validate(); err != nil {
		return err
	}
	exists, err := s.videos.IsExist(ctx, comment.VideoID)
	if err != nil {
		return err
	}
	if !exists {
		return domainvideo.ErrVideoNotFound
	}

	mysqlEnqueued := false
	redisEnqueued := false
	if s.commentEvents != nil {
		if err := s.commentEvents.Publish(ctx, comment.Username, comment.VideoID, comment.AuthorID, comment.Content); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityEvents != nil {
		if err := s.popularityEvents.Update(ctx, comment.VideoID, 1); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	if !mysqlEnqueued {
		if err := s.comments.CreateWithPopularity(ctx, comment); err != nil {
			return err
		}
	}
	if !redisEnqueued && s.popularityCache != nil {
		_ = s.popularityCache.Update(ctx, comment.VideoID, 1)
	}
	return nil
}

// Delete 删除评论。
func (s *CommentService) Delete(ctx context.Context, commentID uint, accountID uint) error {
	comment, err := s.comments.GetByID(ctx, commentID)
	if err != nil {
		return err
	}
	if comment == nil {
		return domainvideo.ErrCommentNotFound
	}
	if comment.AuthorID != accountID {
		return domainvideo.ErrPermissionDenied
	}

	if s.commentEvents != nil {
		if err := s.commentEvents.Delete(ctx, commentID); err == nil {
			if s.popularityEvents != nil {
				_ = s.popularityEvents.Update(ctx, comment.VideoID, -1)
			}
			return nil
		}
	}
	if err := s.comments.Delete(ctx, comment); err != nil {
		return err
	}
	if s.popularityCache != nil {
		_ = s.popularityCache.Update(ctx, comment.VideoID, -1)
	}
	return nil
}

// GetAll 查询视频评论。
func (s *CommentService) GetAll(ctx context.Context, videoID uint) ([]domainvideo.Comment, error) {
	exists, err := s.videos.IsExist(ctx, videoID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, domainvideo.ErrVideoNotFound
	}
	return s.comments.GetAll(ctx, videoID)
}
