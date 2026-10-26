package video

import (
	"context"
	"errors"
	"time"
)

var (
	ErrVideoInvalid           = errors.New("video is invalid")
	ErrTitleRequired          = errors.New("title is required")
	ErrPlayURLRequired        = errors.New("play url is required")
	ErrCoverURLRequired       = errors.New("cover url is required")
	ErrVideoNotFound          = errors.New("video not found")
	ErrUnauthorized           = errors.New("unauthorized")
	ErrAlreadyLiked           = errors.New("user has liked this video")
	ErrNotLiked               = errors.New("user has not liked this video")
	ErrCommentInvalid         = errors.New("comment is invalid")
	ErrCommentRequired        = errors.New("video_id and author_id are required")
	ErrCommentContentRequired = errors.New("content is required")
	ErrCommentNotFound        = errors.New("comment not found")
	ErrPermissionDenied       = errors.New("permission denied")
)

// VideoRepository 视频仓储接口。
type VideoRepository interface {
	CreateWithOutbox(ctx context.Context, video *Video, outbox *OutboxMessage) error
	GetByID(ctx context.Context, id uint) (*Video, error)
	Delete(ctx context.Context, id uint) error
	ListByAuthorID(ctx context.Context, authorID uint) ([]Video, error)
	ListIDsAfter(ctx context.Context, afterID uint, limit int) ([]uint, error)
	IsExist(ctx context.Context, id uint) (bool, error)
	UpdateLikesCount(ctx context.Context, id uint, likesCount int64) error
	ChangeLikesCount(ctx context.Context, id uint, change int64) error
	ChangePopularity(ctx context.Context, id uint, change int64) error
}

// LikeRepository 点赞仓储接口。
type LikeRepository interface {
	Like(ctx context.Context, like *Like) error
	Unlike(ctx context.Context, like *Like) error
	LikeIgnoreDuplicate(ctx context.Context, like *Like) (bool, error)
	DeleteByVideoAndAccount(ctx context.Context, videoID, accountID uint) (bool, error)
	IsLiked(ctx context.Context, videoID, accountID uint) (bool, error)
	BatchGetLiked(ctx context.Context, videoIDs []uint, accountID uint) (map[uint]bool, error)
	ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error)

	// LikeWithCounters 在同一个事务内完成点赞、计数和热度更新，作为 MQ 不可用时的降级路径。
	LikeWithCounters(ctx context.Context, videoID, accountID uint) (bool, error)
	// UnlikeWithCounters 在同一个事务内完成取消点赞、计数和热度更新。
	UnlikeWithCounters(ctx context.Context, videoID, accountID uint) (bool, error)
}

// CommentRepository 评论仓储接口。
type CommentRepository interface {
	Create(ctx context.Context, comment *Comment) error
	// CreateWithPopularity 在同一个事务内写评论并增加热度，作为 MQ 不可用时的降级路径。
	CreateWithPopularity(ctx context.Context, comment *Comment) error
	Delete(ctx context.Context, comment *Comment) error
	GetAll(ctx context.Context, videoID uint) ([]Comment, error)
	IsExist(ctx context.Context, id uint) (bool, error)
	GetByID(ctx context.Context, id uint) (*Comment, error)
}

// OutboxRepository 事务消息仓储接口。
type OutboxRepository interface {
	ListPending(ctx context.Context, now time.Time, limit int) ([]OutboxMessage, error)
	Delete(ctx context.Context, id uint) error
	MarkRetry(ctx context.Context, id uint, retryCount int, status string, nextRetry *time.Time) error
	// RequeueFailed 把失败消息重新置为待投递，用于补偿任务。
	RequeueFailed(ctx context.Context, olderThan time.Time, limit int) (int64, error)
}
