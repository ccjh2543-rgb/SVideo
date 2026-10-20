package worker

import (
	"context"
	"encoding/json"
	"errors"
	domainnotification "feedsystem_video_go/internal/domain/notification"
	domainvideo "feedsystem_video_go/internal/domain/video"
	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type LikeWorker struct {
	ch       *amqp.Channel
	likes    domainvideo.LikeRepository
	videos   domainvideo.VideoRepository
	events   EventIdempotency
	notifier domainnotification.Notifier
	queue    string
}

func NewLikeWorker(
	ch *amqp.Channel,
	likes domainvideo.LikeRepository,
	videos domainvideo.VideoRepository,
	events EventIdempotency,
	notifier domainnotification.Notifier,
	queue string,
) *LikeWorker {
	return &LikeWorker{ch: ch, likes: likes, videos: videos, events: events, notifier: notifier, queue: queue}
}

func (w *LikeWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.likes == nil || w.videos == nil {
		return errors.New("like worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}

	deliveries, err := w.ch.Consume(
		w.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}

func (w *LikeWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("like worker: failed to process message: %v", err)
		// 失败消息进入死信队列，避免无限 requeue 热循环。
		_ = d.Nack(false, false)
		return
	}
	_ = d.Ack(false)
}

func (w *LikeWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.LikeEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		// 解析事件失败，直接丢弃
		return nil
	}
	if evt.UserID == 0 || evt.VideoID == 0 {
		return nil
	}
	if w.events != nil {
		first, err := w.events.TryMark(ctx, "like", evt.EventID)
		if err != nil {
			return err
		}
		if !first {
			return nil
		}
	}

	switch evt.Action {
	case "like":
		return w.applyLike(ctx, evt.UserID, evt.VideoID, evt.EventID)
	case "unlike":
		return w.applyUnlike(ctx, evt.UserID, evt.VideoID)
	default:
		return nil
	}
}

func (w *LikeWorker) applyLike(ctx context.Context, userID, videoID uint, eventID string) error {
	ok, err := w.videos.IsExist(ctx, videoID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	created, err := w.likes.LikeIgnoreDuplicate(ctx, &domainvideo.Like{
		VideoID:   videoID,
		AccountID: userID,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return err
	}
	if !created {
		return nil
	}

	if err := w.videos.ChangeLikesCount(ctx, videoID, 1); err != nil {
		return err
	}
	if err := w.videos.ChangePopularity(ctx, videoID, 1); err != nil {
		return err
	}

	if w.notifier != nil {
		item, err := w.videos.GetByID(ctx, videoID)
		if err == nil && item != nil && item.AuthorID != userID {
			_ = w.notifier.Publish(ctx, domainnotification.Event{
				EventID:   eventID,
				AccountID: item.AuthorID,
				Type:      "like",
				Title:     "收到新的点赞",
				Body:      "有人赞了你的视频",
				Data:      map[string]any{"video_id": videoID, "from_account_id": userID},
			})
		}
	}
	return nil
}

func (w *LikeWorker) applyUnlike(ctx context.Context, userID, videoID uint) error {
	ok, err := w.videos.IsExist(ctx, videoID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	deleted, err := w.likes.DeleteByVideoAndAccount(ctx, videoID, userID)
	if err != nil {
		return err
	}
	if !deleted {
		return nil
	}

	if err := w.videos.ChangeLikesCount(ctx, videoID, -1); err != nil {
		return err
	}
	return w.videos.ChangePopularity(ctx, videoID, -1)
}
