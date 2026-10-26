package video

import (
	"context"
	"encoding/json"
	"errors"
	domainnotification "feedsystem_video_go/internal/domain/notification"
	domainvideo "feedsystem_video_go/internal/domain/video"
	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	"feedsystem_video_go/internal/infrastructure/workers"
	"log"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type CommentWorker struct {
	ch       *amqp.Channel
	comments domainvideo.CommentRepository
	videos   domainvideo.VideoRepository
	events   workers.EventIdempotency
	notifier domainnotification.Notifier
	queue    string
}

func NewCommentWorker(
	ch *amqp.Channel,
	comments domainvideo.CommentRepository,
	videos domainvideo.VideoRepository,
	events workers.EventIdempotency,
	notifier domainnotification.Notifier,
	queue string,
) *CommentWorker {
	return &CommentWorker{ch: ch, comments: comments, videos: videos, events: events, notifier: notifier, queue: queue}
}

func (w *CommentWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.comments == nil || w.videos == nil {
		return errors.New("comment worker is not initialized")
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

func (w *CommentWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("comment worker: failed to process message: %v", err)
		_ = d.Nack(false, false)
		return
	}
	_ = d.Ack(false)
}

func (w *CommentWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.CommentEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	if w.events != nil {
		first, err := w.events.TryMark(ctx, "comment", evt.EventID)
		if err != nil {
			return err
		}
		if !first {
			return nil
		}
	}
	var err error
	switch evt.Action {
	case "publish":
		err = w.applyPublish(ctx, &evt)
	case "delete":
		err = w.applyDelete(ctx, &evt)
	default:
		return nil
	}
	if err != nil && w.events != nil {
		// 业务失败时删除幂等标记，保证消息重放可以重新处理。
		if unmarkErr := w.events.Unmark(ctx, "comment", evt.EventID); unmarkErr != nil {
			log.Printf("comment worker: unmark failed event_id=%s: %v", evt.EventID, unmarkErr)
		}
	}
	return err
}

func (w *CommentWorker) applyPublish(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	if evt == nil || evt.VideoID == 0 || evt.AuthorID == 0 || strings.TrimSpace(evt.Content) == "" {
		return nil
	}

	c := &domainvideo.Comment{
		Username: strings.TrimSpace(evt.Username),
		VideoID:  evt.VideoID,
		AuthorID: evt.AuthorID,
		Content:  strings.TrimSpace(evt.Content),
	}
	// 评论写入与热度更新放在同一事务，避免“评论成功但热度没加”。
	if err := w.comments.CreateWithPopularity(ctx, c); err != nil {
		if errors.Is(err, domainvideo.ErrVideoNotFound) {
			return nil
		}
		return err
	}

	if w.notifier != nil {
		item, err := w.videos.GetByID(ctx, evt.VideoID)
		if err == nil && item != nil && item.AuthorID != evt.AuthorID {
			_ = w.notifier.Publish(ctx, domainnotification.Event{
				EventID:   evt.EventID,
				AccountID: item.AuthorID,
				Type:      "comment",
				Title:     "收到新的评论",
				Body:      strings.TrimSpace(evt.Content),
				Data:      map[string]any{"video_id": evt.VideoID, "from_account_id": evt.AuthorID},
			})
		}
	}
	return nil
}

func (w *CommentWorker) applyDelete(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	if evt == nil || evt.CommentID == 0 {
		return nil
	}
	c, err := w.comments.GetByID(ctx, evt.CommentID)
	if err != nil {
		return err
	}
	if c == nil {
		return nil
	}
	return w.comments.Delete(ctx, c)
}
