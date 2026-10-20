package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	feedapp "feedsystem_video_go/internal/application/feed"
	"feedsystem_video_go/internal/middleware/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

// FanoutWorker 消费视频发布事件，执行推拉结合写扩散。
type FanoutWorker struct {
	ch      *amqp.Channel
	service *feedapp.FanoutService
	events  EventIdempotency
	queue   string
}

// NewFanoutWorker 创建写扩散 Worker。
func NewFanoutWorker(ch *amqp.Channel, service *feedapp.FanoutService, events EventIdempotency, queue string) *FanoutWorker {
	return &FanoutWorker{ch: ch, service: service, events: events, queue: queue}
}

// Run 启动消费循环。
func (w *FanoutWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.service == nil {
		return errors.New("fanout worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}

	deliveries, err := w.ch.Consume(w.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("fanout deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}

func (w *FanoutWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("fanout worker: failed to process message: %v", err)
		_ = d.Nack(false, false)
		return
	}
	_ = d.Ack(false)
}

func (w *FanoutWorker) process(ctx context.Context, body []byte) error {
	var event rabbitmq.TimelineEvent
	if err := json.Unmarshal(body, &event); err != nil {
		// 无法解析的消息重试也不会成功，直接丢弃。
		return nil
	}
	if event.VideoID == 0 || event.AuthorID == 0 {
		log.Printf("fanout worker: skip invalid event video_id=%d author_id=%d", event.VideoID, event.AuthorID)
		return nil
	}
	if w.events != nil {
		first, err := w.events.TryMark(ctx, "fanout", event.EventID)
		if err != nil {
			return err
		}
		if !first {
			return nil
		}
	}
	return w.service.Handle(ctx, event.VideoID, event.AuthorID, time.UnixMilli(event.CreateTime))
}
