package feed

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	"feedsystem_video_go/internal/infrastructure/workers"

	amqp "github.com/rabbitmq/amqp091-go"
)

// largeFanoutConsumerName 大粉丝量写扩散的消费幂等命名空间。
const largeFanoutConsumerName = "fanout_large"

// LargeFanoutExecutor 执行大粉丝量作者的写扩散。
// 生产实现为 feedapp.FanoutService，抽出接口便于单测。
type LargeFanoutExecutor interface {
	FanoutFollowers(ctx context.Context, eventID string, videoID, authorID uint, score int64) error
}

// LargeFanoutWorker 消费大粉丝量写扩散任务。
// 与普通 FanoutWorker 独立队列，避免一次大扩散阻塞其他作者的写扩散。
type LargeFanoutWorker struct {
	ch       *amqp.Channel
	executor LargeFanoutExecutor
	events   workers.EventIdempotency
	queue    string
}

// NewLargeFanoutWorker 创建大粉丝量写扩散 Worker。
func NewLargeFanoutWorker(ch *amqp.Channel, executor LargeFanoutExecutor, events workers.EventIdempotency, queue string) *LargeFanoutWorker {
	return &LargeFanoutWorker{ch: ch, executor: executor, events: events, queue: queue}
}

// Run 启动消费循环。
func (w *LargeFanoutWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.executor == nil {
		return errors.New("large fanout worker is not initialized")
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
				return errors.New("large fanout deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}

// handleDelivery 处理单条任务：失败进死信，避免无限重试占用队列。
func (w *LargeFanoutWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("large fanout worker: failed to process message: %v", err)
		_ = d.Nack(false, false)
		return
	}
	_ = d.Ack(false)
}

// process 解析任务并执行写扩散；中途失败时进度已保存，重投从断点继续。
func (w *LargeFanoutWorker) process(ctx context.Context, body []byte) error {
	var event rabbitmq.LargeFanoutEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return nil
	}
	if event.VideoID == 0 || event.AuthorID == 0 {
		return nil
	}
	if w.events != nil {
		first, err := w.events.TryMark(ctx, largeFanoutConsumerName, event.EventID)
		if err != nil {
			return err
		}
		if !first {
			return nil
		}
	}
	if err := w.executor.FanoutFollowers(ctx, event.EventID, event.VideoID, event.AuthorID, event.Score); err != nil {
		if w.events != nil {
			if unmarkErr := w.events.Unmark(ctx, largeFanoutConsumerName, event.EventID); unmarkErr != nil {
				log.Printf("large fanout worker: unmark failed event_id=%s: %v", event.EventID, unmarkErr)
			}
		}
		return err
	}
	return nil
}
