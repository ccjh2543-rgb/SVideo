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

// heavyPushConsumerName 重度推送消费幂等命名空间。
const heavyPushConsumerName = "heavy_push"

// HeavyPushExecutor 执行重度粉丝推送。
// 生产实现为 feedapp.HeavyPushService，抽出接口便于单测。
type HeavyPushExecutor interface {
	Push(ctx context.Context, videoID, authorID uint, score int64) error
}

// HeavyPushWorker 消费重度推送任务，把大 V 视频写入重度粉丝 Inbox。
// 与 FanoutWorker 独立队列，避免大 V 的重度粉丝推送阻塞普通写扩散。
type HeavyPushWorker struct {
	ch       *amqp.Channel
	executor HeavyPushExecutor
	events   workers.EventIdempotency
	queue    string
}

// NewHeavyPushWorker 创建重度推送 Worker。
func NewHeavyPushWorker(ch *amqp.Channel, executor HeavyPushExecutor, events workers.EventIdempotency, queue string) *HeavyPushWorker {
	return &HeavyPushWorker{ch: ch, executor: executor, events: events, queue: queue}
}

// Run 启动消费循环。
func (w *HeavyPushWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.executor == nil {
		return errors.New("heavy push worker is not initialized")
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
				return errors.New("heavy push deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}

// handleDelivery 处理单条任务：失败投递死信，避免无限重试占用队列。
func (w *HeavyPushWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("heavy push worker: failed to process message: %v", err)
		_ = d.Nack(false, false)
		return
	}
	_ = d.Ack(false)
}

// process 解析任务并执行推送，成功后保留幂等标记。
func (w *HeavyPushWorker) process(ctx context.Context, body []byte) error {
	var event rabbitmq.HeavyPushEvent
	if err := json.Unmarshal(body, &event); err != nil {
		// 解析失败说明消息已损坏，重试也不会成功。
		return nil
	}
	if event.VideoID == 0 || event.AuthorID == 0 {
		return nil
	}
	if w.events != nil {
		first, err := w.events.TryMark(ctx, heavyPushConsumerName, event.EventID)
		if err != nil {
			return err
		}
		if !first {
			return nil
		}
	}
	if err := w.executor.Push(ctx, event.VideoID, event.AuthorID, event.Score); err != nil {
		// 业务失败时删除幂等标记，保证死信重放可以重新处理。
		if w.events != nil {
			if unmarkErr := w.events.Unmark(ctx, heavyPushConsumerName, event.EventID); unmarkErr != nil {
				log.Printf("heavy push worker: unmark failed event_id=%s: %v", event.EventID, unmarkErr)
			}
		}
		return err
	}
	return nil
}
