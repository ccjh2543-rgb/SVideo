package feed

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	feedapp "feedsystem_video_go/internal/application/feed"
	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	"feedsystem_video_go/internal/infrastructure/workers"

	amqp "github.com/rabbitmq/amqp091-go"
)

// defaultFanoutConcurrency 写扩散默认并发处理的消息数。
// 单 goroutine 时一条大粉丝量消息会阻塞后续所有消息，工作池保证不同作者互不阻塞。
const defaultFanoutConcurrency = 4

// FanoutWorker 消费视频发布事件，执行推拉结合写扩散。
type FanoutWorker struct {
	ch          *amqp.Channel
	service     *feedapp.FanoutService
	events      workers.EventIdempotency
	queue       string
	concurrency int
}

// NewFanoutWorker 创建写扩散 Worker。
func NewFanoutWorker(ch *amqp.Channel, service *feedapp.FanoutService, events workers.EventIdempotency, queue string) *FanoutWorker {
	return &FanoutWorker{ch: ch, service: service, events: events, queue: queue, concurrency: defaultFanoutConcurrency}
}

// SetConcurrency 设置并发处理的消息数，n <= 0 时使用默认值。
func (w *FanoutWorker) SetConcurrency(n int) {
	if w == nil || n <= 0 {
		return
	}
	w.concurrency = n
}

// Run 启动消费循环与工作池。
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

	concurrency := w.concurrency
	if concurrency <= 0 {
		concurrency = defaultFanoutConcurrency
	}
	jobs := make(chan amqp.Delivery)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range jobs {
				w.handleDelivery(ctx, d)
			}
		}()
	}
	// 退出时先关闭任务队列，等待正在处理的消息结束，避免 Ack/Nack 丢失。
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("fanout deliveries channel closed")
			}
			select {
			case jobs <- d:
			case <-ctx.Done():
				return ctx.Err()
			}
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
	if err := w.service.Handle(ctx, event.EventID, event.VideoID, event.AuthorID, time.UnixMilli(event.CreateTime)); err != nil {
		// 业务失败时删除幂等标记，保证 DLQ 重放或补偿任务能够重新处理。
		if w.events != nil {
			if unmarkErr := w.events.Unmark(ctx, "fanout", event.EventID); unmarkErr != nil {
				log.Printf("fanout worker: unmark failed event_id=%s: %v", event.EventID, unmarkErr)
			}
		}
		return err
	}
	return nil
}
