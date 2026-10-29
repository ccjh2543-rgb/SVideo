package video

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
	videocache "feedsystem_video_go/internal/infrastructure/cache/redis/video"
	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	"feedsystem_video_go/internal/infrastructure/workers"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// popularityBatchWindow 热度事件聚合窗口。
	popularityBatchWindow = 100 * time.Millisecond
	// popularityBatchSize 单批最多聚合的事件数。
	popularityBatchSize = 500
)

// popularityDelivery 一条待处理的热度消息。
type popularityDelivery struct {
	delivery amqp.Delivery
	event    rabbitmq.PopularityEvent
}

// PopularityWorker 热度事件消费者。
// 按时间窗口批量聚合，再一次性写入 Redis 候选桶，降低高 QPS 下的命令数。
type PopularityWorker struct {
	ch      *amqp.Channel
	cache   *rediscache.Client
	updater *videocache.PopularityCache
	events  workers.EventIdempotency
	queue   string
}

// NewPopularityWorker 创建热度消费者。
func NewPopularityWorker(ch *amqp.Channel, cache *rediscache.Client, events workers.EventIdempotency, queue string) *PopularityWorker {
	return &PopularityWorker{
		ch:      ch,
		cache:   cache,
		updater: videocache.NewPopularityCache(cache),
		events:  events,
		queue:   queue,
	}
}

// Run 消费热度事件，按窗口或批次大小触发聚合写入。
func (w *PopularityWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.cache == nil {
		return errors.New("popularity worker is not initialized")
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

	ticker := time.NewTicker(popularityBatchWindow)
	defer ticker.Stop()

	pending := make([]popularityDelivery, 0, popularityBatchSize)
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			w.flush(shutdownCtx, pending)
			cancel()
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				w.flush(shutdownCtx, pending)
				cancel()
				return errors.New("deliveries channel closed")
			}
			item, accepted := w.prepareDelivery(ctx, d)
			if !accepted {
				continue
			}
			pending = append(pending, item)
			if len(pending) >= popularityBatchSize {
				w.flush(ctx, pending)
				pending = pending[:0]
			}
		case <-ticker.C:
			if len(pending) == 0 {
				continue
			}
			w.flush(ctx, pending)
			pending = pending[:0]
		}
	}
}

// prepareDelivery 解析并做消费幂等检查。
// 返回 accepted=false 表示消息已确认或已拒绝，不需要进入批次。
func (w *PopularityWorker) prepareDelivery(ctx context.Context, d amqp.Delivery) (popularityDelivery, bool) {
	var evt rabbitmq.PopularityEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		// 解析失败的消息无法重试，直接确认丢弃。
		_ = d.Ack(false)
		return popularityDelivery{}, false
	}
	if evt.VideoID == 0 || evt.Change == 0 {
		_ = d.Ack(false)
		return popularityDelivery{}, false
	}
	if w.events != nil {
		first, err := w.events.TryMark(ctx, "popularity", evt.EventID)
		if err != nil {
			log.Printf("popularity worker: idempotency mark failed event_id=%s: %v", evt.EventID, err)
			_ = d.Nack(false, false)
			return popularityDelivery{}, false
		}
		if !first {
			_ = d.Ack(false)
			return popularityDelivery{}, false
		}
	}
	return popularityDelivery{delivery: d, event: evt}, true
}

// flush 将当前批次写入 Redis；失败时回滚幂等标记并拒绝整批消息。
func (w *PopularityWorker) flush(ctx context.Context, pending []popularityDelivery) {
	if len(pending) == 0 {
		return
	}

	updates := make([]domainfeed.HotUpdate, 0, len(pending))
	for _, item := range pending {
		occurredAt := item.event.OccurredAt
		if occurredAt.IsZero() {
			occurredAt = time.Now()
		}
		updates = append(updates, domainfeed.HotUpdate{
			VideoID:    item.event.VideoID,
			Change:     item.event.Change,
			OccurredAt: occurredAt,
			Weight:     1,
		})
	}

	if err := w.updater.UpdateBatch(ctx, updates); err != nil {
		log.Printf("popularity worker: batch update failed size=%d: %v", len(pending), err)
		w.rollbackAndReject(ctx, pending)
		return
	}

	for _, item := range pending {
		_ = item.delivery.Ack(false)
	}
}

// rollbackAndReject 回滚幂等标记，并把消息拒绝到 DLQ，避免批次部分成功。
func (w *PopularityWorker) rollbackAndReject(ctx context.Context, pending []popularityDelivery) {
	for _, item := range pending {
		if w.events != nil {
			if err := w.events.Unmark(ctx, "popularity", item.event.EventID); err != nil {
				log.Printf("popularity worker: unmark failed event_id=%s: %v", item.event.EventID, err)
			}
		}
		_ = item.delivery.Nack(false, false)
	}
}
