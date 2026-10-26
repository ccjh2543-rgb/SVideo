package feed

import (
	"context"
	"encoding/json"
	"feedsystem_video_go/internal/domain/video"
	"feedsystem_video_go/internal/infrastructure/cache/redis"
	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	"fmt"
	"log"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	oredis "github.com/redis/go-redis/v9"
)

// OutboxPoller 定期轮询本地消息表（outbox），将 pending 状态的消息投递到 MQ。
type OutboxPoller struct {
	outbox   video.OutboxRepository
	tmq      *rabbitmq.TimelineMQ
	interval time.Duration
}

// NewOutboxPoller 创建 OutboxPoller 实例。
func NewOutboxPoller(outbox video.OutboxRepository, tmq *rabbitmq.TimelineMQ) *OutboxPoller {
	return &OutboxPoller{
		outbox:   outbox,
		tmq:      tmq,
		interval: 1 * time.Second,
	}
}

// Run 启动轮询循环，直到 ctx 被取消。
func (p *OutboxPoller) Run(ctx context.Context) error {
	if p.tmq == nil {
		log.Println("TimelineMQ is nil, outbox poller skipped")
		return nil
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *OutboxPoller) poll(ctx context.Context) {
	now := time.Now()
	messages, err := p.outbox.ListPending(ctx, now, 100)
	if err != nil || len(messages) == 0 {
		return
	}

	for _, msg := range messages {
		// 使用 outbox 消息 ID 作为事件 ID：重复投递时消费端幂等表可以拦截。
		eventID := strconv.FormatUint(uint64(msg.ID), 10)
		err := p.tmq.PublishVideo(context.Background(), eventID, msg.VideoID, msg.AuthorID, msg.CreateTime)
		if err == nil {
			_ = p.outbox.Delete(ctx, msg.ID)
			continue
		}

		log.Printf("投递MQ失败: VideoID: %d, err: %v", msg.VideoID, err)
		retryCount := msg.RetryCount + 1
		if retryCount >= 3 {
			_ = p.outbox.MarkRetry(ctx, msg.ID, retryCount, "failed", nil)
		} else {
			next := now.Add(time.Duration(1<<retryCount) * time.Second)
			_ = p.outbox.MarkRetry(ctx, msg.ID, retryCount, "pending", &next)
		}
	}
}

// TimelineConsumer 消费 Timeline 消息并将视频 ID 写入 Redis ZSET。
type TimelineConsumer struct {
	tmq       *rabbitmq.TimelineMQ
	queueName string
	cache     *redis.Client
}

// NewTimelineConsumer 创建 TimelineConsumer 实例。
func NewTimelineConsumer(tmq *rabbitmq.TimelineMQ, queueName string, cache *redis.Client) *TimelineConsumer {
	return &TimelineConsumer{tmq: tmq, queueName: queueName, cache: cache}
}

// Run 启动消费循环，直到 ctx 被取消或 Channel 关闭。
func (c *TimelineConsumer) Run(ctx context.Context) error {
	if c.tmq == nil || c.tmq.Ch == nil {
		log.Println("TimelineMQ is nil, consumer skipped")
		return nil
	}

	msgs, err := c.tmq.Ch.Consume(
		c.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("注册消费失败: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("timeline consumer: deliveries channel closed")
			}
			c.handleDelivery(ctx, msg)
		}
	}
}

func (c *TimelineConsumer) handleDelivery(ctx context.Context, msg amqp.Delivery) {
	var event rabbitmq.TimelineEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Printf("反序列化失败: %v", err)
		_ = msg.Ack(false)
		return
	}

	writeCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	timelineKey := "feed:global_timeline"
	err := c.cache.ZAdd(writeCtx, timelineKey, oredis.Z{
		Score:  float64(event.CreateTime),
		Member: fmt.Sprintf("%d", event.VideoID),
	})
	if err != nil {
		log.Printf("写入Zset失败: %v", err)
		_ = msg.Nack(false, false)
		return
	}

	// 保持 ZSET 最多 1000 条，删除最旧的多余条目
	_ = c.cache.ZRemRangeByRank(writeCtx, timelineKey, 0, -1001)

	_ = msg.Ack(false)
}

// StartOutboxPoller 启动 Outbox 轮询器。
func StartOutboxPoller(outbox video.OutboxRepository, tmq *rabbitmq.TimelineMQ) {
	poller := NewOutboxPoller(outbox, tmq)
	go func() {
		_ = poller.Run(context.Background())
	}()
}

// Deprecated: 使用 NewTimelineConsumer 和 Run 方法替代。
func StartConsumer(tmq *rabbitmq.TimelineMQ, queueName string, redisClient *redis.Client) {
	consumer := NewTimelineConsumer(tmq, queueName, redisClient)
	go func() {
		_ = consumer.Run(context.Background())
	}()
}
