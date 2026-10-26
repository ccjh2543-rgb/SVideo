// Package feed 提供 Feed 相关后台任务：Outbox 补偿与死信重放。
package feed

import (
	"context"
	"log"
	"strconv"
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// defaultCompensateInterval 补偿任务扫描间隔。
	defaultCompensateInterval = 30 * time.Second
	// defaultCompensateDelay 只补偿失败超过该时长的消息，避免刚失败就被反复重投。
	defaultCompensateDelay = time.Minute
	// defaultCompensateBatch 单轮最多补偿的消息数。
	defaultCompensateBatch = 100
	// defaultMaxReplay 死信消息最多重放次数，超过后丢弃并告警，避免毒消息死循环。
	defaultMaxReplay = 2
	// replayCountHeader 记录死信重放次数。
	replayCountHeader = "x-replay-count"
)

// OutboxCompensator 定期把 failed 状态的 outbox 消息重新置为 pending。
// 解决“三次投递失败后永久不再投递”的问题。
type OutboxCompensator struct {
	outbox   domainvideo.OutboxRepository
	interval time.Duration
	delay    time.Duration
	batch    int
}

// NewOutboxCompensator 创建 Outbox 补偿任务。
func NewOutboxCompensator(outbox domainvideo.OutboxRepository) *OutboxCompensator {
	return &OutboxCompensator{
		outbox:   outbox,
		interval: defaultCompensateInterval,
		delay:    defaultCompensateDelay,
		batch:    defaultCompensateBatch,
	}
}

// Run 启动补偿循环，直到 ctx 取消。
func (c *OutboxCompensator) Run(ctx context.Context) error {
	if c == nil || c.outbox == nil {
		return nil
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.requeue(ctx)
		}
	}
}

// requeue 执行一轮补偿。
func (c *OutboxCompensator) requeue(ctx context.Context) {
	olderThan := time.Now().Add(-c.delay)
	count, err := c.outbox.RequeueFailed(ctx, olderThan, c.batch)
	if err != nil {
		log.Printf("outbox compensator: requeue failed: %v", err)
		return
	}
	if count > 0 {
		log.Printf("outbox compensator: requeued %d failed messages", count)
	}
}

// TimelineRawPublisher 按原始消息体重新投递的发布器。
type TimelineRawPublisher interface {
	PublishRaw(ctx context.Context, body []byte, headers amqp.Table) error
}

// DLQReplayer 消费死信队列并重新投递到时间线交换机。
// 采用“最多重放 N 次”的策略，避免毒消息在两个队列之间无限循环。
type DLQReplayer struct {
	ch        *amqp.Channel
	publisher TimelineRawPublisher
	queue     string
	maxReplay int
}

// NewDLQReplayer 创建死信重放任务。
func NewDLQReplayer(ch *amqp.Channel, publisher TimelineRawPublisher, queue string) *DLQReplayer {
	return &DLQReplayer{
		ch:        ch,
		publisher: publisher,
		queue:     queue,
		maxReplay: defaultMaxReplay,
	}
}

// Run 启动死信消费循环。
func (r *DLQReplayer) Run(ctx context.Context) error {
	if r == nil || r.ch == nil || r.publisher == nil || r.queue == "" {
		return nil
	}
	deliveries, err := r.ch.Consume(r.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			r.handle(ctx, d)
		}
	}
}

// handle 处理单条死信：重放一次或丢弃。
func (r *DLQReplayer) handle(ctx context.Context, d amqp.Delivery) {
	count := replayCount(d.Headers)
	if count >= r.maxReplay {
		log.Printf("dlq replayer: drop poison message queue=%s replay=%d", r.queue, count)
		_ = d.Ack(false)
		return
	}

	headers := make(amqp.Table, len(d.Headers)+1)
	for key, value := range d.Headers {
		headers[key] = value
	}
	headers[replayCountHeader] = int32(count + 1)

	// 重放投递设置超时，避免 MQ 流控时死信消费者被单条消息卡住。
	publishCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := r.publisher.PublishRaw(publishCtx, d.Body, headers); err != nil {
		log.Printf("dlq replayer: republish failed queue=%s: %v", r.queue, err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

// replayCount 解析死信重放次数。
func replayCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	switch value := headers[replayCountHeader].(type) {
	case int:
		return value
	case int32:
		return int(value)
	case int64:
		return int(value)
	case string:
		count, err := strconv.Atoi(value)
		if err == nil {
			return count
		}
	}
	return 0
}
