package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 重度粉丝推送使用独立交换机与队列：
// 与 video.timeline.* 隔离，避免任务消息被写扩散/最新流队列重复消费。
const (
	HeavyPushExchange   = "video.heavy_push.events"
	HeavyPushQueueName  = "video.heavy_push.queue"
	heavyPushBindingKey = "video.heavy_push.*"
	heavyPushRoutingKey = "video.heavy_push.task"
	heavyPushDLX        = "video.heavy_push.events.dlx"
	HeavyPushDLQName    = "video.heavy_push.dlq"
)

// HeavyPushEvent 大 V 发布后的重度粉丝推送任务。
// EventID 复用写扩散事件的 outbox 消息 ID，保证重复投递时消费端可以幂等拦截。
type HeavyPushEvent struct {
	EventID  string `json:"event_id"`
	VideoID  uint   `json:"video_id"`
	AuthorID uint   `json:"author_id"`
	// Score 视频发布时间毫秒时间戳，作为 Inbox ZSet 的 score。
	Score int64 `json:"score"`
}

// HeavyPushMQ 重度粉丝推送任务队列。
type HeavyPushMQ struct {
	*RabbitMQ
}

// NewHeavyPushMQ 声明重度推送队列与死信队列。
func NewHeavyPushMQ(base *RabbitMQ) (*HeavyPushMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopicWithDLQ(
		HeavyPushExchange,
		HeavyPushQueueName,
		heavyPushBindingKey,
		heavyPushDLX,
		HeavyPushDLQName,
		"#",
	); err != nil {
		return nil, err
	}
	return &HeavyPushMQ{RabbitMQ: base}, nil
}

// PublishHeavyPush 投递一条重度粉丝推送任务。
func (m *HeavyPushMQ) PublishHeavyPush(ctx context.Context, eventID string, videoID, authorID uint, score int64) error {
	if m == nil || m.RabbitMQ == nil {
		return errors.New("heavy push mq is not initialized")
	}
	if videoID == 0 || authorID == 0 {
		return errors.New("videoID and authorID are required")
	}
	return m.PublishJSON(ctx, HeavyPushExchange, heavyPushRoutingKey, HeavyPushEvent{
		EventID:  eventID,
		VideoID:  videoID,
		AuthorID: authorID,
		Score:    score,
	})
}

// PublishRaw 按原始消息体重新投递，用于死信队列重放。
func (m *HeavyPushMQ) PublishRaw(ctx context.Context, body []byte, headers amqp.Table) error {
	if m == nil || m.RabbitMQ == nil || m.Ch == nil {
		return errors.New("heavy push mq is not initialized")
	}
	return m.Ch.PublishWithContext(ctx, HeavyPushExchange, heavyPushRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Headers:      headers,
		Body:         body,
	})
}
