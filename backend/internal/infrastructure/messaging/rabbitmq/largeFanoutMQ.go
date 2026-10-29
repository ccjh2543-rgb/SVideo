package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 大粉丝量作者（粉丝多但未达大 V 阈值）的写扩散使用独立交换机与队列：
// 与普通 fanout 隔离，避免一次大扩散占满普通写扩散的消费能力。
const (
	LargeFanoutExchange   = "video.fanout_large.events"
	LargeFanoutQueueName  = "video.fanout.large.queue"
	largeFanoutBindingKey = "video.fanout_large.*"
	largeFanoutRoutingKey = "video.fanout_large.task"
	largeFanoutDLX        = "video.fanout_large.events.dlx"
	LargeFanoutDLQName    = "video.fanout_large.dlq"
)

// LargeFanoutEvent 大粉丝量作者的写扩散任务。
type LargeFanoutEvent struct {
	EventID  string `json:"event_id"`
	VideoID  uint   `json:"video_id"`
	AuthorID uint   `json:"author_id"`
	// Score 视频发布时间毫秒时间戳，作为 Inbox ZSet 的 score。
	Score int64 `json:"score"`
}

// LargeFanoutMQ 大粉丝量写扩散任务队列。
type LargeFanoutMQ struct {
	*RabbitMQ
}

// NewLargeFanoutMQ 声明大粉丝量写扩散队列与死信队列。
func NewLargeFanoutMQ(base *RabbitMQ) (*LargeFanoutMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopicWithDLQ(
		LargeFanoutExchange,
		LargeFanoutQueueName,
		largeFanoutBindingKey,
		largeFanoutDLX,
		LargeFanoutDLQName,
		"#",
	); err != nil {
		return nil, err
	}
	return &LargeFanoutMQ{RabbitMQ: base}, nil
}

// PublishLargeFanout 投递一条大粉丝量写扩散任务。
func (m *LargeFanoutMQ) PublishLargeFanout(ctx context.Context, eventID string, videoID, authorID uint, score int64) error {
	if m == nil || m.RabbitMQ == nil {
		return errors.New("large fanout mq is not initialized")
	}
	if videoID == 0 || authorID == 0 {
		return errors.New("videoID and authorID are required")
	}
	return m.PublishJSON(ctx, LargeFanoutExchange, largeFanoutRoutingKey, LargeFanoutEvent{
		EventID:  eventID,
		VideoID:  videoID,
		AuthorID: authorID,
		Score:    score,
	})
}

// PublishRaw 按原始消息体重新投递，用于死信队列重放。
func (m *LargeFanoutMQ) PublishRaw(ctx context.Context, body []byte, headers amqp.Table) error {
	if m == nil || m.RabbitMQ == nil || m.Ch == nil {
		return errors.New("large fanout mq is not initialized")
	}
	return m.Ch.PublishWithContext(ctx, LargeFanoutExchange, largeFanoutRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Headers:      headers,
		Body:         body,
	})
}
