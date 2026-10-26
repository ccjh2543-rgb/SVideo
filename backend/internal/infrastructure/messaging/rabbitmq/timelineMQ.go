package rabbitmq

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/util"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TimelineMQ struct {
	*RabbitMQ
}

const (
	timelineExchange   = "video.timeline.events"
	TimelineQueueName  = "video.timeline.update.queue.v2"
	timelineBindingKey = "video.timeline.*"
	timelinePublishRK  = "video.timeline.publish"
	timelineDLX        = "video.timeline.events.dlx"
	TimelineDLQName    = "video.timeline.events.dlq"
)

type TimelineEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	AuthorID   uint      `json:"author_id"`
	CreateTime int64     `json:"create_time"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewTimelineMQ(base *RabbitMQ) (*TimelineMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopicWithDLQ(
		timelineExchange,
		TimelineQueueName,
		timelineBindingKey,
		timelineDLX,
		TimelineDLQName,
		"#",
	); err != nil {
		return nil, err
	}
	return &TimelineMQ{RabbitMQ: base}, nil
}

// PublishVideo 发布视频事件。
// eventID 使用 outbox 消息 ID，保证同一事务消息重复投递时消费端可以幂等拦截；
// 传空时退化为随机 ID，保持对旧调用方的兼容。
func (t *TimelineMQ) PublishVideo(ctx context.Context, eventID string, videoID uint, authorID uint, createTime time.Time) error {
	if t == nil || t.RabbitMQ == nil {
		return errors.New("timeline mq is not initialized")
	}
	if videoID == 0 || authorID == 0 {
		return errors.New("videoID and authorID are required")
	}
	if eventID == "" {
		id, err := util.RandHex(16)
		if err != nil {
			return err
		}
		eventID = id
	}
	timeline := TimelineEvent{
		EventID:    eventID,
		VideoID:    videoID,
		AuthorID:   authorID,
		CreateTime: createTime.UnixMilli(),
		OccurredAt: time.Now(),
	}
	return t.PublishJSON(ctx, timelineExchange, timelinePublishRK, timeline)
}

// PublishRaw 按原始消息体重新投递到时间线交换机，用于死信队列重放。
func (t *TimelineMQ) PublishRaw(ctx context.Context, body []byte, headers amqp.Table) error {
	if t == nil || t.RabbitMQ == nil || t.Ch == nil {
		return errors.New("timeline mq is not initialized")
	}
	return t.Ch.PublishWithContext(ctx, timelineExchange, timelinePublishRK, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Headers:      headers,
		Body:         body,
	})
}
