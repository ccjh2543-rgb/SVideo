package rabbitmq

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/util"
	"time"
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

func (t *TimelineMQ) PublishVideo(ctx context.Context, videoID uint, authorID uint, createTime time.Time) error {
	if t == nil || t.RabbitMQ == nil {
		return errors.New("timeline mq is not initialized")
	}
	if videoID == 0 || authorID == 0 {
		return errors.New("videoID and authorID are required")
	}
	id, err := util.RandHex(16)
	if err != nil {
		return err
	}
	timeline := TimelineEvent{
		EventID:    id,
		VideoID:    videoID,
		AuthorID:   authorID,
		CreateTime: createTime.UnixMilli(),
		OccurredAt: time.Now(),
	}
	return t.PublishJSON(ctx, timelineExchange, timelinePublishRK, timeline)
}
