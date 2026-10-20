package rabbitmq

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/util"
	"time"
)

type SocialMQ struct {
	*RabbitMQ
}

const (
	socialExchange   = "social.events"
	SocialQueueName  = "social.events.v2"
	socialBindingKey = "social.*"
	socialDLX        = "social.events.dlx"
	SocialDLQName    = "social.events.dlq"

	socialFollowRK   = "social.follow"
	socialUnfollowRK = "social.unfollow"
)

type SocialEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	FollowerID uint      `json:"follower_id"`
	VloggerID  uint      `json:"vlogger_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewSocialMQ(base *RabbitMQ) (*SocialMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopicWithDLQ(socialExchange, SocialQueueName, socialBindingKey, socialDLX, SocialDLQName, "#"); err != nil {
		return nil, err
	}
	return &SocialMQ{RabbitMQ: base}, nil
}

func (s *SocialMQ) Follow(ctx context.Context, followerID, vloggerID uint) error {
	return s.publish(ctx, "follow", socialFollowRK, followerID, vloggerID)
}

func (s *SocialMQ) UnFollow(ctx context.Context, followerID, vloggerID uint) error {
	return s.publish(ctx, "unfollow", socialUnfollowRK, followerID, vloggerID)
}

func (s *SocialMQ) publish(ctx context.Context, action, routingKey string, followerID, vloggerID uint) error {
	if s == nil || s.RabbitMQ == nil {
		return errors.New("social mq is not initialized")
	}
	if followerID == 0 || vloggerID == 0 {
		return errors.New("followerID and vloggerID are required")
	}
	id, err := util.RandHex(16)
	if err != nil {
		return err
	}
	evt := SocialEvent{
		EventID:    id,
		Action:     action,
		FollowerID: followerID,
		VloggerID:  vloggerID,
		OccurredAt: time.Now().UTC(),
	}
	return s.PublishJSON(ctx, socialExchange, routingKey, evt)
}
