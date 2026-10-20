package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// Message Redis Pub/Sub 消息。
type Message struct {
	Channel string
	Payload []byte
}

// Subscription Redis 订阅封装。
type Subscription struct {
	pubsub *goredis.PubSub
	ch     chan Message
}

// Publish 向指定频道发布消息。
func (c *Client) Publish(ctx context.Context, channel string, payload []byte) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Publish(ctx, channel, payload).Err()
}

// Subscribe 订阅指定频道，等待订阅确认后返回。
func (c *Client) Subscribe(ctx context.Context, channel string) (*Subscription, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	pubsub := c.rdb.Subscribe(ctx, channel)

	sub := &Subscription{
		pubsub: pubsub,
		ch:     make(chan Message, 128),
	}
	go func() {
		defer close(sub.ch)
		for msg := range pubsub.Channel() {
			sub.ch <- Message{Channel: msg.Channel, Payload: []byte(msg.Payload)}
		}
	}()
	return sub, nil
}

// Messages 返回消息通道。
func (s *Subscription) Messages() <-chan Message {
	if s == nil {
		return nil
	}
	return s.ch
}

// Close 关闭订阅。
func (s *Subscription) Close() error {
	if s == nil || s.pubsub == nil {
		return nil
	}
	return s.pubsub.Close()
}
