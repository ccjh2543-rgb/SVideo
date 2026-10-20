package ws

import (
	"context"
	"encoding/json"
	"log"

	domainnotification "feedsystem_video_go/internal/domain/notification"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

// NotificationChannel 所有 API 实例共享的 Redis Pub/Sub 频道。
const NotificationChannel = "ws:notifications"

// RedisNotifier 通过 Redis Pub/Sub 发布和消费通知。
// Worker 进程只调用 Publish，API 进程调用 Run 推送给本机连接。
type RedisNotifier struct {
	client *rediscache.Client
	hub    *Hub
	sub    *rediscache.Subscription
}

// NewRedisNotifier 创建通知发布器。
func NewRedisNotifier(client *rediscache.Client, hub *Hub) *RedisNotifier {
	return &RedisNotifier{client: client, hub: hub}
}

// Publish 发布一条通知。
func (n *RedisNotifier) Publish(ctx context.Context, event domainnotification.Event) error {
	if n == nil || n.client == nil {
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return n.client.Publish(ctx, NotificationChannel, payload)
}

// Run 订阅通知并推送给本机 WebSocket 连接。
func (n *RedisNotifier) Run(ctx context.Context) error {
	if n == nil || n.client == nil || n.hub == nil {
		return nil
	}
	sub, err := n.client.Subscribe(ctx, NotificationChannel)
	if err != nil {
		return err
	}
	n.sub = sub
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-sub.Messages():
			if !ok {
				return nil
			}
			var event domainnotification.Event
			if err := json.Unmarshal(message.Payload, &event); err != nil {
				log.Printf("ws notifier: invalid notification payload: %v", err)
				continue
			}
			if event.AccountID == 0 {
				continue
			}
			n.hub.SendToAccount(event.AccountID, message.Payload)
		}
	}
}

// Close 关闭订阅。
func (n *RedisNotifier) Close() error {
	if n == nil || n.sub == nil {
		return nil
	}
	return n.sub.Close()
}
