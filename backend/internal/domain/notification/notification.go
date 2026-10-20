// Package notification 定义站内通知领域接口。
package notification

import (
	"context"
	"time"
)

// Event 一条站内通知。
type Event struct {
	EventID   string         `json:"event_id"`
	AccountID uint           `json:"account_id"`
	Type      string         `json:"type"`
	Title     string         `json:"title"`
	Body      string         `json:"body"`
	Data      map[string]any `json:"data,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

// Notifier 通知发布接口。
// API 进程使用 WebSocket Hub 消费，Worker 进程通过 Redis Pub/Sub 发布。
type Notifier interface {
	Publish(ctx context.Context, event Event) error
}
