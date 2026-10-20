// Package workers 提供消费者 Worker 的共享能力。
package workers

import "context"

// EventIdempotency 消费端幂等接口。
// 返回 true 表示首次处理，false 表示重复消息。
type EventIdempotency interface {
	TryMark(ctx context.Context, consumer string, eventID string) (bool, error)
}
