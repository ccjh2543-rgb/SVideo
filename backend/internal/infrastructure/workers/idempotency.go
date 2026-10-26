// Package workers 提供消费者 Worker 的共享能力。
package workers

import "context"

// EventIdempotency 消费端幂等接口。
// 返回 true 表示首次处理，false 表示重复消息。
type EventIdempotency interface {
	TryMark(ctx context.Context, consumer string, eventID string) (bool, error)
	// Unmark 删除幂等标记。
	// 业务处理失败时调用，保证消息重放（DLQ、补偿任务）能够重新进入业务。
	Unmark(ctx context.Context, consumer string, eventID string) error
}
