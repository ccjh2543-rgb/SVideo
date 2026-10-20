package worker

import "context"

// EventIdempotency 消费端幂等接口。
// 返回 true 表示首次处理，false 表示重复消息。
type EventIdempotency interface {
	TryMark(ctx context.Context, consumer string, eventID string) (bool, error)
}
