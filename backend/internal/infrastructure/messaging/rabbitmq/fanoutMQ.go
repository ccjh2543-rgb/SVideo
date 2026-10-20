package rabbitmq

import "errors"

// FanoutQueueName 视频写扩散队列。
// 与 Timeline 队列绑定到同一个 timeline exchange，RabbitMQ 会为两个队列各投递一份消息。
const (
	FanoutQueueName = "video.fanout.queue"
	fanoutDLX       = "video.fanout.dlx"
	FanoutDLQName   = "video.fanout.dlq"
)

// FanoutMQ 视频写扩散消息队列。
type FanoutMQ struct {
	*RabbitMQ
}

// NewFanoutMQ 声明写扩散队列。
func NewFanoutMQ(base *RabbitMQ) (*FanoutMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopicWithDLQ(
		timelineExchange,
		FanoutQueueName,
		timelineBindingKey,
		fanoutDLX,
		FanoutDLQName,
		"#",
	); err != nil {
		return nil, err
	}
	return &FanoutMQ{RabbitMQ: base}, nil
}
