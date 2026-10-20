package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/config"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

func NewRabbitMQ(cfg *config.RabbitMQConfig) (*RabbitMQ, error) {
	if cfg == nil {
		return nil, errors.New("rabbitmq config is nil")
	}
	url := "amqp://" + cfg.Username + ":" + cfg.Password + "@" + cfg.Host + ":" + strconv.Itoa(cfg.Port) + "/"
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &RabbitMQ{Conn: conn, Ch: ch}, nil
}

func (r *RabbitMQ) Close() error {
	if r == nil || r.Ch == nil || r.Conn == nil {
		return nil
	}
	if err := r.Ch.Close(); err != nil {
		return err
	}
	if err := r.Conn.Close(); err != nil {
		return err
	}
	return nil
}

func (r *RabbitMQ) DeclareTopic(exchange string, queue string, bindingKey string) error {
	if r == nil || r.Ch == nil {
		return errors.New("rabbitmq is not initialized")
	}
	if exchange == "" || queue == "" || bindingKey == "" {
		return errors.New("exchange/queue/bindingKey is required")
	}

	if err := r.Ch.ExchangeDeclare(
		exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	q, err := r.Ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return r.Ch.QueueBind(
		q.Name,
		bindingKey,
		exchange,
		false,
		nil,
	)
}

// DeclareTopicWithDLQ 声明带死信队列的 topic 拓扑。
// 主队列消费失败并 Nack(requeue=false) 时，消息会进入 dlq。
func (r *RabbitMQ) DeclareTopicWithDLQ(
	exchange string,
	queue string,
	bindingKey string,
	dlxExchange string,
	dlq string,
	dlqBindingKey string,
) error {
	if r == nil || r.Ch == nil {
		return errors.New("rabbitmq is not initialized")
	}
	if exchange == "" || queue == "" || bindingKey == "" || dlxExchange == "" || dlq == "" {
		return errors.New("exchange/queue/bindingKey/dlx/dlq is required")
	}
	if dlqBindingKey == "" {
		dlqBindingKey = "#"
	}

	for _, name := range []string{exchange, dlxExchange} {
		if err := r.Ch.ExchangeDeclare(name, "topic", true, false, false, false, nil); err != nil {
			return err
		}
	}

	mainQueue, err := r.Ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    dlxExchange,
			"x-dead-letter-routing-key": dlqBindingKey,
		},
	)
	if err != nil {
		return err
	}
	if err := r.Ch.QueueBind(mainQueue.Name, bindingKey, exchange, false, nil); err != nil {
		return err
	}

	deadQueue, err := r.Ch.QueueDeclare(dlq, true, false, false, false, nil)
	if err != nil {
		return err
	}
	return r.Ch.QueueBind(deadQueue.Name, dlqBindingKey, dlxExchange, false, nil)
}

func (r *RabbitMQ) PublishJSON(ctx context.Context, exchange string, routingKey string, payload any) error {
	if r == nil || r.Ch == nil {
		return errors.New("rabbitmq is not initialized")
	}
	if exchange == "" || routingKey == "" {
		return errors.New("exchange and routingKey are required")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return r.Ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         b,
	})
}
