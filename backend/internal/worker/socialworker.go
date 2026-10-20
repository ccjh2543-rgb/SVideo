package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/domain/creator"
	domainnotification "feedsystem_video_go/internal/domain/notification"
	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/util"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SocialWorker 是处理社交关系（关注/取关）的消费者 Worker。
// 它从 RabbitMQ 指定队列中消费 SocialEvent 消息，并调用 SocialRepository 持久化关系。
type SocialWorker struct {
	ch       *amqp.Channel
	repo     *social.SocialRepository
	creators creator.Repository
	events   EventIdempotency
	notifier domainnotification.Notifier
	queue    string
}

// NewSocialWorker 创建 SocialWorker 实例。
func NewSocialWorker(
	ch *amqp.Channel,
	repo *social.SocialRepository,
	creators creator.Repository,
	events EventIdempotency,
	notifier domainnotification.Notifier,
	queue string,
) *SocialWorker {
	return &SocialWorker{ch: ch, repo: repo, creators: creators, events: events, notifier: notifier, queue: queue}
}

// Run 启动 Worker，开始消费消息并处理，直到 ctx 被取消或发生不可恢复的错误。
// 返回错误可能是 Channel 被关闭、上下文取消或其他启动失败的情况。
func (w *SocialWorker) Run(ctx context.Context) error {
	// 防御性检查：避免因未初始化而空指针
	if w == nil || w.ch == nil || w.repo == nil {
		return errors.New("social worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}

	// 开始消费指定队列的消息。
	// 参数依次为：队列名、消费者标签（空则自动生成）、自动确认(false 即手动确认)、
	// 排他(false)、仅本地(false)、等待(false)、额外参数(nil)。
	deliveries, err := w.ch.Consume(
		w.queue,
		"",    // consumer tag，空字符串让服务端自动生成
		false, // auto-ack 设为 false，采用手动确认，确保消息处理成功后才从队列移除
		false, // exclusive 排他性，false 允许多个消费者共享队列
		false, // no-local 未使用，false
		false, // no-wait 是否非阻塞等待，false 表示等待服务器声明队列后才开始消费
		nil,   // args 额外参数，无特殊需要
	)
	if err != nil {
		return err
	}

	// 无限循环，同时监听上下文结束和消息到达
	for {
		select {
		case <-ctx.Done():
			// 上下文被取消（例如收到 SIGTERM 或程序退出），优雅停止
			return ctx.Err()
		case d, ok := <-deliveries:
			// deliveries 信道被关闭（例如连接断开），返回错误
			if !ok {
				return errors.New("deliveries channel closed")
			}
			// 处理接收到的消息
			w.handleDelivery(ctx, d)
		}
	}
}

// handleDelivery 处理单条 AMQP 消息，协调解析与确认/拒绝流程。
func (w *SocialWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	// 调用核心业务逻辑处理消息体
	if err := w.process(ctx, d.Body); err != nil {
		// 处理失败，记录日志并投递到死信队列，避免无限重试。
		log.Printf("social worker: failed to process message: %v", err)
		_ = d.Nack(false, false)
		return
	}
	// 处理成功，发送 Ack 确认消息，从队列中移除
	_ = d.Ack(false) // false 表示不批量确认，仅当前消息
}

// process 解析消息为 SocialEvent，并根据动作执行关注或取关。
// 返回 error 表示该消息需要重试；返回 nil 表示处理成功或消息可安全丢弃（无需重试）。
func (w *SocialWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.SocialEvent
	// 反序列化 JSON 为事件结构体，解析失败直接丢弃（因为重试也无法解析）
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil // 丢弃无效消息，不重试
	}
	// 基础校验：FollowerID 或 VloggerID 为零的无效消息直接丢弃
	if evt.FollowerID == 0 || evt.VloggerID == 0 {
		return nil
	}
	if w.events != nil {
		first, err := w.events.TryMark(ctx, "social", evt.EventID)
		if err != nil {
			return err
		}
		if !first {
			return nil
		}
	}

	// 根据事件中的 Action 字段分发业务逻辑
	switch evt.Action {
	case "follow":
		// 尝试插入关注关系
		err := w.repo.Follow(ctx, &social.Social{
			FollowerID: evt.FollowerID,
			VloggerID:  evt.VloggerID,
		})
		if err == nil {
			// 只有真正新增关注关系时才增加粉丝数，重复消息不会重复计数。
			if w.creators != nil {
				if err := w.creators.IncrementFans(ctx, evt.VloggerID, 1); err != nil {
					return err
				}
			}
			if w.notifier != nil {
				_ = w.notifier.Publish(ctx, domainnotification.Event{
					EventID:   evt.EventID,
					AccountID: evt.VloggerID,
					Type:      "follow",
					Title:     "有新的粉丝",
					Body:      "有人关注了你",
					Data:      map[string]any{"follower_id": evt.FollowerID},
				})
			}
			return nil
		}
		// 处理 MySQL 重复键错误（Error 1062: Duplicate entry）
		// 关注关系存在是幂等场景，不应视为错误，直接忽略
		if util.IsDuplicateEntry(err) {
			return nil // 重复关注，视为成功，不重试
		}
		// 其他数据库错误（连接问题、死锁等），返回错误触发 Nack 重新入队重试
		return err

	case "unfollow":
		// 尝试删除关注关系，删除成功后减少粉丝数。
		if err := w.repo.Unfollow(ctx, &social.Social{
			FollowerID: evt.FollowerID,
			VloggerID:  evt.VloggerID,
		}); err != nil {
			return err
		}
		if w.creators != nil {
			return w.creators.IncrementFans(ctx, evt.VloggerID, -1)
		}
		return nil

	default:
		// 未知动作类型，丢弃消息不处理，不重试
		return nil
	}
}
