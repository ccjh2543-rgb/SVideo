package social

import (
	"context"
	"encoding/json"
	"errors"
	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainnotification "feedsystem_video_go/internal/domain/notification"
	domainsocial "feedsystem_video_go/internal/domain/social"
	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	"feedsystem_video_go/internal/infrastructure/workers"
	"feedsystem_video_go/internal/util"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// FollowingCacheInvalidator 关注列表缓存失效接口。
// 关注关系变化后清理缓存，避免关注流继续读到旧数据。
type FollowingCacheInvalidator interface {
	Invalidate(ctx context.Context, followerID uint) error
}

// HeavyRegistry 重度用户索引维护接口。
type HeavyRegistry interface {
	OnFollowBigV(ctx context.Context, uid, bigV uint) (becameHeavy bool, isHeavy bool, err error)
	OnUnfollowBigV(ctx context.Context, uid, bigV uint) error
	ListHeavyFollowing(ctx context.Context, uid uint) ([]uint, error)
	TryBeginBackfill(ctx context.Context, uid uint) (bool, error)
	MarkBackfilled(ctx context.Context, uid uint) error
}

// newFollowBackfillLimit 新关注时回填到粉丝 Inbox 的作者历史视频条数。
// 取作者 Outbox 的近期内容即可，更早的历史仍由 DB 游标兜底。
const newFollowBackfillLimit = 20

// heavyBackfillLimit 新晋重度用户回填每个大 V 的条数。
const heavyBackfillLimit = 20

// SocialWorker 是处理社交关系（关注/取关）的消费者 Worker。
// 它从 RabbitMQ 指定队列中消费 SocialEvent 消息，并调用 SocialRepository 持久化关系。
type SocialWorker struct {
	ch             *amqp.Channel
	repo           domainsocial.Repository
	stats          domainsocial.StatsRepository
	timeline       domainfeed.TimelineRepository
	followingCache FollowingCacheInvalidator
	heavy          HeavyRegistry
	events         workers.EventIdempotency
	notifier       domainnotification.Notifier
	queue          string
}

// NewSocialWorker 创建 SocialWorker 实例。
func NewSocialWorker(
	ch *amqp.Channel,
	repo domainsocial.Repository,
	stats domainsocial.StatsRepository,
	timeline domainfeed.TimelineRepository,
	followingCache FollowingCacheInvalidator,
	heavy HeavyRegistry,
	events workers.EventIdempotency,
	notifier domainnotification.Notifier,
	queue string,
) *SocialWorker {
	return &SocialWorker{
		ch:             ch,
		repo:           repo,
		stats:          stats,
		timeline:       timeline,
		followingCache: followingCache,
		heavy:          heavy,
		events:         events,
		notifier:       notifier,
		queue:          queue,
	}
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
	var err error
	switch evt.Action {
	case "follow":
		err = w.applyFollow(ctx, &evt)
	case "unfollow":
		err = w.applyUnfollow(ctx, &evt)
	default:
		// 未知动作类型，丢弃消息不处理，不重试
		return nil
	}
	if err != nil && w.events != nil {
		// 业务失败时删除幂等标记，保证重放时可以重新处理。
		if unmarkErr := w.events.Unmark(ctx, "social", evt.EventID); unmarkErr != nil {
			log.Printf("social worker: unmark failed event_id=%s: %v", evt.EventID, unmarkErr)
		}
	}
	return err
}

// applyFollow 在事务内写入关注关系与粉丝数，并清理关注列表缓存。
func (w *SocialWorker) applyFollow(ctx context.Context, evt *rabbitmq.SocialEvent) error {
	relation := &domainsocial.Social{
		FollowerID: evt.FollowerID,
		VloggerID:  evt.VloggerID,
	}
	if err := w.repo.FollowWithStats(ctx, relation); err != nil {
		// 重复关注是幂等场景，不重复增加粉丝数；仍清理一次缓存，修复可能丢失的失效消息。
		if util.IsDuplicateEntry(err) || errors.Is(err, domainsocial.ErrAlreadyFollowed) {
			w.invalidateFollowing(ctx, evt.FollowerID)
			return nil
		}
		return err
	}
	w.invalidateFollowing(ctx, evt.FollowerID)
	// 新关注后把作者最近的视频回填到粉丝 Inbox，
	// 避免“关注成功但首屏为空”，要等下一次发布才可见。
	w.backfillInbox(ctx, evt.FollowerID, evt.VloggerID)
	w.updateHeavyOnFollow(ctx, evt.FollowerID, evt.VloggerID)

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

// applyUnfollow 在事务内删除关注关系与粉丝数，并清理粉丝 Inbox 中该博主的残留视频。
func (w *SocialWorker) applyUnfollow(ctx context.Context, evt *rabbitmq.SocialEvent) error {
	relation := &domainsocial.Social{
		FollowerID: evt.FollowerID,
		VloggerID:  evt.VloggerID,
	}
	if err := w.repo.UnfollowWithStats(ctx, relation); err != nil {
		// 关系不存在说明已经取关或事件乱序，直接视为成功，避免重复扣减粉丝数。
		if errors.Is(err, domainsocial.ErrNotFollowed) {
			w.invalidateFollowing(ctx, evt.FollowerID)
			return nil
		}
		return err
	}
	w.invalidateFollowing(ctx, evt.FollowerID)
	w.cleanInbox(ctx, evt.FollowerID, evt.VloggerID)
	w.removeHeavyOnUnfollow(ctx, evt.FollowerID, evt.VloggerID)
	return nil
}

// invalidateFollowing 清理关注列表缓存，失败由 TTL 兜底。
func (w *SocialWorker) invalidateFollowing(ctx context.Context, followerID uint) {
	if w.followingCache == nil {
		return
	}
	_ = w.followingCache.Invalidate(ctx, followerID) // 缓存失效失败不影响取关结果
}

// updateHeavyOnFollow 维护重度用户索引，并在跨入阈值时回填大 V 最近内容。
func (w *SocialWorker) updateHeavyOnFollow(ctx context.Context, followerID, vloggerID uint) {
	if w.heavy == nil || w.stats == nil {
		return
	}
	stats, err := w.stats.Get(ctx, vloggerID)
	if err != nil {
		log.Printf("social worker: heavy stats failed vlogger=%d: %v", vloggerID, err)
		return
	}
	if stats == nil || (!stats.IsBig && stats.FansCount < domainsocial.BigVFansThreshold) {
		return
	}

	becameHeavy, isHeavy, err := w.heavy.OnFollowBigV(ctx, followerID, vloggerID)
	if err != nil {
		log.Printf("social worker: heavy follow failed follower=%d vlogger=%d: %v", followerID, vloggerID, err)
		return
	}
	if !isHeavy {
		return
	}
	if becameHeavy {
		w.backfillAllHeavy(ctx, followerID)
		return
	}
	w.backfillHeavyAuthor(ctx, followerID, vloggerID)
}

// removeHeavyOnUnfollow 维护重度用户索引。
func (w *SocialWorker) removeHeavyOnUnfollow(ctx context.Context, followerID, vloggerID uint) {
	if w.heavy == nil {
		return
	}
	if err := w.heavy.OnUnfollowBigV(ctx, followerID, vloggerID); err != nil {
		log.Printf("social worker: heavy unfollow failed follower=%d vlogger=%d: %v", followerID, vloggerID, err)
	}
}

// backfillAllHeavy 新晋重度用户回填所有已关注大 V 的最近内容。
// 只有全部成功才标记 backfilled；失败时锁自动过期，后续读路径仍走安全拉取。
func (w *SocialWorker) backfillAllHeavy(ctx context.Context, followerID uint) {
	started, err := w.heavy.TryBeginBackfill(ctx, followerID)
	if err != nil || !started {
		return
	}
	members, err := w.heavy.ListHeavyFollowing(ctx, followerID)
	if err != nil {
		log.Printf("social worker: heavy following list failed follower=%d: %v", followerID, err)
		return
	}
	for _, vloggerID := range members {
		if !w.backfillHeavyAuthor(ctx, followerID, vloggerID) {
			return
		}
	}
	if err := w.heavy.MarkBackfilled(ctx, followerID); err != nil {
		log.Printf("social worker: mark heavy backfilled failed follower=%d: %v", followerID, err)
	}
}

// backfillHeavyAuthor 把某个大 V 最近内容回填到重度粉丝 Inbox。
// 返回 false 表示本次回填失败，调用方不能标记 backfilled。
func (w *SocialWorker) backfillHeavyAuthor(ctx context.Context, followerID, vloggerID uint) bool {
	if w.timeline == nil {
		return true
	}
	entries, err := w.timeline.ReadOutbox(ctx, vloggerID, 0, heavyBackfillLimit)
	if err != nil {
		log.Printf("social worker: heavy backfill read failed vlogger=%d: %v", vloggerID, err)
		return false
	}
	if len(entries) == 0 {
		return true
	}
	if err := w.timeline.PushInboxEntriesWindow(ctx, followerID, entries, domainfeed.DefaultInboxCapacity); err != nil {
		log.Printf("social worker: heavy backfill push failed follower=%d vlogger=%d: %v", followerID, vloggerID, err)
		return false
	}
	return true
}

// backfillInbox 把作者 Outbox 中最近的视频回填到新粉丝的 Inbox。
//
// 只对小 V 回填：大 V 本就走读时拉取 Outbox，粉丝第一次刷新就能看到，
// 回填反而会放大写扩散。回填失败不影响关注结果，历史内容仍可通过 DB 兜底。
func (w *SocialWorker) backfillInbox(ctx context.Context, followerID, vloggerID uint) {
	if w.timeline == nil || w.stats == nil || followerID == 0 || vloggerID == 0 {
		return
	}
	stats, err := w.stats.Get(ctx, vloggerID)
	if err != nil {
		log.Printf("social worker: get stats failed vlogger=%d: %v", vloggerID, err)
		return
	}
	if stats != nil && (stats.IsBig || stats.FansCount >= domainsocial.BigVFansThreshold) {
		return
	}
	entries, err := w.timeline.ReadOutbox(ctx, vloggerID, 0, newFollowBackfillLimit)
	if err != nil {
		log.Printf("social worker: backfill read outbox failed vlogger=%d: %v", vloggerID, err)
		return
	}
	if len(entries) == 0 {
		return
	}
	if err := w.timeline.PushInboxEntriesWindow(ctx, followerID, entries, domainfeed.DefaultInboxCapacity); err != nil {
		log.Printf("social worker: backfill inbox failed follower=%d vlogger=%d: %v", followerID, vloggerID, err)
	}
}

// cleanInbox 删除粉丝 Inbox 中该作者最近的视频。
// Outbox 只保留最近 50 条，更早的残留由读路径按关注集合过滤兜底。
func (w *SocialWorker) cleanInbox(ctx context.Context, followerID, vloggerID uint) {
	if w.timeline == nil {
		return
	}
	entries, err := w.timeline.ReadOutbox(ctx, vloggerID, 0, 50)
	if err != nil {
		log.Printf("social worker: read outbox failed vlogger=%d: %v", vloggerID, err)
		return
	}
	if len(entries) == 0 {
		return
	}
	videoIDs := make([]uint, 0, len(entries))
	for _, entry := range entries {
		videoIDs = append(videoIDs, entry.VideoID)
	}
	if err := w.timeline.RemoveFromInbox(ctx, followerID, videoIDs); err != nil {
		log.Printf("social worker: clean inbox failed follower=%d vlogger=%d: %v", followerID, vloggerID, err)
	}
}
