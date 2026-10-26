package video

import (
	"context"
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"

	"gorm.io/gorm"
)

// OutboxRepository 事务消息仓储的 MySQL 实现。
type OutboxRepository struct {
	db *gorm.DB
}

// NewOutboxRepository 创建事务消息仓储。
func NewOutboxRepository(db *gorm.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// ListPending 查询待投递消息。
func (r *OutboxRepository) ListPending(ctx context.Context, now time.Time, limit int) ([]domainvideo.OutboxMessage, error) {
	var models []OutboxModel
	if err := r.db.WithContext(ctx).
		Where("status = ?", "pending").
		Where("next_retry IS NULL OR next_retry <= ?", now).
		Order("create_time ASC").
		Limit(limit).
		Find(&models).Error; err != nil {
		return nil, err
	}
	messages := make([]domainvideo.OutboxMessage, 0, len(models))
	for _, model := range models {
		messages = append(messages, *toOutboxDomain(&model))
	}
	return messages, nil
}

// Delete 删除已投递消息。
func (r *OutboxRepository) Delete(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).Delete(&OutboxModel{}, id).Error
}

// MarkRetry 更新重试次数、状态和下次重试时间。
func (r *OutboxRepository) MarkRetry(
	ctx context.Context,
	id uint,
	retryCount int,
	status string,
	nextRetry *time.Time,
) error {
	updates := map[string]interface{}{
		"retry_count": retryCount,
		"status":      status,
		"next_retry":  nextRetry,
	}
	return r.db.WithContext(ctx).Model(&OutboxModel{}).Where("id = ?", id).Updates(updates).Error
}

// RequeueFailed 把失败消息重新置为待投递。
// 只处理创建时间早于 olderThan 的记录，避免刚失败的消息立即被反复重投。
func (r *OutboxRepository) RequeueFailed(ctx context.Context, olderThan time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	result := r.db.WithContext(ctx).
		Model(&OutboxModel{}).
		Where("status = ? AND create_time < ?", "failed", olderThan).
		Limit(limit).
		Updates(map[string]interface{}{
			"status":      "pending",
			"retry_count": 0,
			"next_retry":  nil,
		})
	return result.RowsAffected, result.Error
}
