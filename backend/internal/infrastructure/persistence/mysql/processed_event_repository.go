package mysql

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ProcessedEventRepository 消费端幂等记录仓储。
type ProcessedEventRepository struct {
	db *gorm.DB
}

// NewProcessedEventRepository 创建幂等记录仓储。
func NewProcessedEventRepository(db *gorm.DB) *ProcessedEventRepository {
	return &ProcessedEventRepository{db: db}
}

// TryMark 尝试标记事件已处理。
// 返回 true 表示首次处理，false 表示重复事件。
func (r *ProcessedEventRepository) TryMark(ctx context.Context, consumer string, eventID string) (bool, error) {
	if r == nil || r.db == nil {
		return true, nil
	}
	if consumer == "" || eventID == "" {
		// 没有 event_id 的旧消息无法判断重复，按首次处理放行。
		return true, nil
	}
	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&ProcessedEventModel{Consumer: consumer, EventID: eventID})
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrDuplicatedKey) {
			return false, nil
		}
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}
