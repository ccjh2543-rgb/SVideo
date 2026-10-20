package mysql

import "time"

// ProcessedEventModel 消费端幂等记录。
// (consumer, event_id) 唯一，保证同一消费者不会重复处理同一条事件。
type ProcessedEventModel struct {
	ID        uint      `gorm:"primaryKey"`
	Consumer  string    `gorm:"size:64;not null;uniqueIndex:uk_consumer_event"`
	EventID   string    `gorm:"size:64;not null;uniqueIndex:uk_consumer_event"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName 指定幂等记录表名。
func (ProcessedEventModel) TableName() string {
	return "processed_events"
}
