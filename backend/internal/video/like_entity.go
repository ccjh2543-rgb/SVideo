package video

import "time"

type Like struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	VideoID   uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"video_id"`
	AccountID uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"account_id"`
	CreatedAt time.Time `json:"created_at"`
}

// type Like struct {
// 	ID        uint      `gorm:"primaryKey" json:"id"`
// 	// 调整顺序：account_id 在前，video_id 在后
// 	AccountID uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"account_id"`
// 	VideoID   uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"video_id"`
// 	CreatedAt time.Time `json:"created_at"`
// }

type LikeRequest struct {
	VideoID uint `json:"video_id"`
}
