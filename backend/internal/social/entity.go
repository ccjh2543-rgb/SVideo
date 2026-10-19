package social

import "feedsystem_video_go/internal/account"

// Social 博主-粉丝关注关系表（数据库实体）
// 设计说明：这是一个典型的"多对多关系"中间表，实现类似抖音的"关注-粉丝"功能
type Social struct {
	// 主键ID
	ID uint `gorm:"primaryKey"`

	// 关注者ID（粉丝ID）
	// - not null: 不允许为空，每一条关系必须有一个关注者
	// - index: 加速查询"我关注了哪些人"
	// - uniqueIndex: 和VloggerID组成联合唯一索引，防止重复关注
	FollowerID uint `gorm:"not null;index:idx_social_follower;uniqueIndex:idx_social_follower_vlogger"`

	// 被关注者ID（博主ID）
	// - not null: 不允许为空，每一条关系必须有一个被关注者
	// - index: 加速查询"我的粉丝列表"
	// - uniqueIndex: 和FollowerID组成联合唯一索引，保证同一个人不能关注同一个博主两次
	VloggerID uint `gorm:"not null;index:idx_social_vlogger;uniqueIndex:idx_social_follower_vlogger"`
}

// 关注博主时的请求参数
type FollowRequest struct {
	VloggerID uint `json:"vlogger_id"`
}

// 取消关注博主时的请求参数
type UnfollowRequest struct {
	VloggerID uint `json:"vlogger_id"`
}

// 查询博主所有粉丝时的请求参数
type GetAllFollowersRequest struct {
	VloggerID uint `json:"vlogger_id"`
}

// 查询博主所有粉丝时的响应参数
type GetAllFollowersResponse struct {
	Followers []*account.Account `json:"followers"`
}

// 查询关注列表时的请求参数
type GetAllVloggersRequest struct {
	FollowerID uint `json:"follower_id"`
}

// 查询关注列表时的响应参数
type GetAllVloggersResponse struct {
	Vloggers []*account.Account `json:"vloggers"`
}
