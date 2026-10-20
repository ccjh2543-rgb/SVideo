package social

// FollowRequest 关注博主请求。
type FollowRequest struct {
	VloggerID uint `json:"vlogger_id"`
}

// UnfollowRequest 取消关注请求。
type UnfollowRequest struct {
	VloggerID uint `json:"vlogger_id"`
}

// GetAllFollowersRequest 查询粉丝列表请求。
type GetAllFollowersRequest struct {
	VloggerID uint `json:"vlogger_id"`
}

// GetAllVloggersRequest 查询关注列表请求。
type GetAllVloggersRequest struct {
	FollowerID uint `json:"follower_id"`
}

// AccountInfo 社交列表返回的账号信息。
type AccountInfo struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

// GetAllFollowersResponse 粉丝列表响应。
type GetAllFollowersResponse struct {
	Followers []AccountInfo `json:"followers"`
}

// GetAllVloggersResponse 关注列表响应。
type GetAllVloggersResponse struct {
	Vloggers []AccountInfo `json:"vloggers"`
}
