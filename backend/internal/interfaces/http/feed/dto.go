package feed

import "time"

// FeedAuthor 视频作者信息，只暴露前端需要的字段。
type FeedAuthor struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

// FeedVideoItem 视频流卡片。
type FeedVideoItem struct {
	ID          uint       `json:"id"`
	Author      FeedAuthor `json:"author"`
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	PlayURL     string     `json:"play_url"`
	CoverURL    string     `json:"cover_url"`
	CreateTime  int64      `json:"create_time"`
	LikesCount  int64      `json:"likes_count"`
	IsLiked     bool       `json:"is_liked"`
}

// ListLatestRequest 最新视频流请求。
type ListLatestRequest struct {
	Limit      int   `json:"limit"`
	LatestTime int64 `json:"latest_time"`
	LatestID   *uint `json:"latest_id,omitempty"`
}

// ListLatestResponse 最新视频流响应。
type ListLatestResponse struct {
	VideoList []FeedVideoItem `json:"video_list"`
	NextTime  int64           `json:"next_time"`
	NextID    *uint           `json:"next_id,omitempty"`
	HasMore   bool            `json:"has_more"`
}

// ListLikesCountRequest 点赞数排行请求。
type ListLikesCountRequest struct {
	Limit            int    `json:"limit"`
	LikesCountBefore *int64 `json:"likes_count_before,omitempty"`
	IDBefore         *uint  `json:"id_before,omitempty"`
}

// ListLikesCountResponse 点赞数排行响应。
type ListLikesCountResponse struct {
	VideoList            []FeedVideoItem `json:"video_list"`
	NextLikesCountBefore *int64          `json:"next_likes_count_before,omitempty"`
	NextIDBefore         *uint           `json:"next_id_before,omitempty"`
	HasMore              bool            `json:"has_more"`
}

// ListByFollowingRequest 关注流请求。
type ListByFollowingRequest struct {
	Limit      int   `json:"limit"`
	LatestTime int64 `json:"latest_time"`
}

// ListByFollowingResponse 关注流响应。
type ListByFollowingResponse struct {
	VideoList []FeedVideoItem `json:"video_list"`
	NextTime  int64           `json:"next_time"`
	HasMore   bool            `json:"has_more"`
}

// ListByPopularityRequest 热度推荐流请求。
type ListByPopularityRequest struct {
	Limit          int   `json:"limit"`
	AsOf           int64 `json:"as_of"`
	Offset         int   `json:"offset"`
	LatestIDBefore *uint `json:"latest_id_before,omitempty"`

	// 以下字段用于 Redis 不可用时的数据库降级分页。
	LatestPopularity int64     `json:"latest_popularity"`
	LatestBefore     time.Time `json:"latest_before"`
}

// ListByPopularityResponse 热度推荐流响应。
type ListByPopularityResponse struct {
	VideoList  []FeedVideoItem `json:"video_list"`
	AsOf       int64           `json:"as_of"`
	NextOffset int             `json:"next_offset"`
	HasMore    bool            `json:"has_more"`

	NextLatestPopularity *int64     `json:"next_latest_popularity,omitempty"`
	NextLatestBefore     *time.Time `json:"next_latest_before,omitempty"`
	NextLatestIDBefore   *uint      `json:"next_latest_id_before,omitempty"`
}
