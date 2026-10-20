package feed

import "time"

// FeedAuthor 视频作者信息（专门给Feed流用的精简版，不暴露敏感字段）
// 设计说明：只返回前端需要的字段，保护用户隐私
type FeedAuthor struct {
	ID       uint   `json:"id"`       // 作者用户ID
	Username string `json:"username"` // 作者昵称
}

// FeedVideoItem 视频流卡片Item（抖音首页刷到的每个视频卡片）
// 设计说明：这是VO视图对象，专门给前端用的，和数据库Video实体字段不一样
type FeedVideoItem struct {
	ID          uint       `json:"id"`                    // 视频ID
	Author      FeedAuthor `json:"author"`                // 作者信息
	Title       string     `json:"title"`                 // 视频标题
	Description string     `json:"description,omitempty"` // 视频简介（空就不返回这个字段）
	PlayURL     string     `json:"play_url"`              // 视频播放地址
	CoverURL    string     `json:"cover_url"`             // 封面图地址
	CreateTime  int64      `json:"create_time"`           // 发布时间戳（毫秒）
	LikesCount  int64      `json:"likes_count"`           // 点赞数
	IsLiked     bool       `json:"is_liked"`              // 当前登录用户有没有点赞这个视频
}

// -----------------------------------------------------------------------------
// 【最新视频流】 按发布时间倒序 - 对应抖音"关注"tab旁边的"最新"tab
// -----------------------------------------------------------------------------

// ListLatestRequest 获取最新视频流请求参数
// 设计说明：用时间戳游标分页，不用offset，翻多少页都不慢
type ListLatestRequest struct {
	Limit      int   `json:"limit"`               // 每页返回多少条，一般10条
	LatestTime int64 `json:"latest_time"`         // 上一页最后一条的发布时间（游标，第一页传0）
	LatestID   *uint `json:"latest_id,omitempty"` // 同毫秒发布的 ID 兜底游标
}

// ListLatestResponse 获取最新视频流返回结果
type ListLatestResponse struct {
	VideoList []FeedVideoItem `json:"video_list"`        // 视频列表
	NextTime  int64           `json:"next_time"`         // 下一页请求传这个值回去当游标
	NextID    *uint           `json:"next_id,omitempty"` // 同毫秒兜底游标
	HasMore   bool            `json:"has_more"`          // 还有没有下一页
}

// -----------------------------------------------------------------------------
// 【按点赞数排序】 经典热门榜单
// -----------------------------------------------------------------------------

// ListLikesCountRequest 按点赞数排序的视频流请求
// 设计说明：双字段游标，点赞数相同就用ID排序，保证分页不重复不漏
type ListLikesCountRequest struct {
	Limit            int    `json:"limit"`                        // 每页条数
	LikesCountBefore *int64 `json:"likes_count_before,omitempty"` // 上一页最后一条的点赞数（游标）
	IDBefore         *uint  `json:"id_before,omitempty"`          // 上一页最后一条的视频ID（游标，解决同分问题）
}

// LikesCountCursor 点赞数排序分页游标（内部用）
type LikesCountCursor struct {
	LikesCount int64 // 点赞数游标
	ID         uint  // 视频ID游标（同分兜底用）
}

// ListLikesCountResponse 按点赞数排序的视频流返回
type ListLikesCountResponse struct {
	VideoList            []FeedVideoItem `json:"video_list"`
	NextLikesCountBefore *int64          `json:"next_likes_count_before,omitempty"`
	NextIDBefore         *uint           `json:"next_id_before,omitempty"`
	HasMore              bool            `json:"has_more"`
}

// -----------------------------------------------------------------------------
// 【关注流】 只看我关注的人发的视频 - 对应抖音"关注"tab
// -----------------------------------------------------------------------------

// ListByFollowingRequest 获取我关注的人的视频流请求
type ListByFollowingRequest struct {
	Limit      int   `json:"limit"`       // 每页条数
	LatestTime int64 `json:"latest_time"` // 时间游标，第一页传0
}

// ListByFollowingResponse 获取我关注的人的视频流返回
type ListByFollowingResponse struct {
	VideoList []FeedVideoItem `json:"video_list"`
	NextTime  int64           `json:"next_time"`
	HasMore   bool            `json:"has_more"`
}

// -----------------------------------------------------------------------------
// 【热度推荐流】 抖音首页"推荐"tab核心算法 - 按热度分排序
// -----------------------------------------------------------------------------

// ListByPopularityRequest 按热度排序的推荐流请求
// 设计说明：这是最复杂的分页，支持Redis ZSET排序 + 数据库降级兜底
type ListByPopularityRequest struct {
	Limit          int   `json:"limit"`                      // 每页条数
	AsOf           int64 `json:"as_of"`                      // 热度榜单版本号（服务器返回的分钟时间戳，第一页传0）
	Offset         int   `json:"offset"`                     // Redis ZSET的offset（下一页从这里开始）
	LatestIDBefore *uint `json:"latest_id_before,omitempty"` // 兜底游标

	// DB降级专用字段（Redis挂了的时候才用，前端不用传）
	LatestPopularity int64     `json:"latest_popularity"`
	LatestBefore     time.Time `json:"latest_before"`
}

// ListByPopularityResponse 按热度排序的推荐流返回
// 设计说明：支持双路径返回 - Redis正常走offset，降级了走游标分页
type ListByPopularityResponse struct {
	VideoList  []FeedVideoItem `json:"video_list"`
	AsOf       int64           `json:"as_of"`       // 这个榜单是哪个时间点计算的
	NextOffset int             `json:"next_offset"` // 下一页offset
	HasMore    bool            `json:"has_more"`

	// DB降级专用的下一页游标（降级了自动切到这种模式）
	NextLatestPopularity *int64     `json:"next_latest_popularity,omitempty"`
	NextLatestBefore     *time.Time `json:"next_latest_before,omitempty"`
	NextLatestIDBefore   *uint      `json:"next_latest_id_before,omitempty"`
}
