package feed

import "time"

// FeedItem 视频流卡片展示模型。
// 只包含前端需要的字段，不暴露账号敏感信息；JSON 序列化由接口层 DTO 负责。
type FeedItem struct {
	ID          uint
	AuthorID    uint
	AuthorName  string
	Title       string
	Description string
	PlayURL     string
	CoverURL    string
	CreateTime  int64 // 秒级时间戳
	LikesCount  int64
	IsLiked     bool
}

// LatestResult 最新视频流结果。
type LatestResult struct {
	VideoList []FeedItem
	NextTime  int64
	NextID    *uint
	HasMore   bool
}

// LikesCountResult 点赞数排行结果。
type LikesCountResult struct {
	VideoList            []FeedItem
	NextLikesCountBefore *int64
	NextIDBefore         *uint
	HasMore              bool
}

// FollowingResult 关注流结果。
type FollowingResult struct {
	VideoList []FeedItem
	NextTime  int64
	HasMore   bool
}

// PopularityResult 热度推荐流结果。
// 正常路径使用 AsOf + NextOffset 翻页；降级路径使用后三个游标字段。
type PopularityResult struct {
	VideoList            []FeedItem
	AsOf                 int64
	NextOffset           int
	HasMore              bool
	NextLatestPopularity *int64
	NextLatestBefore     *time.Time
	NextLatestIDBefore   *uint
}
