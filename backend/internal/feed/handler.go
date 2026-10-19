// Package feed 短视频推荐流模块
//
// 【数据流总览】
// 前端滑动刷视频 → Handler接收请求 → Service三级缓存查询 → Repository查数据库
//
//	      ↓
//	回填各级缓存 ← ← ←
//
// 【三级缓存架构】
// L1: 本地内存缓存 (3秒 TTL) → L2: Redis分布式缓存 → L3: MySQL数据库
package feed

import (
	"feedsystem_video_go/internal/middleware/jwt"
	"time"

	"github.com/gin-gonic/gin"
)

// FeedHandler Feed流HTTP接口处理器
// 职责：参数校验 + 上下文转换 + 调用Service + 返回结果
// 数据流：HTTP Request JSON → Request结构体 → Service层
type FeedHandler struct {
	service *FeedService
}

// NewFeedHandler 创建Feed流处理器（依赖注入）
func NewFeedHandler(service *FeedService) *FeedHandler {
	return &FeedHandler{service: service}
}

// -----------------------------------------------------------------------------
// 【最新视频流接口】 按发布时间倒序
// 数据流：
// 前端传 latest_time 游标 → 转成 time.Time → Service.ListLatest → 返回给前端
// -----------------------------------------------------------------------------

// ListLatest 获取最新发布的视频流（时间游标分页）
// 对应抖音首页"最新"Tab
func (f *FeedHandler) ListLatest(c *gin.Context) {
	// 1. 参数绑定 + 校验
	var req ListLatestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// 2. 参数兜底：最多一次拿50条，默认10条
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}

	// 3. 前端毫秒时间戳 → Go time.Time 转换
	var latestTime time.Time
	if req.LatestTime > 0 {
		latestTime = time.UnixMilli(req.LatestTime)
	}

	// 4. 从JWT取当前用户ID（未登录就是0）
	// 作用：标记视频"我有没有点赞过"
	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	// 5. 调用Service业务层
	feedItems, err := f.service.ListLatest(c.Request.Context(), req.Limit, latestTime, viewerAccountID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	// 6. 返回结果给前端
	c.JSON(200, feedItems)
}

// -----------------------------------------------------------------------------
// 【点赞数排行接口】 经典热门榜
// 数据流：
// 前端传 (likes_count_before, id_before) 双游标 → 组装成Cursor结构体 → Service
// -----------------------------------------------------------------------------

// ListLikesCount 按点赞数从高到低排序的视频流
// 对应抖音"热门榜"功能
func (f *FeedHandler) ListLikesCount(c *gin.Context) {
	// 1. 参数绑定
	var req ListLikesCountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}

	// 2. 双游标校验：两个必须同时传或者同时不传
	var cursor *LikesCountCursor
	if req.LikesCountBefore != nil || req.IDBefore != nil {
		if req.LikesCountBefore == nil || req.IDBefore == nil {
			c.JSON(400, gin.H{"error": "likes_count_before and id_before must be provided together"})
			return
		}

		likesCountBefore := *req.LikesCountBefore
		idBefore := *req.IDBefore

		// 3. 游标合法性校验
		if likesCountBefore < 0 {
			c.JSON(400, gin.H{"error": "invalid cursor: likes_count_before must be >= 0"})
			return
		}
		if idBefore == 0 {
			if likesCountBefore != 0 {
				c.JSON(400, gin.H{"error": "invalid cursor: id_before must be > 0"})
				return
			}
		} else {
			// 4. 组装内部游标结构体
			cursor = &LikesCountCursor{
				LikesCount: likesCountBefore,
				ID:         idBefore,
			}
		}
	}

	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	feedItems, err := f.service.ListLikesCount(c.Request.Context(), req.Limit, cursor, viewerAccountID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, feedItems)
}

// -----------------------------------------------------------------------------
// 【关注流接口】 只看我关注的人
// 数据流：
// 取当前登录用户ID → Service查我关注的人的ID列表 → 查这些人的视频
// -----------------------------------------------------------------------------

// ListByFollowing 获取我关注的博主发布的视频流
// 对应抖音"关注"Tab，登录用户才能用
func (f *FeedHandler) ListByFollowing(c *gin.Context) {
	var req ListByFollowingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}

	// 关注流必须是登录用户
	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	var latestTime time.Time
	if req.LatestTime > 0 {
		latestTime = time.Unix(req.LatestTime, 0)
	}

	feedItems, err := f.service.ListByFollowing(c.Request.Context(), req.Limit, latestTime, viewerAccountID)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, feedItems)
}

// -----------------------------------------------------------------------------
// 【热度推荐流接口】 抖音首页核心推荐算法
// 数据流（双路径）：
// 正常路径：Redis ZSET按热度分排序 → O(logN) 超快速
// 降级路径：Redis挂了 → 切到数据库按时间+热度排序 → 用户无感知
// -----------------------------------------------------------------------------

// ListByPopularity 按算法热度排序的推荐流
// 对应抖音首页"推荐"Tab，核心流量入口
func (f *FeedHandler) ListByPopularity(c *gin.Context) {
	var req ListByPopularityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}

	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	var latestPopularity int64
	var latestBefore time.Time
	var latestIDBefore uint

	if req.LatestPopularity < 0 {
		c.JSON(400, gin.H{"error": "latest_popularity must be >= 0"})
		return
	}

	// DB降级模式游标校验
	anyCursor := !req.LatestBefore.IsZero() || req.LatestIDBefore != nil
	if anyCursor {
		if req.LatestBefore.IsZero() || req.LatestIDBefore == nil || *req.LatestIDBefore == 0 {
			c.JSON(400, gin.H{"error": "latest_before and latest_id_before must be provided together"})
			return
		}
		latestPopularity = req.LatestPopularity
		latestBefore = req.LatestBefore
		latestIDBefore = *req.LatestIDBefore
	}

	// 调用热度流核心逻辑
	resp, err := f.service.ListByPopularity(
		c.Request.Context(),
		req.Limit,
		req.AsOf,
		req.Offset,
		viewerAccountID,
		latestPopularity,
		latestBefore,
		latestIDBefore,
	)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, resp)
}
