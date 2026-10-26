package feed

import (
	"context"
	"errors"
	"net/http"
	"time"

	appfeed "feedsystem_video_go/internal/application/feed"
	domainfeed "feedsystem_video_go/internal/domain/feed"
	"feedsystem_video_go/internal/interfaces/http/middleware/jwt"

	"github.com/gin-gonic/gin"
)

// FeedHandler Feed 流 HTTP 处理器。
// 只负责参数校验、身份提取和 DTO 转换，业务编排在 application 层。
type FeedHandler struct {
	service *appfeed.FeedService
}

// NewFeedHandler 创建 Feed 流处理器。
func NewFeedHandler(service *appfeed.FeedService) *FeedHandler {
	return &FeedHandler{service: service}
}

// ListLatest 获取最新视频流。
func (h *FeedHandler) ListLatest(c *gin.Context) {
	var req ListLatestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}

	var latestTime time.Time
	if req.LatestTime > 0 {
		latestTime = time.UnixMilli(req.LatestTime)
	}

	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	var latestID uint
	if req.LatestID != nil {
		latestID = *req.LatestID
	}

	result, err := h.service.ListLatest(c.Request.Context(), req.Limit, latestTime, latestID, viewerAccountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, ListLatestResponse{
		VideoList: toFeedVideoItems(result.VideoList),
		NextTime:  result.NextTime,
		NextID:    result.NextID,
		HasMore:   result.HasMore,
	})
}

// ListLikesCount 按点赞数从高到低返回视频流。
func (h *FeedHandler) ListLikesCount(c *gin.Context) {
	var req ListLikesCountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}

	var cursor *domainfeed.LikesCountCursor
	if req.LikesCountBefore != nil || req.IDBefore != nil {
		if req.LikesCountBefore == nil || req.IDBefore == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "likes_count_before and id_before must be provided together"})
			return
		}

		likesCountBefore := *req.LikesCountBefore
		idBefore := *req.IDBefore
		if likesCountBefore < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cursor: likes_count_before must be >= 0"})
			return
		}
		if idBefore == 0 {
			if likesCountBefore != 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cursor: id_before must be > 0"})
				return
			}
		} else {
			cursor = &domainfeed.LikesCountCursor{
				LikesCount: likesCountBefore,
				ID:         idBefore,
			}
		}
	}

	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	result, err := h.service.ListLikesCount(c.Request.Context(), req.Limit, cursor, viewerAccountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, ListLikesCountResponse{
		VideoList:            toFeedVideoItems(result.VideoList),
		NextLikesCountBefore: result.NextLikesCountBefore,
		NextIDBefore:         result.NextIDBefore,
		HasMore:              result.HasMore,
	})
}

// ListByFollowing 获取当前用户关注博主发布的视频流。
func (h *FeedHandler) ListByFollowing(c *gin.Context) {
	var req ListByFollowingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}

	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	var latestTime time.Time
	if req.LatestTime > 0 {
		// 关注流游标统一使用毫秒时间戳，与 Redis ZSet 的 score 精度一致。
		latestTime = time.UnixMilli(req.LatestTime)
	}
	var latestID uint
	if req.LatestID != nil {
		latestID = *req.LatestID
	}

	// 关注流总预算：Redis 阶段 120ms + 数据库降级 300ms，留出余量。
	reqCtx, cancel := context.WithTimeout(c.Request.Context(), 500*time.Millisecond)
	defer cancel()

	result, err := h.service.ListByFollowing(reqCtx, req.Limit, latestTime, latestID, viewerAccountID)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.Header("Retry-After", "1")
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "feed temporarily unavailable"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, ListByFollowingResponse{
		VideoList: toFeedVideoItems(result.VideoList),
		NextTime:  result.NextTime,
		NextID:    result.NextID,
		HasMore:   result.HasMore,
	})
}

// ListByPopularity 按热度算法返回推荐流。
func (h *FeedHandler) ListByPopularity(c *gin.Context) {
	var req ListByPopularityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}
	// as_of 白名单：必须是整点、不能是未来、最多回看 7 天，避免任意 key 击穿缓存。
	if req.AsOf > 0 {
		if req.AsOf%3600 != 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "as_of must be an hour boundary"})
			return
		}
		asOf := time.Unix(req.AsOf, 0).UTC()
		now := time.Now().UTC()
		if asOf.After(now.Add(time.Hour)) || asOf.Before(now.Add(-7*24*time.Hour)) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "as_of out of range"})
			return
		}
	}

	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	var latestPopularity int64
	var latestBefore time.Time
	var latestIDBefore uint

	if req.LatestPopularity < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "latest_popularity must be >= 0"})
		return
	}

	anyCursor := !req.LatestBefore.IsZero() || req.LatestIDBefore != nil
	if anyCursor {
		if req.LatestBefore.IsZero() || req.LatestIDBefore == nil || *req.LatestIDBefore == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "latest_before and latest_id_before must be provided together"})
			return
		}
		latestPopularity = req.LatestPopularity
		latestBefore = req.LatestBefore
		latestIDBefore = *req.LatestIDBefore
	}

	result, err := h.service.ListByPopularity(
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, ListByPopularityResponse{
		VideoList:            toFeedVideoItems(result.VideoList),
		AsOf:                 result.AsOf,
		NextOffset:           result.NextOffset,
		HasMore:              result.HasMore,
		NextLatestPopularity: result.NextLatestPopularity,
		NextLatestBefore:     result.NextLatestBefore,
		NextLatestIDBefore:   result.NextLatestIDBefore,
	})
}

// toFeedVideoItems 把应用层展示模型转换为接口层 DTO。
func toFeedVideoItems(items []appfeed.FeedItem) []FeedVideoItem {
	if items == nil {
		return nil
	}
	result := make([]FeedVideoItem, 0, len(items))
	for _, item := range items {
		result = append(result, FeedVideoItem{
			ID: item.ID,
			Author: FeedAuthor{
				ID:       item.AuthorID,
				Username: item.AuthorName,
			},
			Title:       item.Title,
			Description: item.Description,
			PlayURL:     item.PlayURL,
			CoverURL:    item.CoverURL,
			CreateTime:  item.CreateTime,
			LikesCount:  item.LikesCount,
			IsLiked:     item.IsLiked,
		})
	}
	return result
}
