package social

import (
	"net/http"

	appsocial "feedsystem_video_go/internal/application/social"
	domainsocial "feedsystem_video_go/internal/domain/social"
	"feedsystem_video_go/internal/interfaces/http/middleware/jwt"

	"github.com/gin-gonic/gin"
)

// SocialHandler 社交 HTTP 处理器。
type SocialHandler struct {
	service *appsocial.SocialService
}

// NewSocialHandler 创建社交处理器。
func NewSocialHandler(service *appsocial.SocialService) *SocialHandler {
	return &SocialHandler{service: service}
}

// Follow 关注博主。
func (h *SocialHandler) Follow(c *gin.Context) {
	var req FollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.VloggerID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "vlogger_id is required"})
		return
	}
	followerID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}

	relation := &domainsocial.Social{FollowerID: followerID, VloggerID: req.VloggerID}
	if err := h.service.Follow(c.Request.Context(), relation); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "followed"})
}

// Unfollow 取消关注。
func (h *SocialHandler) Unfollow(c *gin.Context) {
	var req UnfollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.VloggerID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "vlogger_id is required"})
		return
	}
	followerID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}

	relation := &domainsocial.Social{FollowerID: followerID, VloggerID: req.VloggerID}
	if err := h.service.Unfollow(c.Request.Context(), relation); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "unfollowed"})
}

// GetAllFollowers 查询粉丝列表；未指定 vlogger_id 时查询当前用户。
func (h *SocialHandler) GetAllFollowers(c *gin.Context) {
	var req GetAllFollowersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	vloggerID := req.VloggerID
	if vloggerID == 0 {
		accountID, err := jwt.GetAccountID(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		vloggerID = accountID
	}

	followers, err := h.service.GetFollowers(c.Request.Context(), vloggerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, GetAllFollowersResponse{Followers: toAccountInfos(followers)})
}

// GetAllVloggers 查询关注列表；未指定 follower_id 时查询当前用户。
func (h *SocialHandler) GetAllVloggers(c *gin.Context) {
	var req GetAllVloggersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	followerID := req.FollowerID
	if followerID == 0 {
		accountID, err := jwt.GetAccountID(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		followerID = accountID
	}

	vloggers, err := h.service.GetFollowing(c.Request.Context(), followerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, GetAllVloggersResponse{Vloggers: toAccountInfos(vloggers)})
}

// toAccountInfos 把领域账号摘要转换为接口层 DTO。
func toAccountInfos(accounts []*domainsocial.AccountBrief) []AccountInfo {
	if accounts == nil {
		return nil
	}
	result := make([]AccountInfo, 0, len(accounts))
	for _, account := range accounts {
		result = append(result, AccountInfo{ID: account.ID, Username: account.Username})
	}
	return result
}
