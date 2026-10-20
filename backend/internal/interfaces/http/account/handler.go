// Package account 账号接口层，负责 HTTP 参数绑定、错误映射和响应组装。
package account

import (
	"errors"
	"net/http"
	"time"

	appaccount "feedsystem_video_go/internal/application/account"
	domainaccount "feedsystem_video_go/internal/domain/account"
	"feedsystem_video_go/internal/interfaces/http/middleware/jwt"

	"github.com/gin-gonic/gin"
)

// Handler 账号 HTTP 处理器。
type Handler struct {
	service *appaccount.Service
}

// NewHandler 创建账号 HTTP 处理器。
func NewHandler(service *appaccount.Service) *Handler {
	return &Handler{service: service}
}

// Register 注册账号。
func (h *Handler) Register(c *gin.Context) {
	var req registerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, err)
		return
	}
	if err := h.service.Register(c.Request.Context(), req.Username, req.Password); err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "account created"})
}

// Login 登录并返回双 Token。
func (h *Handler) Login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, err)
		return
	}
	pair, err := h.service.Login(c.Request.Context(), req.Username, req.Password)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, newTokenResponse(pair))
}

// Refresh 使用 Refresh Token 轮换双 Token。
func (h *Handler) Refresh(c *gin.Context) {
	var req refreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, err)
		return
	}
	if req.RefreshToken == "" {
		writeError(c, domainaccount.ErrTokenInvalid)
		return
	}
	pair, err := h.service.Refresh(c.Request.Context(), req.RefreshToken)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, newTokenResponse(pair))
}

// Logout 撤销当前登录会话。
func (h *Handler) Logout(c *gin.Context) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		writeError(c, domainaccount.ErrPermissionDenied)
		return
	}
	var req logoutRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, err)
		return
	}
	if err := h.service.Logout(c.Request.Context(), accountID, req.RefreshToken); err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "account logged out"})
}

// ChangePassword 修改当前账号密码。
func (h *Handler) ChangePassword(c *gin.Context) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		writeError(c, domainaccount.ErrPermissionDenied)
		return
	}
	var req changePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, err)
		return
	}
	if err := h.service.ChangePassword(c.Request.Context(), accountID, req.OldPassword, req.NewPassword); err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "successfully password changed"})
}

// Rename 修改当前账号用户名，并返回新的双 Token。
func (h *Handler) Rename(c *gin.Context) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		writeError(c, domainaccount.ErrPermissionDenied)
		return
	}
	var req renameRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, err)
		return
	}
	pair, err := h.service.Rename(c.Request.Context(), accountID, req.NewUsername)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, newTokenResponse(pair))
}

// FindByID 按 ID 查询账号。
func (h *Handler) FindByID(c *gin.Context) {
	var req findRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, err)
		return
	}
	account, err := h.service.FindByID(c.Request.Context(), req.ID)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, newAccountResponse(account))
}

// FindByUsername 按用户名查询账号。
func (h *Handler) FindByUsername(c *gin.Context) {
	var req findRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, err)
		return
	}
	account, err := h.service.FindByUsername(c.Request.Context(), req.Username)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, newAccountResponse(account))
}

// newTokenResponse 组装双 Token 响应。
func newTokenResponse(pair *domainaccount.TokenPair) tokenResponse {
	if pair == nil {
		return tokenResponse{}
	}
	now := time.Now()
	return tokenResponse{
		Token:            pair.AccessToken,
		AccessToken:      pair.AccessToken,
		RefreshToken:     pair.RefreshToken,
		TokenType:        "Bearer",
		ExpiresIn:        int64(pair.AccessExpiresAt.Sub(now).Seconds()),
		RefreshExpiresIn: int64(pair.RefreshExpiresAt.Sub(now).Seconds()),
	}
}

// newAccountResponse 组装账号响应。
func newAccountResponse(account *domainaccount.Account) accountResponse {
	if account == nil {
		return accountResponse{}
	}
	return accountResponse{ID: account.ID, Username: account.Username}
}

// writeError 将领域错误映射为 HTTP 状态码。
func writeError(c *gin.Context, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, domainaccount.ErrUsernameRequired),
		errors.Is(err, domainaccount.ErrPasswordRequired):
		status = http.StatusBadRequest
	case errors.Is(err, domainaccount.ErrUsernameTaken):
		status = http.StatusConflict
	case errors.Is(err, domainaccount.ErrAccountNotFound):
		status = http.StatusNotFound
	case errors.Is(err, domainaccount.ErrInvalidCredentials),
		errors.Is(err, domainaccount.ErrTokenInvalid),
		errors.Is(err, domainaccount.ErrTokenRevoked):
		status = http.StatusUnauthorized
	case errors.Is(err, domainaccount.ErrPermissionDenied):
		status = http.StatusForbidden
	}
	c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
}
