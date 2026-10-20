package ws

import (
	"net/http"
	"strings"

	domainaccount "feedsystem_video_go/internal/domain/account"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// Handler WebSocket 升级处理器。
type Handler struct {
	tokens   domainaccount.TokenService
	hub      *Hub
	upgrader websocket.Upgrader
}

// NewHandler 创建 WebSocket 处理器。
func NewHandler(tokens domainaccount.TokenService, hub *Hub) *Handler {
	return &Handler{
		tokens: tokens,
		hub:    hub,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// 课程项目允许跨域；生产环境应限制 Origin。
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// Handle 校验 Access Token 并升级 WebSocket。
// 客户端连接示例：/ws?token=<access_token>
func (h *Handler) Handle(c *gin.Context) {
	token := strings.TrimSpace(c.Query("token"))
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
		return
	}
	claims, err := h.tokens.ParseAccess(token)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
		return
	}

	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	newClient(h.hub, conn, claims.AccountID)
}
