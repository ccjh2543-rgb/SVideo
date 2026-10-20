// Package idempotency 提供基于 Idempotency-Key 的接口幂等中间件。
package idempotency

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	jwt "feedsystem_video_go/internal/middleware/jwt"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
)

// Require 要求客户端可选携带 Idempotency-Key。
// 同一个账号 + 同一个 key 在 TTL 内只允许成功进入业务一次。
// 未携带 key 时直接放行，保持对旧客户端的兼容。
func Require(cache *rediscache.Client, prefix string, ttl time.Duration) gin.HandlerFunc {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return func(c *gin.Context) {
		key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		if key == "" || cache == nil {
			c.Next()
			return
		}

		accountID, _ := jwt.GetAccountID(c)
		redisKey := fmt.Sprintf("idem:%s:%d:%s", prefix, accountID, key)
		ok, err := cache.SetNX(c.Request.Context(), redisKey, []byte("1"), ttl)
		if err != nil {
			// Redis 故障时 fail-open，避免幂等组件阻塞业务。
			c.Next()
			return
		}
		if !ok {
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{
				"error": "duplicate request",
			})
			return
		}

		c.Next()
		// 服务端错误允许客户端使用同一个幂等键重试。
		if c.Writer.Status() >= http.StatusInternalServerError {
			_ = cache.Del(c.Request.Context(), redisKey)
		}
	}
}
