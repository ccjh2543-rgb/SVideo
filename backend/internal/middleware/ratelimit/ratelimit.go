package ratelimit

import (
	jwt "feedsystem_video_go/internal/middleware/jwt"
	"fmt"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type KeyFunc func(*gin.Context) (string, bool) // 限流维度提取函数：返回用于限流的key和是否成功

// buildKey 构造Redis限流计数器的key
func buildKey(keyPrefix, subject string) string {
	keyPrefix = strings.TrimSpace(keyPrefix)
	if keyPrefix == "" {
		keyPrefix = "default"
	}
	return fmt.Sprintf("feedsystem:ratelimit:%s:%s", keyPrefix, strings.TrimSpace(subject))
}

// KeyByIP 按客户端IP限流
func KeyByIP(c *gin.Context) (string, bool) {
	ip := strings.TrimSpace(c.ClientIP())
	if ip == "" {
		return "", false
	}
	return ip, true
}

// KeyByAccount 按登录用户ID限流（已登录用户用这个）
func KeyByAccount(c *gin.Context) (string, bool) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil || accountID == 0 {
		return "", false
	}
	return strconv.FormatUint(uint64(accountID), 10), true
}
