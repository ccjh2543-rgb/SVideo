package ratelimit

import (
	jwt "feedsystem_video_go/internal/middleware/jwt"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type KeyFunc func(*gin.Context) (string, bool) // 限流维度提取函数：返回用于限流的key和是否成功

// Limit 创建限流中间件
//
//	cache: Redis客户端实例
//	keyPrefix: 限流计数器的Redis key前缀（区分不同接口，比如"login"、"register"）
//	maxRequests: 时间窗口内最大请求次数
//	window: 限流时间窗口（比如1分钟、1小时）
//	keyFunc: 限流维度函数：按IP限流/按用户ID限流等
func Limit(
	cache *rediscache.Client, // Redis客户端
	keyPrefix string, // 限流key前缀（区分不同业务场景）
	maxRequests int64, // 窗口内最大请求数
	window time.Duration, // 限流时间窗口
	keyFunc KeyFunc, // 限流维度提取函数
) gin.HandlerFunc {
	return func(c *gin.Context) {
		/*
			c.Next()
			return
			表示限流失败，放行

			if count > maxRequests
			表示超过最大请求数，返回429错误和JSON响应
			限流成功
		*/

		// 限流参数校验
		if cache == nil || keyFunc == nil || maxRequests <= 0 || window <= 0 {
			c.Next()
			return
		}
		// 提取限流维度
		subject, ok := keyFunc(c)
		if !ok {
			c.Next()
			return
		}
		// 构造Redis流计数器key
		key := buildKey(keyPrefix, subject)
		// 增加计数器值并设置过期时间
		count, err := cache.IncrementWithExpire(c.Request.Context(), key, window)
		if err != nil {
			c.Next()
			return
		}
		// 检查是否超过最大请求数
		if count > maxRequests {
			// 中断并返回429错误和JSON响应
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "too many requests",
			})
			return
		}
		// 放行，继续处理请求
		c.Next()
	}
}

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
