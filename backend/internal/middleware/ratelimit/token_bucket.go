package ratelimit

import (
	"fmt"
	"math"
	"net/http"
	"time"

	jwt "feedsystem_video_go/internal/middleware/jwt"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
)

// TokenBucketConfig 令牌桶配置。
type TokenBucketConfig struct {
	Capacity        int64         // 桶容量，允许的突发请求数
	RefillPerSecond float64       // 每秒补充的令牌数
	TTL             time.Duration // Redis key 过期时间
}

// TokenBucket 创建基于 Redis Lua 的分布式令牌桶限流中间件。
// Redis 不可用或配置非法时 fail-open，避免限流组件拖垮业务。
func TokenBucket(
	cache *rediscache.Client,
	keyPrefix string,
	cfg TokenBucketConfig,
	keyFunc KeyFunc,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cache == nil || keyFunc == nil || cfg.Capacity <= 0 || cfg.RefillPerSecond <= 0 {
			c.Next()
			return
		}

		subject, ok := keyFunc(c)
		if !ok || subject == "" {
			c.Next()
			return
		}

		ttl := cfg.TTL
		if ttl <= 0 {
			ttl = DefaultBucketTTL(cfg.Capacity, cfg.RefillPerSecond)
		}

		allowed, remaining, err := cache.TakeToken(
			c.Request.Context(),
			buildKey(keyPrefix, subject),
			cfg.Capacity,
			cfg.RefillPerSecond,
			ttl,
		)
		if err != nil {
			c.Next()
			return
		}
		if !allowed {
			c.Header("Retry-After", "1")
			c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "too many requests",
			})
			return
		}

		c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
		c.Next()
	}
}

// DefaultBucketTTL 根据容量和补充速率计算一个足够覆盖桶恢复周期的 TTL。
func DefaultBucketTTL(capacity int64, refillPerSecond float64) time.Duration {
	if capacity <= 0 || refillPerSecond <= 0 {
		return time.Minute
	}
	seconds := math.Ceil(float64(capacity) / refillPerSecond)
	if seconds < 60 {
		seconds = 60
	}
	return time.Duration(seconds) * time.Second
}

// KeyByAccountOrIP 优先按登录账号限流，匿名请求回退到 IP。
func KeyByAccountOrIP(c *gin.Context) (string, bool) {
	if accountID, err := jwt.GetAccountID(c); err == nil && accountID > 0 {
		return fmt.Sprintf("account:%d", accountID), true
	}
	ip := c.ClientIP()
	if ip == "" {
		return "", false
	}
	return "ip:" + ip, true
}
