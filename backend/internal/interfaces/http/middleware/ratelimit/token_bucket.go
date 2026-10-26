package ratelimit

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
	jwt "feedsystem_video_go/internal/interfaces/http/middleware/jwt"

	"github.com/gin-gonic/gin"
)

// localBucket 进程内令牌桶，Redis 不可用时兜底。
type localBucket struct {
	mu       sync.Mutex
	tokens   float64
	last     time.Time
	lastSeen time.Time
}

var (
	localBuckets sync.Map // key -> *localBucket
	localCalls   atomic.Uint64
	localIdleTTL = 10 * time.Minute
)

// TokenBucketConfig 令牌桶配置。
type TokenBucketConfig struct {
	Capacity        int64         // 桶容量，允许的突发请求数
	RefillPerSecond float64       // 每秒补充的令牌数
	TTL             time.Duration // Redis key 过期时间
}

// TokenBucket 创建基于 Redis Lua 的分布式令牌桶限流中间件。
// Redis 不可用时退回进程内令牌桶，保证故障期间仍然有基础限流能力。
func TokenBucket(
	cache *rediscache.Client,
	keyPrefix string,
	cfg TokenBucketConfig,
	keyFunc KeyFunc,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		if keyFunc == nil || cfg.Capacity <= 0 || cfg.RefillPerSecond <= 0 {
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

		redisKey := buildKey(keyPrefix, subject)
		var allowed bool
		var remaining int64
		var err error
		if cache != nil {
			allowed, remaining, err = cache.TakeToken(
				c.Request.Context(),
				redisKey,
				cfg.Capacity,
				cfg.RefillPerSecond,
				ttl,
			)
		} else {
			err = fmt.Errorf("redis cache disabled")
		}

		if err != nil {
			// Redis 故障：使用本地令牌桶兜底，而不是直接放行。
			if localCalls.Add(1)%1024 == 0 {
				cleanupLocalBuckets()
			}
			if !allowLocal(redisKey, cfg.Capacity, cfg.RefillPerSecond) {
				c.Header("Retry-After", "1")
				c.Header("X-RateLimit-Remaining", "0")
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"error": "too many requests",
				})
				return
			}
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

// allowLocal 使用进程内令牌桶判断是否放行。
// 多实例部署时每个实例独立限流，属于故障期的降级精度损失。
func allowLocal(key string, capacity int64, refillPerSecond float64) bool {
	value, _ := localBuckets.LoadOrStore(key, &localBucket{
		tokens: float64(capacity),
		last:   time.Now(),
	})
	bucket := value.(*localBucket)

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	now := time.Now()
	if elapsed := now.Sub(bucket.last).Seconds(); elapsed > 0 {
		bucket.tokens = math.Min(float64(capacity), bucket.tokens+elapsed*refillPerSecond)
		bucket.last = now
	}
	bucket.lastSeen = now
	if bucket.tokens >= 1 {
		bucket.tokens--
		return true
	}
	return false
}

// cleanupLocalBuckets 清理长时间未使用的本地桶，避免 key 无限增长。
func cleanupLocalBuckets() {
	localBuckets.Range(func(key, value any) bool {
		bucket := value.(*localBucket)
		bucket.mu.Lock()
		idle := time.Since(bucket.lastSeen)
		bucket.mu.Unlock()
		if idle > localIdleTTL {
			localBuckets.Delete(key)
		}
		return true
	})
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
