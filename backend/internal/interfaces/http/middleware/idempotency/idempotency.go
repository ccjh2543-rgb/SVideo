// Package idempotency 提供基于 Idempotency-Key 的接口幂等中间件。
package idempotency

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
	jwt "feedsystem_video_go/internal/interfaces/http/middleware/jwt"

	"github.com/gin-gonic/gin"
)

// fallbackTTL 客户端未显式提供幂等键时的去重窗口。
// 只用于挡住双击和网络重试，窗口过长会把用户的正常重复操作误判为重复请求。
const fallbackTTL = 2 * time.Second

// Require 提供接口级幂等保护。
//
// 优先使用客户端传入的 Idempotency-Key；未传时按“账号 + 方法 + 路径 + 请求体”
// 生成指纹做短窗口去重，因此写接口无需依赖客户端改造也能获得基础幂等能力。
// 同一个账号 + 同一个 key 在 TTL 内只允许成功进入业务一次。
func Require(cache *rediscache.Client, prefix string, ttl time.Duration) gin.HandlerFunc {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return func(c *gin.Context) {
		key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		effectiveTTL := ttl
		if key == "" {
			fingerprint, err := requestFingerprint(c)
			if err != nil {
				c.Next()
				return
			}
			key = fingerprint
			effectiveTTL = fallbackTTL
		}
		if cache == nil {
			c.Next()
			return
		}

		accountID, _ := jwt.GetAccountID(c)
		redisKey := fmt.Sprintf("idem:%s:%d:%s", prefix, accountID, key)
		ok, err := cache.SetNX(c.Request.Context(), redisKey, []byte("1"), effectiveTTL)
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

// requestFingerprint 生成请求指纹，并恢复请求体供后续处理器读取。
func requestFingerprint(c *gin.Context) (string, error) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return "", err
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))

	accountID, _ := jwt.GetAccountID(c)
	hash := sha256.New()
	hash.Write([]byte(strconv.FormatUint(uint64(accountID), 10)))
	hash.Write([]byte("|"))
	hash.Write([]byte(c.Request.Method))
	hash.Write([]byte("|"))
	hash.Write([]byte(c.Request.URL.Path))
	hash.Write([]byte("|"))
	hash.Write(body)
	return hex.EncodeToString(hash.Sum(nil)), nil
}
