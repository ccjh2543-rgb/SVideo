package jwt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/auth"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
)

// JWTAuth 返回一个 Gin 中间件，用于强制 JWT 验证。
// 如果请求中没有合法的 Authorization 头，或者 Token 无效/已吊销，则返回 401。
// 验证通过后，将 accountID 和 username 存入上下文，后续处理函数可通过 GetAccountID / GetUsername 获取。
// 验证逻辑：比对请求中的 Token 与 Redis 中缓存的当前有效 Token 或数据库中的 Token。
func JWTAuth(accountRepo *account.AccountRepository, cache *rediscache.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 读取 Authorization 头，格式应为 "Bearer <token>"
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			return
		}

		// 按空格分割，期望得到 ["Bearer", "<token>"]
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization header"})
			return
		}

		tokenString := parts[1]

		// 解析并校验 JWT 签名与有效期，返回 Claims
		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}

		// 进入核心验证流程：检查 Token 是否与当前有效 Token 一致
		check(c, claims, tokenString, accountRepo, cache)
	}
}

// SoftJWTAuth 返回一个“软性” JWT 中间件。
// 与 JWTAuth 的区别：如果请求没有 Authorization 头，不会拒绝，而是直接放行给后续处理（允许匿名访问）。
// 但如果携带了 Authorization 头，则必须校验通过，否则返回 401。
func SoftJWTAuth(accountRepo *account.AccountRepository, cache *rediscache.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			// 没有 Token，直接放行，允许未登录访问
			c.Next()
			return
		}

		// 以下校验逻辑与 JWTAuth 完全一致
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization header"})
			return
		}

		tokenString := parts[1]

		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}

		check(c, claims, tokenString, accountRepo, cache)
	}
}

// check 执行 Token 有效性验证的核心逻辑，被 JWTAuth 和 SoftJWTAuth 复用。
// 它先尝试从 Redis 中获取当前有效 Token，若命中则比对；否则回退到数据库查询，并将结果缓存到 Redis。
// 验证通过后，将 accountID 和 username 注入 Gin 上下文，并调用 c.Next() 放行。
func check(c *gin.Context, claims *auth.Claims, tokenString string, accountRepo *account.AccountRepository, cache *rediscache.Client) {
	// 构造缓存键：account:<accountID>
	key := fmt.Sprintf("account:%d", claims.AccountID)

	// 优先查询 Redis（如果可用），提高性能
	if cache != nil {
		// 设置 50ms 超时，避免 Redis 阻塞拖慢请求
		cacheCtx, cancel := context.WithTimeout(c.Request.Context(), 50*time.Millisecond)
		defer cancel()

		b, err := cache.GetBytes(cacheCtx, key)
		if err == nil {
			// Redis 命中：比较缓存中的 Token 是否与请求携带的完全相同
			if string(b) != tokenString {
				// Token 不匹配，说明用户已在其他地方登录或 Token 已被吊销
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
				return
			}
			// 验证通过，设置用户信息到上下文
			c.Set("accountID", claims.AccountID)
			c.Set("username", claims.Username)
			c.Next()
			return
		}
		// Redis 未命中或出错，继续走数据库兜底逻辑
	}

	// Redis 不可用或未命中，查询数据库获取用户当前的 Token
	accountInfo, err := accountRepo.FindByID(c.Request.Context(), claims.AccountID)
	if err != nil || accountInfo.Token == "" || accountInfo.Token != tokenString {
		// 数据库错误、账户无 Token 或 Token 不匹配均视为无效
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
		return
	}

	// 数据库验证通过，尝试将 Token 回写到 Redis 以加速后续请求
	if cache != nil {
		cacheCtx, cancel := context.WithTimeout(c.Request.Context(), 50*time.Millisecond)
		defer cancel()

		// 设置 24 小时过期时间
		if err := cache.SetBytes(cacheCtx, key, []byte(tokenString), 24*time.Hour); err != nil {
			// 缓存写入失败仅记录日志，不影响正常业务流程
			log.Printf("failed to set cache: %v", err)
		}
	}

	// 将认证通过的用户信息注入上下文
	c.Set("accountID", claims.AccountID)
	c.Set("username", claims.Username)
	c.Next()
}

// GetAccountID 从 Gin 上下文中提取验证中间件注入的 accountID。
// 若不存在或类型错误则返回 error。
func GetAccountID(c *gin.Context) (uint, error) {
	uidValue, exists := c.Get("accountID")
	if !exists {
		return 0, errors.New("accountID not found")
	}

	accountID, ok := uidValue.(uint)
	if !ok {
		return 0, errors.New("accountID has invalid type")
	}

	return accountID, nil
}

// GetUsername 从 Gin 上下文中提取验证中间件注入的 username。
// 若不存在或类型错误则返回 error。
func GetUsername(c *gin.Context) (string, error) {
	val, exists := c.Get("username")
	if !exists {
		return "", errors.New("username not found")
	}

	username, ok := val.(string)
	if !ok {
		return "", errors.New("username has invalid type")
	}

	return username, nil
}
