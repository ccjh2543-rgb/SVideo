// Package jwt 提供基于双 Token 的 Gin 鉴权中间件。
//
// 该包位于接口层，只依赖领域层定义的 TokenService 接口，
// 不直接依赖具体的 JWT 实现，便于替换和测试。
package jwt

import (
	"errors"
	"net/http"
	"strings"

	domainaccount "feedsystem_video_go/internal/domain/account"

	"github.com/gin-gonic/gin"
)

const (
	contextAccountID = "accountID"
	contextUsername  = "username"
)

// JWTAuth 强制校验 Access Token。
// 缺少 Authorization 头或令牌无效时返回 401。
func JWTAuth(tokens domainaccount.TokenService) gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, err := parseAccessToken(c, tokens)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		setClaims(c, claims)
		c.Next()
	}
}

// SoftJWTAuth 软鉴权：
// 没有 Authorization 头时允许匿名访问；携带了令牌则必须校验通过。
func SoftJWTAuth(tokens domainaccount.TokenService) gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.TrimSpace(c.GetHeader("Authorization")) == "" {
			c.Next()
			return
		}
		claims, err := parseAccessToken(c, tokens)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		setClaims(c, claims)
		c.Next()
	}
}

// parseAccessToken 从 Authorization 头解析 Access Token。
func parseAccessToken(c *gin.Context, tokens domainaccount.TokenService) (*domainaccount.Claims, error) {
	if tokens == nil {
		return nil, domainaccount.ErrTokenInvalid
	}
	authHeader := strings.TrimSpace(c.GetHeader("Authorization"))
	if authHeader == "" {
		return nil, errors.New("missing authorization header")
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, errors.New("invalid authorization header")
	}
	claims, err := tokens.ParseAccess(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, domainaccount.ErrTokenInvalid
	}
	return claims, nil
}

// setClaims 将认证信息写入 Gin 上下文，供后续 Handler 使用。
func setClaims(c *gin.Context, claims *domainaccount.Claims) {
	if claims == nil {
		return
	}
	c.Set(contextAccountID, claims.AccountID)
	c.Set(contextUsername, claims.Username)
}

// GetAccountID 从 Gin 上下文中提取 accountID。
func GetAccountID(c *gin.Context) (uint, error) {
	value, exists := c.Get(contextAccountID)
	if !exists {
		return 0, errors.New("accountID not found")
	}
	accountID, ok := value.(uint)
	if !ok {
		return 0, errors.New("accountID has invalid type")
	}
	return accountID, nil
}

// GetUsername 从 Gin 上下文中提取 username。
func GetUsername(c *gin.Context) (string, error) {
	value, exists := c.Get(contextUsername)
	if !exists {
		return "", errors.New("username not found")
	}
	username, ok := value.(string)
	if !ok {
		return "", errors.New("username has invalid type")
	}
	return username, nil
}
