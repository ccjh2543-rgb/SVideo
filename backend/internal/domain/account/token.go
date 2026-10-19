package account

import "time"

// TokenType 令牌类型，用于防止 Access Token 与 Refresh Token 混用。
type TokenType string

const (
	TokenTypeAccess  TokenType = "access"
	TokenTypeRefresh TokenType = "refresh"
)

// Claims 解析后的令牌声明。
type Claims struct {
	AccountID uint
	Username  string
	TokenType TokenType
	JTI       string
	FamilyID  string
	ExpiresAt time.Time
}

// TokenPair 一次签发得到的双 Token。
type TokenPair struct {
	AccessToken      string
	RefreshToken     string
	AccessExpiresAt  time.Time
	RefreshExpiresAt time.Time
	RefreshJTI       string
	FamilyID         string
}

// TokenService 令牌服务接口，由 infrastructure/security 实现。
type TokenService interface {
	// Issue 签发 Access Token 和 Refresh Token。
	// familyID 为空时创建新的登录会话，非空时沿用原会话（刷新轮换场景）。
	Issue(accountID uint, username string, familyID string) (*TokenPair, error)
	// ParseAccess 解析 Access Token，类型不匹配时返回 ErrTokenInvalid。
	ParseAccess(token string) (*Claims, error)
	// ParseRefresh 解析 Refresh Token，类型不匹配时返回 ErrTokenInvalid。
	ParseRefresh(token string) (*Claims, error)
}

// PasswordHasher 密码哈希接口，由 infrastructure/security 实现。
type PasswordHasher interface {
	Hash(password string) (string, error)
	Compare(hash string, password string) error
}
