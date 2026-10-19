package account

import (
	"context"
	"time"
)

// Repository 账号仓储接口。
// 具体实现放在 infrastructure/persistence/mysql。
type Repository interface {
	Create(ctx context.Context, account *Account) error
	FindByID(ctx context.Context, id uint) (*Account, error)
	FindByUsername(ctx context.Context, username string) (*Account, error)
	UpdateUsername(ctx context.Context, id uint, username string) error
	UpdatePassword(ctx context.Context, id uint, passwordHash string) error
}

// RefreshToken 刷新令牌持久化记录。
// 数据库保存完整记录用于审计和重放检测，Redis 只作为后续的性能缓存。
type RefreshToken struct {
	ID        uint
	AccountID uint
	JTI       string
	FamilyID  string
	ExpiresAt time.Time
	RevokedAt *time.Time
	CreatedAt time.Time
}

// RefreshTokenRepository 刷新令牌仓储接口。
type RefreshTokenRepository interface {
	// Create 保存一条刷新令牌记录。
	Create(ctx context.Context, token *RefreshToken) error
	// FindByJTI 按 JWT ID 查询刷新令牌。
	FindByJTI(ctx context.Context, jti string) (*RefreshToken, error)
	// RevokeIfActive 原子地把未撤销的令牌标记为已撤销。
	// 返回 false 表示令牌已经被其他并发请求撤销，可用于检测刷新令牌重放。
	RevokeIfActive(ctx context.Context, jti string) (bool, error)
	// RevokeFamily 撤销同一登录会话下的全部刷新令牌。
	RevokeFamily(ctx context.Context, familyID string) error
	// RevokeAllByAccount 撤销账号的全部刷新令牌，用于改密、改名和全端登出。
	RevokeAllByAccount(ctx context.Context, accountID uint) error
}
