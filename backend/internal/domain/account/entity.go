// Package account 账号与认证领域。
//
// 该包只包含领域实体、领域错误和仓储接口，不依赖 GORM、Redis、Gin 等外部实现。
package account

import (
	"errors"
	"strings"
)

// 领域错误：应用层和接口层通过这些哨兵错误做统一映射。
var (
	ErrUsernameRequired   = errors.New("username is required")
	ErrPasswordRequired   = errors.New("password is required")
	ErrUsernameTaken      = errors.New("username already exists")
	ErrAccountNotFound    = errors.New("account not found")
	ErrInvalidCredentials = errors.New("invalid username or password")
	ErrTokenInvalid       = errors.New("invalid token")
	ErrTokenRevoked       = errors.New("token has been revoked")
	ErrPermissionDenied   = errors.New("permission denied")
)

// Account 账号聚合根。
// 只保存账号自身的业务状态，密码以哈希形式保存，不保存任何 Token。
type Account struct {
	ID           uint
	Username     string
	PasswordHash string
}

// NormalizeUsername 清理用户名首尾空白，并校验非空。
func NormalizeUsername(username string) (string, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return "", ErrUsernameRequired
	}
	return username, nil
}

// ValidatePassword 校验原始密码非空。
// 密码强度策略由应用层决定，领域层只保证最基本的非空约束。
func ValidatePassword(password string) error {
	if password == "" {
		return ErrPasswordRequired
	}
	return nil
}

// Rename 修改用户名前的领域校验。
func (a *Account) Rename(newUsername string) error {
	if a == nil {
		return ErrAccountNotFound
	}
	normalized, err := NormalizeUsername(newUsername)
	if err != nil {
		return err
	}
	a.Username = normalized
	return nil
}
