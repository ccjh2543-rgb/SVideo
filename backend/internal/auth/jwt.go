// internal/auth/jwt.go
package auth

import (
	"errors"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// jwtSecret 读取 JWT 签名密钥。
// 密钥只能来自环境变量，禁止写入代码或配置文件；未配置时直接返回错误，避免使用不安全默认值。
func jwtSecret() ([]byte, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return nil, errors.New("JWT_SECRET is required")
	}
	return []byte(secret), nil
}

type Claims struct {
	AccountID uint   `json:"account_id"`
	Username  string `json:"username"`
	jwt.RegisteredClaims
}

func GenerateToken(accountID uint, username string) (string, error) {
	now := time.Now()

	claims := Claims{
		AccountID: accountID,
		Username:  username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	// 生成 JWT
	// 1. 选择签名方法（HS256）
	// 2. 传递 claims 到 NewWithClaims 函数
	// 3. 生成 JWT 字符串
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// 4. 读取密钥并签名 JWT 字符串
	secret, err := jwtSecret()
	if err != nil {
		return "", err
	}
	return token.SignedString(secret)
}

func ParseToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(
		tokenString,
		&Claims{},
		func(token *jwt.Token) (interface{}, error) {
			if token.Method == nil || token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, errors.New("unexpected signing method")
			}
			return jwtSecret()
		},
	)
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, jwt.ErrTokenInvalidClaims
	}

	return claims, nil
}
