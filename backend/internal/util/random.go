package util

import (
	"crypto/rand"
	"encoding/hex"
)

// RandHex 生成 n 字节的随机数据的十六进制字符串表示。
// 返回长度为 2*n 的十六进制字符串，适用于生成唯一 ID、Token 等场景。
func RandHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
