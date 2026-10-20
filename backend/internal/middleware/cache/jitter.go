// Package cache 提供缓存通用能力：TTL 抖动、逻辑过期、布隆过滤器。
package cache

import (
	"math"
	"math/rand"
	"time"
)

// Jitter 在 base 基础上增加 ±ratio 比例的随机抖动，避免缓存同时失效。
// ratio 建议取 0.1~0.3。
func Jitter(base time.Duration, ratio float64) time.Duration {
	if base <= 0 {
		return base
	}
	if ratio <= 0 {
		return base
	}
	if ratio > 1 {
		ratio = 1
	}

	delta := float64(base) * ratio
	offset := (rand.Float64()*2 - 1) * delta
	result := time.Duration(float64(base) + offset)
	if result <= 0 {
		return time.Duration(math.Max(float64(time.Millisecond), float64(base)/2))
	}
	return result
}
