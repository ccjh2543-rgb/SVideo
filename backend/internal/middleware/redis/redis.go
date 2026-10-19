package redis

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/util"
	"strconv"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func NewFromEnv(cfg *config.RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	return &Client{rdb: rdb}, nil
}

func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Ping(ctx).Err()
}

func IsMiss(err error) bool {
	return err == redis.Nil
}

func (c *Client) Lock(ctx context.Context, key string, ttl time.Duration) (token string, ok bool, err error) {
	if c == nil || c.rdb == nil {
		return "", false, nil
	}
	token, err = util.RandHex(16)
	if err != nil {
		return "", false, err
	}
	ok, err = c.rdb.SetNX(ctx, key, token, ttl).Result()
	return token, ok, err
}

var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

var incrementWithExpireScript = redis.NewScript(`
local count = redis.call("INCR", KEYS[1])
if count == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return count
`)

// tokenBucketScript 基于 Redis Hash 的令牌桶 Lua 实现。
// KEYS[1] 桶 key；ARGV[1] 容量；ARGV[2] 每秒补充速率；
// ARGV[3] 当前毫秒时间戳；ARGV[4] key 过期毫秒数。
// 返回 {是否放行, 剩余令牌数}。
var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local data = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

local delta = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + delta * refill)

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call("HSET", key, "tokens", tokens, "ts", now)
redis.call("PEXPIRE", key, ttl)
return {allowed, math.floor(tokens)}
`)

func (c *Client) Unlock(ctx context.Context, key string, token string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	_, err := unlockScript.Run(ctx, c.rdb, []string{key}, token).Result()
	return err
}

func (c *Client) IncrementWithExpire(ctx context.Context, key string, expire time.Duration) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, nil
	}
	return incrementWithExpireScript.Run(
		ctx,
		c.rdb,
		[]string{key},
		expire.Milliseconds(),
	).Int64()
}

// TakeToken 从令牌桶中尝试取一个令牌。
// 返回 allowed=false 表示令牌不足；remaining 为当前剩余令牌数。
// Redis 不可用时返回错误，由调用方决定是否 fail-open。
func (c *Client) TakeToken(
	ctx context.Context,
	key string,
	capacity int64,
	refillPerSecond float64,
	ttl time.Duration,
) (allowed bool, remaining int64, err error) {
	if c == nil || c.rdb == nil {
		return false, 0, errors.New("redis is not available")
	}
	if capacity <= 0 || refillPerSecond <= 0 || ttl <= 0 {
		return false, 0, errors.New("invalid token bucket config")
	}

	values, err := tokenBucketScript.Run(
		ctx,
		c.rdb,
		[]string{key},
		capacity,
		refillPerSecond,
		time.Now().UnixMilli(),
		ttl.Milliseconds(),
	).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(values) != 2 {
		return false, 0, errors.New("unexpected token bucket result")
	}

	allowedValue, err := toInt64(values[0])
	if err != nil {
		return false, 0, err
	}
	remaining, err = toInt64(values[1])
	if err != nil {
		return false, 0, err
	}
	return allowedValue == 1, remaining, nil
}

// toInt64 兼容 go-redis 返回的 int64 和字符串类型。
func toInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, errors.New("unexpected integer type")
	}
}
