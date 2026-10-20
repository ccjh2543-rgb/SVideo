package redis

import (
	"context"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func (c *Client) ZincrBy(ctx context.Context, key string, member string, score float64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZIncrBy(ctx, key, score, member).Err()
}

func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZAdd(ctx, key, members...).Err()
}

func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start int64, stop int64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZRemRangeByRank(ctx, key, start, stop).Err()
}

func (c *Client) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRangeWithScores(ctx, key, start, stop).Result()
}

func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Expire(ctx, key, ttl).Err()
}

func (c *Client) ZUnionStore(ctx context.Context, dst string, keys []string, aggregate string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZUnionStore(ctx, dst, &redis.ZStore{
		Keys:      keys,
		Aggregate: aggregate,
	}).Err()
}

// ZUnionStoreWeighted 按权重合并多个 ZSET，用于小时级热榜的线性衰减。
func (c *Client) ZUnionStoreWeighted(
	ctx context.Context,
	dst string,
	keys []string,
	weights []float64,
	aggregate string,
) error {
	if c == nil || c.rdb == nil || len(keys) == 0 {
		return nil
	}
	return c.rdb.ZUnionStore(ctx, dst, &redis.ZStore{
		Keys:      keys,
		Weights:   weights,
		Aggregate: aggregate,
	}).Err()
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	n, err := c.rdb.Exists(ctx, key).Result()
	return n > 0, err
}

func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRange(ctx, key, start, stop).Result()
}

func (c *Client) ZRevRangeByScore(ctx context.Context, key string, max, min string, offset, count int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: offset,
		Count:  count,
	}).Result()
}

// ZRevRangeByScoreWithScores 按分数倒序返回成员及分数。
func (c *Client) ZRevRangeByScoreWithScores(
	ctx context.Context,
	key string,
	max string,
	min string,
	offset int64,
	count int64,
) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: offset,
		Count:  count,
	}).Result()
}

// ZRevRangeByScoreWithScoresMulti 使用 Pipeline 批量读取多个 ZSET，用于合并多个大 V 的发件箱。
func (c *Client) ZRevRangeByScoreWithScoresMulti(
	ctx context.Context,
	keys []string,
	max string,
	min string,
	count int64,
) ([][]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	if len(keys) == 0 {
		return [][]redis.Z{}, nil
	}

	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.ZSliceCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.ZRevRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
			Max:   max,
			Min:   min,
			Count: count,
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	results := make([][]redis.Z, len(keys))
	for i, cmd := range cmds {
		values, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		results[i] = values
	}
	return results, nil
}

// ZRem 删除 ZSET 中的指定成员。
func (c *Client) ZRem(ctx context.Context, key string, members ...string) error {
	if c == nil || c.rdb == nil || len(members) == 0 {
		return nil
	}
	return c.rdb.ZRem(ctx, key, members).Err()
}

// ZAddManyWithTrim 批量向多个 ZSET 写入同一个成员，并裁剪、设置 TTL。
// 用于小 V 发布视频时向粉丝 Inbox 写扩散。
func (c *Client) ZAddManyWithTrim(
	ctx context.Context,
	entries map[string][]redis.Z,
	trimStart int64,
	trimStop int64,
	ttl time.Duration,
) error {
	if c == nil || c.rdb == nil || len(entries) == 0 {
		return nil
	}

	pipe := c.rdb.Pipeline()
	for key, members := range entries {
		if len(members) == 0 {
			continue
		}
		pipe.ZAdd(ctx, key, members...)
		pipe.ZRemRangeByRank(ctx, key, trimStart, trimStop)
		if ttl > 0 {
			pipe.Expire(ctx, key, ttl)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}
