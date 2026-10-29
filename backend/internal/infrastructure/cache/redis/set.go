package redis

import (
	"context"
)

// Incr 原子自增整数。
func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, nil
	}
	return c.rdb.Incr(ctx, key).Result()
}

// Decr 原子自减整数。
func (c *Client) Decr(ctx context.Context, key string) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, nil
	}
	return c.rdb.Decr(ctx, key).Result()
}

// SAdd 向集合添加成员。
func (c *Client) SAdd(ctx context.Context, key string, members ...string) (int64, error) {
	if c == nil || c.rdb == nil || len(members) == 0 {
		return 0, nil
	}
	return c.rdb.SAdd(ctx, key, members).Result()
}

// SRem 从集合删除成员。
func (c *Client) SRem(ctx context.Context, key string, members ...string) (int64, error) {
	if c == nil || c.rdb == nil || len(members) == 0 {
		return 0, nil
	}
	return c.rdb.SRem(ctx, key, members).Result()
}

// SIsMember 判断成员是否在集合中。
func (c *Client) SIsMember(ctx context.Context, key string, member string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	return c.rdb.SIsMember(ctx, key, member).Result()
}

// SMembers 返回集合全部成员，仅用于成员数量可控的场景。
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.SMembers(ctx, key).Result()
}

// SScan 使用游标分批扫描集合，避免大集合阻塞 Redis。
func (c *Client) SScan(ctx context.Context, key string, cursor uint64, count int64) ([]string, uint64, error) {
	if c == nil || c.rdb == nil {
		return nil, 0, nil
	}
	return c.rdb.SScan(ctx, key, cursor, "", count).Result()
}
