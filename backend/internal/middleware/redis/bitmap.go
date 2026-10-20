package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// SetBits 批量设置 Bitmap 位，用于布隆过滤器写入。
func (c *Client) SetBits(ctx context.Context, key string, offsets []uint64) error {
	if c == nil || c.rdb == nil || len(offsets) == 0 {
		return nil
	}
	pipe := c.rdb.Pipeline()
	for _, offset := range offsets {
		pipe.SetBit(ctx, key, int64(offset), 1)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// GetBits 批量读取 Bitmap 位，用于布隆过滤器查询。
func (c *Client) GetBits(ctx context.Context, key string, offsets []uint64) ([]bool, error) {
	if c == nil || c.rdb == nil || len(offsets) == 0 {
		return []bool{}, nil
	}
	pipe := c.rdb.Pipeline()
	cmds := make([]*goredis.IntCmd, len(offsets))
	for i, offset := range offsets {
		cmds[i] = pipe.GetBit(ctx, key, int64(offset))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	result := make([]bool, len(cmds))
	for i, cmd := range cmds {
		value, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		result[i] = value == 1
	}
	return result, nil
}
