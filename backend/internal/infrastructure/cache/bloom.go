package cache

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync/atomic"

	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

const (
	defaultBloomBits    uint64 = 1 << 24 // 约 2MB，可容纳百万级视频
	defaultBloomHashNum        = 7
)

// BloomFilter 基于 Redis Bitmap 的布隆过滤器。
// 用于拦截不存在的视频 ID，降低缓存穿透压力。
type BloomFilter struct {
	client    *rediscache.Client
	key       string
	bits      uint64
	hashCount int
	enabled   atomic.Bool
}

// NewBloomFilter 创建布隆过滤器。
func NewBloomFilter(client *rediscache.Client, key string, bits uint64, hashCount int) *BloomFilter {
	if bits == 0 {
		bits = defaultBloomBits
	}
	if hashCount <= 0 {
		hashCount = defaultBloomHashNum
	}
	return &BloomFilter{
		client:    client,
		key:       key,
		bits:      bits,
		hashCount: hashCount,
	}
}

// Rebuild 从数据库加载全部视频 ID 重建布隆过滤器。
func (b *BloomFilter) Rebuild(ctx context.Context, load func(afterID uint, limit int) ([]uint, error)) error {
	if b == nil || b.client == nil || load == nil {
		return nil
	}
	const batchSize = 1000
	var afterID uint
	for {
		ids, err := load(afterID, batchSize)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		for _, id := range ids {
			if err := b.Add(ctx, uint64(id)); err != nil {
				return err
			}
		}
		afterID = ids[len(ids)-1]
		if len(ids) < batchSize {
			break
		}
	}
	b.enabled.Store(true)
	return nil
}

// Add 把视频 ID 加入布隆过滤器。
func (b *BloomFilter) Add(ctx context.Context, id uint64) error {
	if b == nil || b.client == nil || id == 0 {
		return nil
	}
	return b.client.SetBits(ctx, b.key, b.positions(id))
}

// Exists 判断视频 ID 是否可能存在。
// 布隆过滤器未启用或 Redis 出错时返回 true（fail-open），避免误杀正常请求。
func (b *BloomFilter) Exists(ctx context.Context, id uint64) (bool, error) {
	if b == nil || b.client == nil || !b.enabled.Load() || id == 0 {
		return true, nil
	}
	values, err := b.client.GetBits(ctx, b.key, b.positions(id))
	if err != nil {
		return true, err
	}
	for _, value := range values {
		if !value {
			return false, nil
		}
	}
	return true, nil
}

// positions 计算一次插入/查询需要访问的 bitmap 下标。
func (b *BloomFilter) positions(id uint64) []uint64 {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], id)
	h1 := hash64(buf[:], 0)
	h2 := hash64(buf[:], 0x9e3779b97f4a7c15)

	positions := make([]uint64, b.hashCount)
	for i := 0; i < b.hashCount; i++ {
		positions[i] = (h1 + uint64(i)*h2) % b.bits
	}
	return positions
}

func hash64(data []byte, seed uint64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(data)
	return h.Sum64() ^ seed
}
