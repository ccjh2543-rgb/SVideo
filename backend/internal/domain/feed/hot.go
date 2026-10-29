package feed

import (
	"fmt"
	"math"
	"time"
)

const (
	// HotWindowHours 热榜窗口长度，按小时桶计算。
	HotWindowHours = 24
	// HotShardCount 小时桶的写入分片数，用于分散 Redis 写热点。
	HotShardCount = 16
	// HotBucketTopN 每个逻辑小时桶保留的候选数量。
	HotBucketTopN int64 = 50000
	// HotCandidateTTL 候选桶存活时间，需要覆盖当前小时和上一小时的构建周期。
	HotCandidateTTL = 3 * time.Hour
	// HotTopTTL Top 桶存活时间，需要覆盖 24 小时窗口与历史 as_of 回看。
	HotTopTTL = 48 * time.Hour
	// HotSnapshotTTL 热榜合并快照的存活时间。
	HotSnapshotTTL = 10 * time.Minute
	// DefaultHotHalfLife 默认热度半衰期，6 小时兼顾实时性与稳定性。
	DefaultHotHalfLife = 6 * time.Hour
)

// HotUpdate 一条热度变更值对象。
// OccurredAt 使用事件发生时间，而不是消费时间；Weight 为业务权重，0 表示使用 1。
type HotUpdate struct {
	VideoID    uint
	Change     int64
	OccurredAt time.Time
	Weight     float64
}

// HotBucketStart 返回事件时间所属的小时桶起点，统一使用 UTC。
func HotBucketStart(eventTime time.Time) time.Time {
	return eventTime.UTC().Truncate(time.Hour)
}

// HotDecayLambda 根据半衰期计算指数衰减系数。
func HotDecayLambda(halfLife time.Duration) float64 {
	if halfLife <= 0 {
		halfLife = DefaultHotHalfLife
	}
	return math.Ln2 / halfLife.Seconds()
}

// HotIncrement 计算事件写入桶时的分数增量。
//
// 桶内存储的是相对桶起点的指数分：
//
//	weight * exp(lambda * (eventTime - bucketStart))
//
// weight 允许为负，用于取消点赞；weight=0 时按 1 处理。
// 合并时再乘桶权重 exp(-lambda * (asOf - bucketStart))，
// 两者相乘正好等于完整的指数衰减分数。
func HotIncrement(eventTime time.Time, weight float64, halfLife time.Duration) float64 {
	if eventTime.IsZero() {
		eventTime = time.Now()
	}
	if weight == 0 {
		weight = 1
	}
	bucketStart := HotBucketStart(eventTime)
	relative := eventTime.Sub(bucketStart).Seconds()
	if relative < 0 {
		relative = 0
	}
	return weight * math.Exp(HotDecayLambda(halfLife)*relative)
}

// HotBucketWeight 计算查询时刻 asOf 下某个小时桶的指数权重。
func HotBucketWeight(asOf, bucketStart time.Time, halfLife time.Duration) float64 {
	age := asOf.Sub(bucketStart).Seconds()
	if age < 0 {
		age = 0
	}
	return math.Exp(-HotDecayLambda(halfLife) * age)
}

// HotShard 返回视频 ID 对应的写入分片。
func HotShard(videoID uint) int {
	return int(videoID % uint(HotShardCount))
}

// HotCandidateKey 返回小时候选桶的 Redis key。
func HotCandidateKey(bucket time.Time, shard int) string {
	return fmt.Sprintf("hot:candidate:1h:%s:%d", HotBucketStart(bucket).Format("2006010215"), shard)
}

// HotTopKey 返回小时 Top 桶的 Redis key。
func HotTopKey(bucket time.Time) string {
	return "hot:top:1h:" + HotBucketStart(bucket).Format("2006010215")
}

// HotSnapshotKey 返回 as_of 对应的热榜合并快照 key。
func HotSnapshotKey(asOf time.Time) string {
	return "hot:video:merge:1h:" + HotBucketStart(asOf).Format("2006010215")
}

// HotCandidateKeys 返回某个小时全部候选分片的 key。
func HotCandidateKeys(bucket time.Time, shards int) []string {
	if shards <= 0 {
		shards = HotShardCount
	}
	keys := make([]string, 0, shards)
	for shard := 0; shard < shards; shard++ {
		keys = append(keys, HotCandidateKey(bucket, shard))
	}
	return keys
}

// HotTopWindowKeys 返回从 asOf 往前共 hours 个小时的 Top 桶 key，顺序与权重一致。
func HotTopWindowKeys(asOf time.Time, hours int) []string {
	if hours <= 0 {
		return nil
	}
	asOf = HotBucketStart(asOf)
	keys := make([]string, 0, hours)
	for i := 0; i < hours; i++ {
		keys = append(keys, HotTopKey(asOf.Add(-time.Duration(i)*time.Hour)))
	}
	return keys
}

// HotDecayWeights 返回从 asOf 往前共 hours 个小时桶的指数权重。
// 最新小时权重为 1，之后按半衰期指数衰减。
func HotDecayWeights(asOf time.Time, hours int, halfLife time.Duration) []float64 {
	if hours <= 0 {
		return nil
	}
	asOf = HotBucketStart(asOf)
	lambda := HotDecayLambda(halfLife)
	weights := make([]float64, hours)
	for i := 0; i < hours; i++ {
		age := time.Duration(i) * time.Hour
		weights[i] = math.Exp(-lambda * age.Seconds())
	}
	return weights
}
