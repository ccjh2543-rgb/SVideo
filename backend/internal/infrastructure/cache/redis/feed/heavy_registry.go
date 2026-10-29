package feed

import (
	"context"
	"strconv"
	"time"

	domainsocial "feedsystem_video_go/internal/domain/social"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

const (
	// HeavyEnterThreshold 兼容旧引用，实际阈值定义在 domain/social。
	HeavyEnterThreshold = domainsocial.HeavyEnterThreshold
	// HeavyExitThreshold 兼容旧引用，实际阈值定义在 domain/social。
	HeavyExitThreshold = domainsocial.HeavyExitThreshold
	// heavyBackfillLockTTL 回填任务锁的过期时间，避免 Worker 异常后永久锁死。
	heavyBackfillLockTTL = 10 * time.Minute
)

// HeavyRegistry 重度用户与双向索引的 Redis 实现。
//
// feed:heavy:flag:{uid}            是否重度用户
// feed:heavy:bigv_count:{uid}      关注的大 V 数
// feed:heavy:following:{uid}       重度用户关注的大 V 集合
// feed:heavy_followers:{bigV}      关注该大 V 的重度用户集合
// feed:heavy:ver:{uid}             重度关系版本
// feed:heavy:backfill:{uid}        大 V 历史内容是否已回填完成
// feed:heavy:backfill:lock:{uid}   回填任务互斥锁
type HeavyRegistry struct {
	cache *rediscache.Client
}

// NewHeavyRegistry 创建重度用户注册表。
func NewHeavyRegistry(cache *rediscache.Client) *HeavyRegistry {
	return &HeavyRegistry{cache: cache}
}

// IsHeavy 判断用户是否重度用户。
func (r *HeavyRegistry) IsHeavy(ctx context.Context, uid uint) (bool, error) {
	if r == nil || r.cache == nil || uid == 0 {
		return false, nil
	}
	return r.cache.Exists(ctx, heavyFlagKey(uid))
}

// OnFollowBigV 记录“关注了一个大 V”。
// 返回 becameHeavy 表示本次操作让用户跨入重度阈值，isHeavy 表示操作后是否重度。
func (r *HeavyRegistry) OnFollowBigV(ctx context.Context, uid, bigV uint) (bool, bool, error) {
	if r == nil || r.cache == nil || uid == 0 || bigV == 0 {
		return false, false, nil
	}

	var count int64
	added, err := r.cache.SAdd(ctx, heavyFollowingKey(uid), strconv.FormatUint(uint64(bigV), 10))
	if err != nil {
		return false, false, err
	}
	if added == 0 {
		// 重复关注事件：不重复加计数，只保证反向索引存在。
		heavy, err := r.IsHeavy(ctx, uid)
		if err != nil {
			return false, false, err
		}
		if heavy {
			if _, err := r.cache.SAdd(ctx, heavyFollowersKey(strconv.FormatUint(uint64(bigV), 10)), strconv.FormatUint(uint64(uid), 10)); err != nil {
				return false, true, err
			}
		}
		return false, heavy, nil
	}
	count, err = r.cache.Incr(ctx, heavyCountKey(uid))
	if err != nil {
		return false, false, err
	}
	if _, err := r.cache.Incr(ctx, heavyVersionKey(uid)); err != nil {
		return false, false, err
	}

	heavy, err := r.IsHeavy(ctx, uid)
	if err != nil {
		return false, false, err
	}
	if count < HeavyEnterThreshold {
		return false, heavy, nil
	}

	if !heavy {
		created, err := r.cache.SetNX(ctx, heavyFlagKey(uid), []byte("1"), 0)
		if err != nil {
			return false, false, err
		}
		if created {
			// 成为重度用户时，把当前关注的所有大 V 都登记到反向索引。
			members, err := r.cache.SMembers(ctx, heavyFollowingKey(uid))
			if err != nil {
				_ = r.cache.Del(ctx, heavyFlagKey(uid))
				return false, false, err
			}
			for _, member := range members {
				if _, err := r.cache.SAdd(ctx, heavyFollowersKey(member), strconv.FormatUint(uint64(uid), 10)); err != nil {
					_ = r.cache.Del(ctx, heavyFlagKey(uid))
					return false, false, err
				}
			}
			return true, true, nil
		}
		heavy = true
	}

	// 已经是重度用户：只登记当前关注的大 V。
	if _, err := r.cache.SAdd(ctx, heavyFollowersKey(strconv.FormatUint(uint64(bigV), 10)), strconv.FormatUint(uint64(uid), 10)); err != nil {
		return false, true, err
	}
	return false, true, nil
}

// OnUnfollowBigV 记录“取关了一个大 V”。
// 只有确实存在于反向索引的关系才减计数，避免重复事件把计数减乱。
func (r *HeavyRegistry) OnUnfollowBigV(ctx context.Context, uid, bigV uint) error {
	if r == nil || r.cache == nil || uid == 0 || bigV == 0 {
		return nil
	}
	bigVKey := heavyFollowersKey(strconv.FormatUint(uint64(bigV), 10))
	uidValue := strconv.FormatUint(uint64(uid), 10)

	// 以正向索引作为计数依据：只有真正删除成功才递减，重复取关不会把计数减乱。
	removedFollowing, err := r.cache.SRem(ctx, heavyFollowingKey(uid), strconv.FormatUint(uint64(bigV), 10))
	if err != nil {
		return err
	}
	if _, err := r.cache.SRem(ctx, bigVKey, uidValue); err != nil {
		return err
	}
	if removedFollowing == 0 {
		return nil
	}

	count, err := r.cache.Decr(ctx, heavyCountKey(uid))
	if err != nil {
		return err
	}
	if count < 0 {
		count = 0
		_ = r.cache.SetBytes(ctx, heavyCountKey(uid), []byte("0"), 0)
	}
	if _, err := r.cache.Incr(ctx, heavyVersionKey(uid)); err != nil {
		return err
	}
	if count >= HeavyExitThreshold {
		return nil
	}

	heavy, err := r.IsHeavy(ctx, uid)
	if err != nil || !heavy {
		return err
	}
	// 退出重度状态：按正向索引清理所有反向索引。
	members, err := r.cache.SMembers(ctx, heavyFollowingKey(uid))
	if err != nil {
		return err
	}
	for _, member := range members {
		if _, err := r.cache.SRem(ctx, heavyFollowersKey(member), uidValue); err != nil {
			return err
		}
	}
	if err := r.cache.Del(ctx, heavyFlagKey(uid)); err != nil {
		return err
	}
	_ = r.cache.Del(ctx, heavyBackfillKey(uid))
	_ = r.cache.Del(ctx, heavyBackfillLockKey(uid))
	return r.cache.Del(ctx, heavyFollowingKey(uid))
}

// EnsureHeavy 根据当前关注的大 V 列表懒重建重度索引。
// 用于已存在用户或历史数据没有重度标记的场景；只做一次，后续由关注/取关维护。
func (r *HeavyRegistry) EnsureHeavy(ctx context.Context, uid uint, bigVIDs []uint) (bool, error) {
	if r == nil || r.cache == nil || uid == 0 || int64(len(bigVIDs)) < domainsocial.HeavyEnterThreshold {
		return false, nil
	}
	heavy, err := r.IsHeavy(ctx, uid)
	if err != nil || heavy {
		return heavy, err
	}

	uidValue := strconv.FormatUint(uint64(uid), 10)
	if err := r.cache.SetBytes(ctx, heavyCountKey(uid), []byte(strconv.Itoa(len(bigVIDs))), 0); err != nil {
		return false, err
	}
	created, err := r.cache.SetNX(ctx, heavyFlagKey(uid), []byte("1"), 0)
	if err != nil {
		return false, err
	}
	if !created {
		return true, nil
	}

	members := make([]string, 0, len(bigVIDs))
	for _, bigV := range bigVIDs {
		if bigV == 0 {
			continue
		}
		members = append(members, strconv.FormatUint(uint64(bigV), 10))
	}
	if _, err := r.cache.SAdd(ctx, heavyFollowingKey(uid), members...); err != nil {
		_ = r.cache.Del(ctx, heavyFlagKey(uid))
		return false, err
	}
	for _, bigV := range members {
		if _, err := r.cache.SAdd(ctx, heavyFollowersKey(bigV), uidValue); err != nil {
			_ = r.cache.Del(ctx, heavyFlagKey(uid))
			return false, err
		}
	}
	return true, nil
}

// ListHeavyFollowing 返回重度用户关注的大 V ID，用于新晋回填。
func (r *HeavyRegistry) ListHeavyFollowing(ctx context.Context, uid uint) ([]uint, error) {
	if r == nil || r.cache == nil || uid == 0 {
		return nil, nil
	}
	members, err := r.cache.SMembers(ctx, heavyFollowingKey(uid))
	if err != nil {
		return nil, err
	}
	return parseHeavyIDs(members), nil
}

// IsBackfilled 判断重度用户的大 V 历史内容是否已经回填完成。
// 未完成时读路径不能跳过已登记大 V 的 Outbox，否则会漏历史内容。
func (r *HeavyRegistry) IsBackfilled(ctx context.Context, uid uint) (bool, error) {
	if r == nil || r.cache == nil || uid == 0 {
		return false, nil
	}
	return r.cache.Exists(ctx, heavyBackfillKey(uid))
}

// TryBeginBackfill 抢占一次回填任务。
// 抢到锁的实例负责回填，其他实例继续走安全读路径。
func (r *HeavyRegistry) TryBeginBackfill(ctx context.Context, uid uint) (bool, error) {
	if r == nil || r.cache == nil || uid == 0 {
		return false, nil
	}
	return r.cache.SetNX(ctx, heavyBackfillLockKey(uid), []byte("1"), heavyBackfillLockTTL)
}

// MarkBackfilled 标记回填完成，并释放回填锁。
func (r *HeavyRegistry) MarkBackfilled(ctx context.Context, uid uint) error {
	if r == nil || r.cache == nil || uid == 0 {
		return nil
	}
	if err := r.cache.SetBytes(ctx, heavyBackfillKey(uid), []byte("1"), 0); err != nil {
		return err
	}
	return r.cache.Del(ctx, heavyBackfillLockKey(uid))
}

// ScanHeavyFollowers 分批扫描大 V 的重度粉丝，避免 SMEMBERS 阻塞。
func (r *HeavyRegistry) ScanHeavyFollowers(ctx context.Context, bigV uint, cursor uint64, count int64) ([]uint, uint64, error) {
	if r == nil || r.cache == nil || bigV == 0 {
		return nil, 0, nil
	}
	members, next, err := r.cache.SScan(ctx, heavyFollowersKey(strconv.FormatUint(uint64(bigV), 10)), cursor, count)
	if err != nil {
		return nil, 0, err
	}
	return parseHeavyIDs(members), next, nil
}

// parseHeavyIDs 把字符串成员转换为用户/大 V ID。
func parseHeavyIDs(members []string) []uint {
	ids := make([]uint, 0, len(members))
	for _, member := range members {
		value, err := strconv.ParseUint(member, 10, 64)
		if err != nil || value == 0 {
			continue
		}
		ids = append(ids, uint(value))
	}
	return ids
}

func heavyFlagKey(uid uint) string {
	return "feed:heavy:flag:" + strconv.FormatUint(uint64(uid), 10)
}

func heavyCountKey(uid uint) string {
	return "feed:heavy:bigv_count:" + strconv.FormatUint(uint64(uid), 10)
}

func heavyFollowingKey(uid uint) string {
	return "feed:heavy:following:" + strconv.FormatUint(uint64(uid), 10)
}

func heavyFollowersKey(bigV string) string {
	return "feed:heavy_followers:" + bigV
}

func heavyVersionKey(uid uint) string {
	return "feed:heavy:ver:" + strconv.FormatUint(uint64(uid), 10)
}

func heavyBackfillKey(uid uint) string {
	return "feed:heavy:backfill:" + strconv.FormatUint(uint64(uid), 10)
}

func heavyBackfillLockKey(uid uint) string {
	return "feed:heavy:backfill:lock:" + strconv.FormatUint(uint64(uid), 10)
}
