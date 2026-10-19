// Package feed Feed流数据访问层
//
// 【SQL查询设计要点】
// 1. 全部使用游标分页，不用 OFFSET（翻到1000页也不慢）
// 2. 使用子查询代替JOIN，性能更好
// 3. 多字段排序时，必须用多字段游标防止分页重复/遗漏
package feed

import (
	"context"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"time"

	"gorm.io/gorm"
)

// FeedRepository Feed流数据库查询层
// 数据流：Service 传查询条件 → Repository 组装SQL → MySQL 返回数据
type FeedRepository struct {
	db *gorm.DB
}

// NewFeedRepository 创建Feed流仓库
func NewFeedRepository(db *gorm.DB) *FeedRepository {
	return &FeedRepository{db: db}
}

// -----------------------------------------------------------------------------
// 【最新视频流查询】 时间游标分页
// 为什么不用 LIMIT offset, limit?
//   - offset 10000 意味着数据库要先扫10000条再扔掉，巨慢！
//   - 游标分页：WHERE create_time < ? 走索引，O(logN) 性能
// -----------------------------------------------------------------------------

// ListLatest
// 功能：按【发布时间倒序】查询视频列表，使用【时间游标分页】（高性能分页方案）
// 核心原理：
// 1. 业务游标：以上一页最后一条数据的 create_time 作为分页标记（叫"时间游标"）
// 2. 数据库优化：依赖 create_time 索引，避免全表扫描，查询极快
// 3. 对比传统分页：比 LIMIT offset 快无数倍，适合短视频Feed流/无限滚动场景
//
// 参数说明：
// ctx      : 上下文，用于链路追踪、超时控制
// limit    : 每页要查询的视频条数（比如每页10条）
// latestBefore : 上一页最后一条视频的【发布时间】（时间游标）
//
//	第一页时：该值为零值（空时间）
//	非第一页：传入上一页最后一条的create_time
//
// 返回值：视频列表指针切片、查询错误
func (repo *FeedRepository) ListLatest(ctx context.Context, limit int, latestBefore time.Time) ([]*video.Video, error) {
	// 定义视频切片，用于接收数据库查询结果
	var videos []*video.Video

	// ===================== 第一步：构建基础查询语句 =====================
	// GORM查询构造器
	// 等价SQL基础部分：SELECT * FROM video ORDER BY create_time DESC
	// 作用：指定查询video表，按【创建时间倒序】排序（最新发布的排在最前面）
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("create_time DESC")

	// ===================== 第二步：处理时间游标（核心分页逻辑） =====================
	// 判断：如果 latestBefore 不是零值 → 代表不是第一页，需要分页查询
	// 第一页：不传时间，直接查最新的N条
	// 非第一页：只查询【发布时间 早于 上一页最后一条时间】的视频
	if !latestBefore.IsZero() {
		// 等价SQL：WHERE create_time < '上一页最后一条的发布时间'
		// 这就是【业务游标】：用时间作为游标，定位下一页数据
		query = query.Where("create_time < ?", latestBefore)
	}

	// ===================== 第三步：执行最终查询 =====================
	// 1. Limit(limit)：限制查询条数（比如10条）
	// 2. Find(&videos)：执行查询，将结果写入videos切片
	// 3. Error：获取查询过程中的异常（如数据库连接失败、表不存在等）
	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		// 查询失败，返回nil和错误信息
		return nil, err
	}

	// 查询成功，返回视频列表
	return videos, nil
}

// -----------------------------------------------------------------------------
// 【点赞数排行查询】 双字段游标分页
// 为什么要双字段？
//   - 很多视频点赞数相同！只用likes_count分页会重复/漏掉
// -----------------------------------------------------------------------------

// ListLikesCountWithCursor 按点赞数倒序查视频
// 设计亮点：双字段游标解决同分问题
// 数据流：传(上一页最后一条的点赞数, 视频ID) → 精确查询不重复不遗漏
func (repo *FeedRepository) ListLikesCountWithCursor(ctx context.Context, limit int, cursor *LikesCountCursor) ([]*video.Video, error) {
	var videos []*video.Video

	// 排序规则：先按点赞数降序，点赞数相同就按ID降序
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("likes_count DESC, id DESC")

	if cursor != nil {
		// 核心SQL逻辑（这行值500块！）
		// WHERE likes_count < 10000                   点赞数比我少的
		//    OR (likes_count = 10000 AND id < 5678)    点赞数相同，但ID比我小的
		// 完美解决同分问题！
		query = query.Where(
			"(likes_count < ?) OR (likes_count = ? AND id < ?)",
			cursor.LikesCount,
			cursor.LikesCount, cursor.ID,
		)
	}

	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

// -----------------------------------------------------------------------------
// 【关注流查询】 子查询IN + 时间游标
// 为什么用子查询不用JOIN？
//   - JOIN两个大表 = 笛卡尔积爆炸
//   - 子查询 = 先查我关注了哪100个人，再查他们的视频，性能快10倍！
// -----------------------------------------------------------------------------

// ListByFollowing 查询我关注的博主发布的视频
// 设计亮点：子查询代替JOIN，性能提升一个数量级
func (repo *FeedRepository) ListByFollowing(ctx context.Context, limit int, viewerAccountID uint, latestBefore time.Time) ([]*video.Video, error) {
	var videos []*video.Video

	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("create_time DESC")

	if viewerAccountID > 0 {
		// 子查询性能优化：
		// 第一步：先查出我关注的所有博主ID（一般也就几百个）
		// 第二步：查这些博主的视频
		// 比 JOIN + GROUP BY 快10倍以上！
		followingSubQuery := repo.db.WithContext(ctx).
			Model(&social.Social{}).
			Select("vlogger_id").
			Where("follower_id = ?", viewerAccountID)

		query = query.Where("author_id IN (?)", followingSubQuery)
	}

	if !latestBefore.IsZero() {
		query = query.Where("create_time < ?", latestBefore)
	}

	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

// -----------------------------------------------------------------------------
// 【热度排行查询】 三字段游标分页
// 热度分相同、发布时间也相同，就用ID兜底
// -----------------------------------------------------------------------------

// ListByPopularity 按算法热度倒序查询（推荐流降级方案）
// 设计：三字段游标保证海量数据下分页绝对准确
func (repo *FeedRepository) ListByPopularity(ctx context.Context, limit int, popularityBefore int64, timeBefore time.Time, idBefore uint) ([]*video.Video, error) {
	var videos []*video.Video

	// 三级排序规则：热度 > 发布时间 > ID
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("popularity DESC, create_time DESC, id DESC")

	// Redis挂了才会走到这里，这就是降级方案的SQL
	if !timeBefore.IsZero() && idBefore > 0 {
		// 三层过滤，绝对保证分页不重复不遗漏！
		query = query.Where(
			"(popularity < ?) OR (popularity = ? AND create_time < ?) OR (popularity = ? AND create_time = ? AND id < ?)",
			popularityBefore,
			popularityBefore, timeBefore,
			popularityBefore, timeBefore, idBefore,
		)
	}

	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

// -----------------------------------------------------------------------------
// 【批量查询】 批量获取视频详情
// 这是三级缓存的最后一道防线
// -----------------------------------------------------------------------------

// GetByIDs 批量根据视频ID查详情
// 数据流：Service传ID数组 → MySQL IN查询 → 返回给Service回填缓存
func (repo *FeedRepository) GetByIDs(ctx context.Context, ids []uint) ([]*video.Video, error) {
	var videos []*video.Video
	if len(ids) == 0 {
		return videos, nil
	}
	if err := repo.db.WithContext(ctx).Model(&video.Video{}).
		Where("id IN ?", ids).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}
