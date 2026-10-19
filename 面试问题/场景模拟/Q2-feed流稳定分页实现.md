# 面试问答记录 - 第2轮

## 问题
Feed 流的稳定分页是如何实现的？

## 回答

我们这个项目**全面采用游标分页（Cursor-based Pagination）**，完全抛弃了传统的 `LIMIT offset, limit` 方案。原因很明显：`OFFSET 10000` 意味着数据库要先扫 10000 条再扔掉，越往后翻越慢，而游标分页直接走索引 `WHERE create_time < ?`，是 O(logN) 级别，翻页性能不衰减。

具体来说，四种 Feed 流分别采用了不同的游标策略：

---

### 1. ListLatest（最新视频流）—— 单字段时间游标

以上一页最后一条视频的 `create_time` 作为游标：

```go
// Repository 层核心逻辑
query := repo.db.WithContext(ctx).Model(&video.Video{}).
    Order("create_time DESC")

if !latestBefore.IsZero() {
    query = query.Where("create_time < ?", latestBefore)
}
query.Limit(limit).Find(&videos)
```

第一页不传游标，直接查最新 N 条。每页返回 `next_time`（最后一条的发布时间毫秒时间戳），下一页带上这个值即可。

**稳定性分析**：单字段时间游标的稳定性是"弱保证"——同一毫秒如果有多个视频发布，理论上可能有遗漏。但在大多数场景下可以接受。如果再严格一点，可以把 ID 也加进去做成双字段。

---

### 2. ListLikesCount（按点赞数排序）—— 双字段游标（点赞数 + ID）

这是最典型的"需要双字段"的场景，因为**大量视频点赞数相同**。只用 `likes_count` 做游标就会出问题：翻到下一页时，同分的视频可能会重复出现或被跳过。

核心 SQL：

```go
query = query.Where(
    "(likes_count < ?) OR (likes_count = ? AND id < ?)",
    cursor.LikesCount,
    cursor.LikesCount, cursor.ID,
)
```

排序规则是 `likes_count DESC, id DESC`。翻译一下就是：**先找点赞数比我少的，如果点赞数相同，就找 ID 比我小的**。因为 ID 是数据库自增主键，全局唯一，这样就保证了绝对不重复不遗漏。

---

### 3. ListByPopularity（热度推荐流）—— 三字段游标 + Redis ZSET 双路径

这个最复杂，支持两条路径：

**正常路径（Redis ZSET）**：
- 每分钟将 60 个分钟级热度窗口合并成一个快照 ZSET（存 Video ID -> 热度分数）
- 直接用 `ZRevRange` 按 offset 分页，快照内翻页是稳定的
- 请求参数中的 `as_of` 字段表示快照版本号

**降级路径（MySQL 三字段游标）**：
- 当 Redis 不可用时，自动降级到 MySQL
- 三字段游标：`(popularity < ?) OR (popularity = ? AND create_time < ?) OR (popularity = ? AND create_time = ? AND id < ?)`
- 排序规则：`popularity DESC, create_time DESC, id DESC`
- 热度相同比时间，时间相同比 ID，**理论上 100% 不重复不遗漏**

---

### 4. ListByFollowing（关注流）—— 时间游标 + 子查询

翻页逻辑和最新流一样，基于时间游标。区别在于多了关注关系的过滤：

```go
followingSubQuery := repo.db.WithContext(ctx).
    Model(&social.Social{}).
    Select("vlogger_id").
    Where("follower_id = ?", viewerAccountID)

query = query.Where("author_id IN (?)", followingSubQuery)
```

这里用**子查询替代 JOIN**，在大数据量下性能更好。

---

### 总结：游标字段数的选择原则

| Feed 类型 | 游标字段数 | 游标字段 | 排序规则 |
|---|---|---|---|
| ListLatest | 1 字段 | `create_time` | `create_time DESC` |
| ListLikesCount | 2 字段 | `likes_count` + `id` | `likes_count DESC, id DESC` |
| ListByFollowing | 1 字段 | `create_time` | `create_time DESC` |
| ListByPopularity(DB) | 3 字段 | `popularity` + `create_time` + `id` | `popularity DESC, create_time DESC, id DESC` |

核心原则是：**排序字段的区分度决定游标字段数**。如果排序字段可能重复，就加 ID 做二级兜底。区分度越差，需要的兜底字段越多。
