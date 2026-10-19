# 面试问题：Feed 流与缓存设计

> 面试官视角：Feed 流是短视频系统的核心，也是面试中**必问最深**的模块。以下问题从浅入深，考察你对缓存架构、分页设计、高并发防护的理解。

---

## 问题 1：介绍一下你的 Feed 流系统有哪些列表接口？分别是如何排序和分页的？

⭐ **难度**：基础  
**考察点**：项目整体理解、分页方案选型

### 问题
> "请介绍一下你项目的 Feed 流都提供了哪些列表接口，每个接口的排序规则和分页方案是什么？"

### 参考答案

项目提供了 **4 种 Feed 流**，对应抖音首页四个 Tab：

| Feed 流 | 排序依据 | 分页方案 | 对应抖音 |
|---------|---------|---------|---------|
| `listLatest` | `create_time` 倒序 | 时间游标 | "最新" Tab |
| `listLikesCount` | `likes_count` 倒序 | 双字段游标 (likes_count + id) | "热门榜" |
| `listByFollowing` | `create_time` 倒序 | 子查询 IN + 时间游标 | "关注" Tab |
| `listByPopularity` | `popularity` 倒序 | Redis ZSET offset + DB 三字段游标降级 | "推荐" Tab |

**核心设计决策：所有接口全部使用游标分页（Cursor Pagination），没有用传统的 LIMIT OFFSET。**

代码位置：[feed/entity.go](file:///f:/zhuomian/项目/feedsystem_video_go-main/backend/internal/feed/entity.go)

### 加分点
- 能主动说明为什么不用 OFFSET：OFFSET 10000 意味着数据库要先扫描 10000 条再丢弃，随着页码增大性能呈线性下降
- 游标分页利用 WHERE 条件走索引，翻到第 1000 页和第 1 页一样快

---

## 问题 2：为什么所有列表接口都用游标分页？游标分页和 OFFSET 分页的本质区别是什么？

⭐⭐ **难度**：进阶  
**考察点**：SQL 索引原理、分页性能优化

### 问题
> "你说你的项目全部使用游标分页，解释一下游标分页的原理，和传统 OFFSET 分页比有什么优缺点？"

### 参考答案

**OFFSET 分页的原理问题**：

```sql
-- OFFSET 分页：翻到第 1001 页
SELECT * FROM video ORDER BY create_time DESC LIMIT 10 OFFSET 10000;
```
数据库执行过程：
1. 从索引树根节点开始扫描，找到 10010 条记录
2. 丢掉前 10000 条，返回后 10 条
3. 随着 OFFSET 增大，扫描行数线性增长

**游标分页的原理**：
```sql
-- 游标分页：第一页
SELECT * FROM video ORDER BY create_time DESC LIMIT 10;

-- 第二页：传上一页最后一条的 create_time
SELECT * FROM video ORDER BY create_time DESC 
WHERE create_time < '2025-04-25 10:00:00'   -- ← 走索引，直接定位！
LIMIT 10;
```
数据库执行过程：
1. 通过 `create_time` 索引 B+Tree 直接定位到游标位置
2. 继续向后扫描 10 条
3. 扫描量恒定为 10 条，和总数据量无关

**优缺点对比**：

| 维度 | OFFSET | 游标 |
|------|--------|------|
| 性能 | O(N)，翻页越慢越慢 | O(logN)，稳定 |
| 随机跳页 | ✅ 支持跳转到第 N 页 | ❌ 只能顺序翻页 |
| 数据变动影响 | ✅ 插入新数据不影响 | ⚠️ 新数据插入可能导致"漏掉" |
| 实现复杂度 | 简单 | 需要传游标参数 |

**为什么短视频 Feed 流适合游标分页？**
- 用户不会跳转到第 100 页，只会一直往下刷
- 游标分页天然适合"无限滚动"场景
- 性能稳定，支撑无限下滑

代码位置：[feed/repo.go](file:///f:/zhuomian/项目/feedsystem_video_go-main/backend/internal/feed/repo.go#L38-L54)

### 加分点
- 能提到游标分页的「数据一致性问题」：如果有新数据插入，下一页第一条可能和上一页最后一条重复。项目通过 `latestBefore - 1ms` 的时间戳微调来防止，同时前端做 ID 去重作为兜底
- 能引出分页方案的更多细节，比如双字段游标的设计

---

## 问题 3：点赞数排序的"双字段游标"是怎么设计的？为什么需要两个字段？

⭐⭐ **难度**：进阶  
**考察点**：分页边界问题、同分排序

### 问题
> "我看你的点赞数排行接口用了双字段游标 `(likes_count, id)`，为什么不用单字段？单字段会出什么问题？"

### 参考答案

**单字段游标的问题（经典面试题）**：

假设排序规则：`ORDER BY likes_count DESC`

```
第1页： 100赞(video1), 100赞(video2), 100赞(video3), 50赞(video4)
                     ↑ 游标: likes_count=50
第2页： 50赞(video5), 50赞(video6), 30赞(video7) 
```

看起来没问题？但如果第 2 页查询时，有一条新视频 `video_new` 发布，也是 50 赞：

```
第2页（有新插入）：50赞(video4), 50赞(video_new), 50赞(video5)
                                                    ↑ 游标: likes_count=30
第3页（重新查）：30赞(video6), 30赞(video7)  
```

**video6 被遗漏了！** 因为它从第 2 页移到了第 1 页的游标之后。

**双字段游标的解决方式**：

```go
// 排序：先按点赞数降序，点赞数相同按 ID 降序
ORDER BY likes_count DESC, id DESC

// 游标条件
WHERE (likes_count < 100)                           -- 点赞数少的
   OR (likes_count = 100 AND id < 5678)             -- 点赞数相同，ID 更小的
```

这样即使有新的 50 赞视频插入，因为 ID 比游标大，不会被漏掉。

代码位置：[feed/repo.go](file:///f:/zhuomian/项目/feedsystem_video_go-main/backend/internal/feed/repo.go#L68-L88)

### 加分点
- 能进一步说明三字段游标（热度排行用了 popularity + create_time + id 三个字段）
- 能主动提及 ORDER BY 和 WHERE 条件的索引匹配规则，Explain 分析

---

## 问题 4：讲一下你的三级缓存架构，每一层分别是什么、TTL 多长、为什么这样设计？

⭐⭐ **难度**：进阶  
**考察点**：多级缓存设计、分层思想、性能优化

### 问题
> "你的项目实现了三级缓存来加速 Feed 流查询，详细讲一下每一层的设计，包括技术选型、TTL、优缺点。"

### 参考答案

**三级缓存架构图**：

```
请求 → L1 本地内存 (go-cache, 3s TTL)
                ↓ 未命中
        L2 Redis 分布式缓存 (1h TTL)
                ↓ 未命中 / Redis 挂了
        L3 MySQL 数据库 ← SingleFlight 防击穿
```

**L1 本地缓存**：
- 技术选型：`patrickmn/go-cache`（进程内内存缓存）
- TTL：3 秒
- 适用场景：同一个进程内短时间内重复请求同一个视频信息
- 优点：毫秒级响应，不涉及网络 IO
- 缺点：进程间不共享，每个进程各自缓存（多实例部署时有 3 秒的不一致窗口）

```go
// 代码位置：feed/service.go
localcache: cache.New(3*time.Second, 5*time.Second)
```

**L2 Redis 分布式缓存**：
- 数据格式：视频对象 JSON 序列化
- TTL：1 小时
- 查询方式：`MGet` 批量查询（一次网络 IO 查多个 key）
- 超时保护：50ms 超时，超时自动降级到 L3
- 回填策略：L3 查到数据后，异步 goroutine 回写 Redis

```go
// 50ms 超时保护，Redis 挂了不拖垮服务
cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
results, err := f.rediscache.MGet(cacheCtx, cacheKeys...)
```

**L3 MySQL 数据库**：
- 最后一道防线
- 配合 SingleFlight 防止缓存击穿

**为什么这么设计**：
1. L1 解决"同一个请求多次查同一条数据"的问题（比如一个视频出现在 Feed 流 + 详情页）
2. L2 解决"不同进程查同一条数据"的问题，减少数据库压力
3. 从 L3 查到数据后，**异步回填 L2 和 L1**，形成完整的缓存闭环

代码位置：[feed/service.go](file:///f:/zhuomian/项目/feedsystem_video_go-main/backend/internal/feed/service.go#L52-L62)

### 加分点
- 主动说明缓存一致性策略：Feed 流场景对一致性要求不高（最终一致即可），所以没有用复杂的缓存更新协议
- 缓存雪崩预防：每个 key 的 TTL 加上随机偏移量，防止大量 key 同时过期
- 缓存穿透防护：即使数据库中不存在的数据也在 L2 缓存一个空值（短 TTL）

---

## 问题 5：SingleFlight 是怎么实现防缓存击穿的？能画一下 1000 个并发请求的时序图吗？

⭐⭐⭐ **难度**：深度  
**考察点**：并发控制、缓存击穿、Go 标准库

### 问题
> "项目中用了 `golang.org/x/sync/singleflight` 来防止缓存击穿，你能详细讲一下它的工作原理吗？如果 1000 个请求同时查一个没有缓存的视频，发生了什么？"

### 参考答案

**缓存击穿**是指：一个热点 key 在缓存失效的瞬间，大量并发请求同时涌入，全部穿透到数据库。

**SingleFlight 工作原理**：

```go
import "golang.org/x/sync/singleflight"

var requestGroup singleflight.Group

// 1000 个 goroutine 同时执行这段代码
func getVideo(videoID uint) (*Video, error) {
    sfKey := fmt.Sprintf("sf:entity:%d", videoID)
    
    v, err, shared := requestGroup.Do(sfKey, func() (interface{}, error) {
        // 【重点】这个函数 body 最多只有 1 个 goroutine 能执行
        // 其余 999 个 goroutine 会阻塞等待这个结果
        return f.repo.GetByIDs(ctx, []uint{videoID})
    })
    
    // shared == true 表示这个结果是和其他 goroutine 共享的
}
```

**1000 个并发请求的时序图**：

```
时间 →
────────────────────────────────────────────────────────
goroutine-1: Do("video:100") → 执行 fn → 查数据库开始...
goroutine-2: Do("video:100") → 等待结果...
goroutine-3: Do("video:100") → 等待结果...
...
goroutine-1000: Do("video:100") → 等待结果...
                                    ↓
                                    数据库查询完成 (10ms)
                                    ↓
goroutine-1: 返回结果 ✅
goroutine-2: 共享结果 ✅ (shared=true)
goroutine-3: 共享结果 ✅
...
goroutine-1000: 共享结果 ✅ (shared=true)
```

**数据库查询次数：1000 次 → 1 次** 🎉

**项目中 SingleFlight 的使用场景**：
1. 视频详情查询（`GetVideoByIDs` 的 L3 层）
2. Feed 流冷数据降级（`ListLatest` 的冷热边界）
3. ZSET 自动重建（`ListLatest` 的 `fallback:global_timeline_rebuild`）

代码位置：[feed/service.go](file:///f:/zhuomian/项目/feedsystem_video_go-main/backend/internal/feed/service.go#L148-L183)

### 加分点
- 能指出 SingleFlight 的潜在问题：如果那个唯一的请求超时或出错，所有 999 个等待的 goroutine 都会收到错误。项目通过回填缓存 + 重试机制缓解
- 能对比分布式锁和 SingleFlight 的区别：分布式锁跨进程，SingleFlight 只对单进程内的并发有效。项目在 Worker 启动时每个进程独立
- 能提到项目中给每个 SF key 加了 timeout 兜底，防止单个请求卡死导致所有 goroutine 阻塞

---

## 问题 6：冷热分离是怎么设计的？Redis ZSET 的水印线是如何工作的？

⭐⭐⭐ **难度**：深度  
**考察点**：大数据量下的缓存策略、冷热数据识别

### 问题
> "你的最新视频流实现了冷热分离，用 Redis ZSET 存热点数据，水印线以下是冷数据。说下为什么这样设计，ZSET 空了怎么办？"

### 参考答案

**设计动机**：
- Redis ZSET 的 `ZRevRangeByScore` 是 O(logN + M) 操作，性能极好
- 但 ZSET 不能无限大，否则占用过多内存，ZADD/ZREM 也会变慢
- 所以只缓存最新 1000 条视频 ID，足够覆盖 99% 的用户滑动请求

**水印线（Watermark）机制**：

```
Redis ZSET: feed:global_timeline
────────────────────────────────────────────
Score(时间戳)          Member(视频ID)
1735000000000         video_1001     ← 最新
1734999999000         video_1000
...
1734500000000         video_1       ← 最老（水印线=这个Score）
────────────────────────────────────────────
         ↑
    用户游标在此以上 → 查 Redis（热数据）
    用户游标在此以下 → 自动降级 MySQL（冷数据）
```

**关键代码逻辑**：
```go
// 1. 获取 ZSET 最老的一条（水印线）
zsetTail, _ := f.rediscache.ZRangeWithScores(ctx, "feed:global_timeline", 0, 0)
watermark := int64(zsetTail[0].Score)

// 2. 判断用户请求在热区还是冷区
if reqTime <= watermark {
    // 冷数据：降级查 MySQL（翻到很旧的数据了）
    baseVideos = f.repo.ListLatest(ctx, limit, latestBefore)
} else {
    // 热数据：查 Redis ZSET
    videoIDs, _ := f.rediscache.ZRevRangeByScore(ctx, "feed:global_timeline", maxScore, "-inf", 0, limit)
    // 再用三级缓存查视频详情
    baseVideos = f.GetVideoByIDs(ctx, videoIDs)
}
```

**ZSET 自动重建机制（ZSET 为空时）**：
1. 检测到 ZSET 为空，用 SingleFlight 锁住（防止多个请求同时重建）
2. 从 MySQL 捞最新的 1000 条视频
3. 批量 ZADD 写入 ZSET
4. 所有在等待的请求递归调用自身，重新走热数据路径

```go
// 全局静态锁，防止多个 goroutine 同时重建
sfKey := "sf:fallback:global_timeline_rebuild"
v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
    // 从 MySQL 捞最新的 1000 条
    dbVideos, _ := f.repo.ListLatest(ctx, 1000, time.Time{})
    // 重建 ZSET
    var zElements []redis.Z
    for _, vid := range dbVideos {
        zElements = append(zElements, redis.Z{
            Score:  float64(vid.CreateTime.UnixMilli()),
            Member: fmt.Sprintf("%d", vid.ID),
        })
    }
    f.rediscache.ZAdd(ctx, "feed:global_timeline", zElements...)
    return "SUCCESS"
})
```

代码位置：[feed/service.go](file:///f:/zhuomian/项目/feedsystem_video_go-main/backend/internal/feed/service.go#L206-L280)

### 加分点
- 能说明为什么 ZSET 只存 ID 不存完整数据：ZSET 只做排序索引，视频详情走三级缓存，职责分离
- ZSET 裁剪：新视频发布时 ZADD，超过 1000 条时 ZREMRANGEBYRANK 裁剪，保持 ZSET 大小恒定
- Outbox Poller 负责把新视频写入 ZSET，Worker 进程消费 timeline 消息后执行 ZADD + ZREMRANGE

---

## 问题 7：热度推荐（热榜）的滑动窗口是怎么实现的？

⭐⭐⭐ **难度**：深度  
**考察点**：滑动窗口算法、Redis ZSET 高级用法、时间窗口设计

### 问题
> "你的热度推荐用了一种基于 Redis 的滑动窗口方案，能详细讲一下热度分是怎么算的？分钟级 ZSET + ZUnionStore 是怎么工作的？"

### 参考答案

**滑动窗口设计**：

```
每 1 分钟一个 ZSET
────────────────────────────────────────────
key: hot:video:1m:202504251420    ← 14:20 这一分钟的点赞热度
key: hot:video:1m:202504251419    ← 14:19
key: hot:video:1m:202504251418    ← 14:18
...
key: hot:video:1m:202504251321    ← 60 分钟前（过期）
```

**热度计算流程**：

1. 点赞 Worker 处理完点赞时，执行 `ZINCRBY hot:video:1m:{当前分钟} 1 {videoID}`
2. 用户请求热度推荐时，服务器把最近 60 个 ZSET 合并成一个快照

```go
// ZUnionStore 合并 60 个分钟的 ZSET
// 目标：hot:video:snapshot:{timestamp}
// 每个分钟 ZSET 权重为 1（简单累加）
f.rediscache.ZUnionStore(ctx, snapshotKey, aggregate, keys...)
```

3. 对快照 ZSET 做 offset 分页（因为快照在 1 分钟内是稳定的）

**为什么用 offset 分页而不是游标**：
- 快照在 1 分钟内不变，offset 不会导致重复或遗漏
- ZSET 的 `ZREVRANGE` 原生支持 offset + count，天然高效
- offset 分页实现简单

**过期清理**：
- Redis key 自带 70 分钟 TTL（比窗口多 10 分钟）
- 过期后自动删除，不需要额外处理

**降级策略**：
- Redis 挂了时，降级到 MySQL 三字段游标分页
```go
// 排序：popularity DESC, create_time DESC, id DESC
// 三字段游标兜底
WHERE (popularity < ?) 
   OR (popularity = ? AND create_time < ?) 
   OR (popularity = ? AND create_time = ? AND id < ?)
```

代码位置：
- [feed/service.go 热度流](file:///f:/zhuomian/项目/feedsystem_video_go-main/backend/internal/feed/service.go#L462-L568)
- [popularity_cache.go](file:///f:/zhuomian/项目/feedsystem_video_go-main/backend/internal/video/popularity_cache.go)

### 加分点
- 能解释为什么选 60 分钟窗口：短视频热度衰减快，1 小时的时间粒度已经足够
- 能提到 `ZUnionStore` 的聚合函数选择：`SUM` 模式
- 快照缓存优化：1 分钟内同页复用，`AsOf` 字段标记快照时间戳
- 对比其他热度算法：比如 Reddit 的 `(点赞数 - 踩数) / 时间衰减`，或者基于分数的加权算法

---

## 问题 8：如果 Redis 突然挂了，你的系统会怎么样？

⭐⭐ **难度**：进阶  
**考察点**：降级设计、高可用意识

### 问题
> "线上 Redis 突然宕机了，你的 Feed 流系统还能用吗？用户会有什么感知？"

### 参考答案

**用户的感知：几乎没有影响** 🎯

系统对 Redis 的所有操作都做了降级处理：

| Redis 功能 | 降级策略 | 用户感知 |
|-----------|---------|---------|
| 视频详情缓存 | 自动切到 L3 MySQL | 无感知（变慢 10ms 但用户不察觉） |
| Feed 流冷热分离 | 全部走 MySQL | 无感知（翻到很旧的页会变慢一点点） |
| 热度排行 ZSET | 切到 MySQL 三字段游标 | 无感知 |
| Token 缓存 | 切到 DB 查询 | 无感知 |
| 限流计数器 | 放行所有请求（限流失效） | 无感知（但可能有安全风险） |

**核心保证**：
```go
// 所有 Redis 操作套上 50ms 超时
cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)

// Redis 出错只打日志，不返回错误给用户
if err != nil {
    log.Printf("L2 Redis 挂了，降级到 MySQL: %v", err)
    missedL2 = missedL1  // 全部降级
}
```

**代价**：
1. 所有请求变慢（从 5ms 变成 50ms）
2. 限流失效（恶意请求可能进来）
3. 数据库压力增大（没了缓存保护）

但至少**系统还能用**，用户还能刷视频。

### 加分点
- 能提到限流失效的风险：Redis 挂了限流保护也没了，恶意攻击可能打垮数据库。建议在限流中间件做本地内存兜底（比如令牌桶算法），即使 Redis 挂了也有本地限流
- 能提到数据库连接池监控：Redis 挂了后 DBA 需要关注 MySQL 连接数是否飙升，防止 MySQL 也被打垮
- 能提到"缓存雪崩"的预防：如果 Redis 重启，所有缓存同时失效，大量请求涌入数据库。建议用 SingleFlight + 缓存预热机制

---

## 问题 9：项目中大量使用 goroutine 做异步操作，怎么保证 goroutine 不泄漏？

⭐⭐ **难度**：进阶  
**考察点**：Go 并发控制、资源管理、context 使用

### 问题
> "你的代码里有很多 `go func() { ... }()` 做异步回写缓存的操作，如果请求量很大，怎么保证这些 goroutine 不会泄漏？"

### 参考答案

项目中异步 goroutine 的使用模式：

**模式一：异步回填缓存（带超时控制）**
```go
go func(k string, b []byte) {
    // 每个异步操作都有独立的超时 context
    setCtx, setCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
    defer setCancel()  // 防止 context 泄漏
    
    f.rediscache.SetBytes(setCtx, k, b, time.Hour)
}(cachekey, b)
```

**为什么不会泄漏**：
1. 每个 goroutine 的操作有明确的时间边界（50ms 超时）
2. 操作完成后 goroutine 自然退出
3. 即使 Redis 卡住了，50ms 后 context 超时，goroutine 退出

**模式二：Worker 进程的 for-select 循环（优雅关闭）**
```go
func (w *LikeWorker) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():  // 收到关闭信号，goroutine 正常退出
            return ctx.Err()
        case d := <-deliveries:
            w.handleDelivery(ctx, d)
        }
    }
}
```

**goroutine 泄漏的三种防护措施**：
1. **永远给异步操作加 context 超时** — 防止操作卡死
2. **defer 释放资源** — 锁、连接、context cancel 函数等
3. **使用 Worker 池模式** — Worker 进程的 goroutine 数量是可控的，和队列数量一致

### 加分点
- 能提到 `errgroup` 或 `sync.WaitGroup` 管理 goroutine 生命周期
- 能指出项目中某些地方 goroutine 没有 recover panic，如果异步操作 panic 会导致整个进程崩溃（建议加上 recover）
- 能通过 pprof 观察 goroutine 数量的经验（`go tool pprof http://localhost:6060/debug/pprof/goroutine`）
