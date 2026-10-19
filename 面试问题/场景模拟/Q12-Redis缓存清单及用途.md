# 面试问答记录 - 第12轮

## 问题
Redis 中一共有哪些缓存，格式是什么样的，都用来实现什么功能？

## 回答

我按照功能模块，把系统中所有的 Redis 缓存全部分类整理如下。

---

### 一、总览

整个系统在 Redis 中使用的 key（缓存）可以分为 **5 大类**：

| 类别 | 数量 | 数据类型 | 核心用途 |
|---|---|---|---|
| **认证类** | 每个用户 1 个 | String | 存储 JWT Token，实现撤销校验 |
| **视频缓存类** | 每个视频 N 个 | String | 视频详情和实体数据缓存 |
| **Feed 流类** | 若干个 | String + ZSET | 关注流缓存、全局时间线 |
| **热度排行类** | 62 个/每次请求 | ZSET | 分钟级滑动窗口 + 合并快照 |
| **限流类** | 每个维度 1 个 | String | Redis 滑动窗口计数器 |

---

### 二、认证相关缓存

#### ① 用户 Token 缓存

| 属性 | 内容 |
|---|---|
| **Key 格式** | `account:{accountID}` |
| **示例** | `account:42` |
| **数据类型** | String |
| **Value** | JWT Token 字符串（如 `eyJhbGciOiJIUzI1NiIs...`） |
| **TTL** | 24 小时 |
| **在哪写入** | `account/service.go` — Login、Rename |
| **在哪读取** | `middleware/jwt/jwt.go` — check 函数 |
| **在哪删除** | `account/service.go` — Logout、ChangePassword |
| **用途** | **Token 撤销校验**：中间件比对请求 Token 是否等于 Redis 中的值，不一致则拒绝 |

```go
// 写入（登录时）
cache.SetBytes(ctx, "account:42", []byte(token), 24*time.Hour)

// 读取（每次请求验证时）
b, _ := cache.GetBytes(ctx, "account:42")
if string(b) != tokenString {
    // token 已被撤销
    c.AbortWithStatusJSON(401, "token has been revoked")
}

// 删除（登出时）
cache.Del(ctx, "account:42")
```

---

### 三、视频/实体缓存类

#### ② 视频详情缓存

| 属性 | 内容 |
|---|---|
| **Key 格式** | `video:detail:id={videoID}` |
| **示例** | `video:detail:id=10086` |
| **数据类型** | String |
| **Value** | JSON 序列化的 `Video` 结构体 |
| **TTL** | 5 分钟 |
| **在哪写入** | `video/video_service.go` — GetDetail |
| **在哪读出** | `video/video_service.go` — GetDetail |
| **在哪删除** | `video/video_service.go` — Delete、UpdatePopularity |
| **用途** | **视频详情页**：缓存标题、描述、播放 URL、点赞数、热度等完整信息 |

#### ③ 视频实体缓存（Feed 流 L2）

| 属性 | 内容 |
|---|---|
| **Key 格式** | `video:entity:{videoID}` |
| **示例** | `video:entity:10086` |
| **数据类型** | String |
| **Value** | JSON 序列化的 `Video` 结构体 |
| **TTL** | 1 小时（回填时设置） |
| **在哪写入** | `feed/service.go` — GetVideoByIDs 中的 SingleFlight 回填 |
| **在哪读出** | `feed/service.go` — GetVideoByIDs（MGet 批量读取） |
| **用途** | **Feed 流三级缓存的 L2**：批量获取视频列表时，避免每页 Feed 都查数据库 |

**与 `video:detail` 的区别**：
- `video:entity` 用于 Feed 流批量查询场景，用 `MGet` 一次拿多个
- `video:detail` 用于单视频详情页，带有分布式锁防击穿

```go
// Feed 流批量查询
cacheKeys := []string{"video:entity:1", "video:entity:2", "video:entity:3"}
results, _ := f.rediscache.MGet(ctx, cacheKeys...)  // 一次网络请求
```

---

### 四、Feed 流缓存类

#### ④ 全局时间线 ZSET（最新视频流）

| 属性 | 内容 |
|---|---|
| **Key 格式** | `feed:global_timeline` |
| **数据类型** | ZSET（Sorted Set） |
| **Score** | 视频发布时间的时间戳（float64） |
| **Member** | 视频 ID 字符串（如 `"10086"`） |
| **TTL** | 不过期（一直维护） |
| **在哪写入** | `worker/outboxworker.go` — StartConsumer |
| **在哪读出** | `feed/service.go` — ListLatest |
| **在哪删除** | `worker/outboxworker.go` — ZRemRangeByRank 裁剪 |
| **用途** | **最新 Feed 流的热数据层**：新视频发布时写入，按时间倒序读取 |

```go
// 写入：视频发布时，ZADD 到时间线
timelineKey := "feed:global_timeline"
redisClient.ZAdd(ctx, timelineKey, oredis.Z{
    Score:  float64(event.CreateTime),
    Member: fmt.Sprintf("%d", event.VideoID),
})

// 裁剪：只保留最新的 1000 条
redisClient.ZRemRangeByRank(ctx, timelineKey, 0, -1001)

// 读取：冷热分离，热数据从这里查
members, _ := f.rediscache.ZRevRangeByScore(ctx, timelineKey, ...)
```

**裁剪机制**：每次写入后都执行 `ZRemRangeByRank(ctx, 0, -1001)`，只保留 ZSET 中排名前 1000 的元素，防止无限增长。

#### ⑤ 关注流缓存

| 属性 | 内容 |
|---|---|
| **Key 格式** | `feed:listByFollowing:limit={limit}:accountID={id}:before={timestamp}` |
| **示例** | `feed:listByFollowing:limit=20:accountID=42:before=1746000000000` |
| **数据类型** | String |
| **Value** | JSON 序列化的 `ListByFollowingResponse` |
| **TTL** | 24 小时 |
| **在哪写入** | `feed/service.go` — ListByFollowing |
| **在哪读出** | `feed/service.go` — ListByFollowing（带分布式锁防击穿） |
| **用途** | **关注流结果缓存**：同一用户的关注流在缓存有效期内不重复计算 |

```go
cacheKey = fmt.Sprintf("feed:listByFollowing:limit=%d:accountID=%d:before=%d",
    limit, viewerAccountID, before)
```

---

### 五、热度排行缓存类（最复杂的一组）

#### ⑥ 分钟级热度窗口 ZSET（60 个）

| 属性 | 内容 |
|---|---|
| **Key 格式** | `hot:video:1m:{YYYYMMDDHHmm}` |
| **示例** | `hot:video:1m:202604301530` |
| **数据类型** | ZSET（Sorted Set） |
| **Score** | 热度值（点赞数+评论数在该分钟内的变化量） |
| **Member** | 视频 ID 字符串 |
| **TTL** | 2 小时 |
| **在哪写入** | `video/popularity_cache.go` — UpdatePopularityCache |
| **在哪读出** | `feed/service.go` — ListByPopularity（ZUNIONSTORE 合并时读取） |
| **用途** | **滑动窗口热榜的基础数据**：每分钟一个窗口，粒度 1 分钟 |

```go
// 写入：热度变化时 ZINCRBY 累加
now := time.Now().UTC().Truncate(time.Minute)
windowKey := "hot:video:1m:" + now.Format("200601021504")
cache.ZincrBy(ctx, windowKey, "10086", float64(+1))  // 热度 +1
cache.Expire(ctx, windowKey, 2*time.Hour)
```

#### ⑦ 合并快照 ZSET（每页请求临时创建）

| 属性 | 内容 |
|---|---|
| **Key 格式** | `hot:video:merge:1m:{YYYYMMDDHHmm}` |
| **示例** | `hot:video:merge:1m:202604301530` |
| **数据类型** | ZSET（Sorted Set） |
| **Score** | 视频在 60 个窗口内的总热度（SUM） |
| **Member** | 视频 ID 字符串 |
| **TTL** | 2 分钟 |
| **在哪创建** | `feed/service.go` — ListByPopularity |
| **在哪读取** | `feed/service.go` — ListByPopularity（ZRevRange 分页） |
| **用途** | **热榜稳定分页的快照**：60 个窗口合并后，在快照上翻页不跳动 |

```go
// 合并：将过去 60 分钟的窗口合并为一个快照
keys := []string{
    "hot:video:1m:202604301530",
    "hot:video:1m:202604301529",
    // ... 共 60 个
}
dest := "hot:video:merge:1m:202604301530"

// 只创建一次，同 asOf 内翻页复用
if exists, _ := cache.Exists(ctx, dest); !exists {
    cache.ZUnionStore(ctx, dest, keys, "SUM")  // 分数相加
    cache.Expire(ctx, dest, 2*time.Minute)     // 2 分钟后自动过期
}

// 分页读取
members, _ := cache.ZRevRange(ctx, dest, start, stop)
```

---

### 六、限流缓存类

#### ⑧ 限流计数器

| 属性 | 内容 |
|---|---|
| **Key 格式** | `feedsystem:ratelimit:{业务前缀}:{维度值}` |
| **示例** | `feedsystem:ratelimit:account_login:192.168.1.100` |
| **示例** | `feedsystem:ratelimit:like_write:42` |
| **数据类型** | String（整数值） |
| **Value** | 递增的计数（如 `1`, `2`, `3`...） |
| **TTL** | 窗口时长（如 1 分钟、1 小时） |
| **在哪写入** | `middleware/ratelimit/ratelimit.go` — 每个请求经过时 +1 |
| **在哪读出** | 同上（Lua 脚本原子操作：INCR → 判断是否 > maxRequests） |
| **用途** | **接口限流**：按 IP 或用户 ID 限制请求频率 |

```go
key := "feedsystem:ratelimit:like_write:42"
// Lua 脚本原子执行：INCR → count==1 时设 PEXPIRE → return count
count, _ := cache.IncrementWithExpire(ctx, key, time.Minute)
if count > 30 {
    c.AbortWithStatusJSON(429, "too many requests")
}
```

---

### 七、分布式锁（辅助类）

虽然不是严格意义上的"缓存"，但也是重要的 Redis 使用场景：

#### ⑨ 分布式锁 Key

| Key 格式 | 示例 | 用途 |
|---|---|---|
| `lock:video:detail:id={videoID}` | `lock:video:detail:id=10086` | 视频详情缓存回填防击穿 |
| `lock:feed:listByFollowing:...` | `lock:feed:listByFollowing:limit=20:...` | 关注流缓存回填防击穿 |

```go
// 获取锁：SETNX + Lua 脚本释放
token, locked, _ := cache.Lock(ctx, lockKey, 2*time.Second)
if locked {
    // 查数据库回填缓存
    defer cache.Unlock(ctx, lockKey, token)
}
```

#### ⑩ SingleFlight Key

| Key 格式 | 示例 | 用途 |
|---|---|---|
| `sf:entity:{videoID}` | `sf:entity:10086` | Feed 流三级缓存 L2→L3 防击穿 |
| `sf:cold:listLatest:{limit}:{time}` | `sf:cold:listLatest:20:1746000000` | 最新流冷数据防击穿 |
| `sf:stitch:listLatest:{limit}:{time}` | `sf:stitch:listLatest:10:1746000000` | 冷热拼接防击穿 |

注意：SingleFlight Key 是进程内的，**不存储在 Redis 中**，只是用于 `singleflight.Group` 的 key 名称。

---

### 八、完整总结一览表

| # | Key 格式 | 数据类型 | TTL | 写入位置 | 用途 |
|---|---|---|---|---|---|
| ① | `account:{id}` | String | 24h | account/service | Token 撤销校验 |
| ② | `video:detail:id={id}` | String | 5min | video/service | 视频详情页缓存 |
| ③ | `video:entity:{id}` | String | 1h | feed/service | Feed 流 L2 缓存 |
| ④ | `feed:global_timeline` | ZSET | 永不过期(裁剪) | worker/outbox | 最新流热数据 |
| ⑤ | `feed:listByFollowing:limit={}:accountID={}:before={}` | String | 24h | feed/service | 关注流结果缓存 |
| ⑥ | `hot:video:1m:{YYYYMMDDHHmm}` × 60 | ZSET | 2h | video/popularity_cache | 分钟窗口热榜 |
| ⑦ | `hot:video:merge:1m:{YYYYMMDDHHmm}` | ZSET | 2min | feed/service | 热榜快照分页 |
| ⑧ | `feedsystem:ratelimit:{prefix}:{dimension}` | String | 窗口时长 | middleware/ratelimit | 限流计数器 |
| ⑨ | `lock:{lockTarget}` | String | 锁时长 | 多处使用 | 分布式锁 |

从数据类型角度看：
- **String（5 个）**：Token、视频详情、视频实体、关注流、限流计数器
- **ZSET（3 个）**：全局时间线、分钟窗口(×60)、合并快照
