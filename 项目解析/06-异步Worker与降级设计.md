# 06 - 异步 Worker 与 降级设计详解

> 零基础学习指南 · 第六篇：后台默默干活的工人们

---

## 🤔 灵魂拷问

> **1000个用户同时点赞，MySQL 会崩吗？**

直接同步写库：1000并发 → 数据库连接池打满 → CPU 100% → 雪崩。

用 MQ + Worker：1000请求全部进队列排队 → Worker 每秒消费50个 → 慢慢写库 → 数据库永远低压运行。

---

## 👷 项目里的 6 个 Worker

| Worker 名称 | 消费什么队列 | 做什么 |
|------------|-----------|-------|
| LikeWorker | like.events 交换机 | 异步把点赞写进 MySQL |
| CommentWorker | comment.events 交换机 | 异步把评论写进 MySQL |
| SocialWorker | social.events 交换机 | 异步把关注写进 MySQL |
| PopularityWorker | video.popularity.events | 聚合分数，更新 Redis 热榜 |
| TimelineWorker | video.timeline | 投递视频到每个粉丝的 Feed 时间线 |
| OutboxPoller | outbox_msgs 表 | 发件箱补偿投递，保证消息不丢 |

---

---

## ❤️ LikeWorker 点赞消费者详解

### 消费代码

```go
func StartLikeConsumer(rmq *LikeMQ, db *gorm.DB) {
    rmq.ConsumeLikes("video.like.queue", func(ctx context.Context, msg *LikeMessage) error {
        
        // ✅ 幂等性检查：数据库里已经有了？
        var count int64
        db.Model(&Like{}).Where("account_id = ? AND video_id = ?", 
            msg.AccountID, msg.VideoID).Count(&count)
        
        if count > 0 {
            // 已经存在了，直接 Ack，不重复消费
            return nil
        }
        
        // 真正写库
        like := &Like{
            AccountID: msg.AccountID,
            VideoID:   msg.VideoID,
            CreatedAt: msg.CreatedAt,
        }
        
        return db.Create(like).Error
    })
}
```

---

### ✨ Worker 设计三要素

| 要素 | 为什么重要 | 没做会怎么样 |
|------|-----------|------------|
| 1. **幂等性** | MQ 至少投递一次，会重复发 | 点1次赞，写进去3条记录 |
| 2. **错误重试** | 消费失败了自动重试 | 点了赞，但是写库失败了永远丢失 |
| 3. **手动 Ack** | 消费成功了才告诉 MQ删掉 | Worker 中途崩溃，消息丢了 |

---

---

## 🔥 PopularityWorker 热度聚合器详解

### 设计思路

用户每点一个赞，发一个小消息：
```json
{
    "video_id": 123,
    "likes_incr": 1,
    "timestamp": 1713879876
}
```

不要实时更新 MySQL，不要实时算排名！

让 PopularityWorker 在后台慢慢聚合：

| 时间段 | 操作 |
|--------|------|
| 用户点赞 | 发 +1 消息到 MQ → 马上返回 |
| 每10秒 | Worker 批量聚合所有 +1 |
| 每1分钟 | 更新 Redis ZSET 热榜排名 |
| 每10分钟 | 批量回写 MySQL 的 likes_count 字段 |

---

### 为什么要这样？

❌ **实时更新的灾难**：
- 1000人点赞 = 1000次 UPDATE videos SET likes_count = likes_count + 1
- 行锁竞争激烈，数据库死锁

✅ **异步聚合的好处**：
- 1000人点赞 = 1000条轻量消息进 MQ
- Worker 批量聚合：video 123 = +42
- 最后只需要 1 条 UPDATE！

> 峰值直接削平了！

---

---

## 📨 OutboxPoller 发件箱补偿器

这是整个项目最深的设计！解决了 MQ 最头疼的问题：
> **发 MQ 之前服务宕机了，消息永远丢失！**

### 发件箱模式双写

```go
// 发布视频接口
func Publish() {
    tx := db.Begin()
    
    // 1. 写 videos 表
    tx.Create(&video)
    
    // ✅ 2. 写 outbox_msgs 发件箱表
    tx.Create(&OutboxMsg{
        EventType: "video.published",
        Payload:   json.Marshal(video),
        Status:    "PENDING",
    })
    
    tx.Commit()  // 两张表原子提交！
    
    // 3. 然后才发 MQ
    timelineMQ.Publish(...)
    msg.Status = "SENT"
}
```

---

### OutboxPoller 后台巡检

每 5 秒扫一次发件箱：
```sql
SELECT * FROM outbox_msgs 
WHERE status = 'PENDING' 
AND created_at < NOW() - INTERVAL 1 MINUTE
```

只要是超过1分钟还没标记为 SENT 的，全部补偿重发！

**100% 保证消息不会丢！**

---

---

## ⚡ 全链路降级全景图

### 正常运行时的调用链

```
用户点赞
    ↓
发 MQ 成功 ✅
    ↓
马上返回给用户："点赞成功！"
    ↓ 后台异步
LikeWorker 消费
    ↓
写 MySQL
```
⏱️ 用户侧响应时间：~10ms

---

### MQ 挂了，降级运行

```
用户点赞
    ↓
发 MQ 失败 ❌
    ↓
【自动降级！】直接同步写 MySQL
    ↓
返回给用户："点赞成功！"
```
⏱️ 用户侧响应时间：~100ms（慢了但功能正常）

✅ **用户完全感知不到！**

---

### Redis 也挂了，极限降级

```
用户点赞
    ↓
发 MQ 失败 ❌
    ↓
同步写 MySQL ✅
    ↓
删缓存失败 ❌ 不管了！
    ↓
返回给用户："点赞成功！"
```
⏱️ 用户侧响应时间：~100ms

✅ **缓存最多脏一会儿，查DB兜底永远能拿到正确数据！**

---

### 只剩 MySQL 活着

```
Redis   ❌ 挂了
RabbitMQ ❌ 挂了
MySQL   ✅ 活着
```

✅ **所有功能 100% 正常！只是慢一点！**

---

---

## 💡 这个项目的架构哲学

### 1. 降级是默认选项

不是「做了降级优化」，而是**天生设计成能降级运行**。

### 2. 业务永远优先

> 宁可数据晚一分钟落盘，也绝对不能让用户点不了赞！

### 3. 峰值永远削平

用户看到的是「秒级响应」，后台在「秒级慢慢写」。

### 4. 面向失败设计

写每一行代码前先问：
- 这一步挂了怎么办？
- 能不能降级？
- 能不能自动恢复？
- 能不能用户无感知？

---

## ✅ 最终验收问题

全部回答上来，这个项目你就吃透了：

1. Worker 消费为什么要做幂等？不做会怎么样？
2. 发件箱模式解决了什么问题？
3. MQ 挂了，Redis 也挂了，项目还能跑吗？
4. 为什么不直接更新 MySQL 热度，要在后台异步聚合？

---

## 🎓 项目学习毕业

恭喜！你已经完整掌握了这个工业级 Go 后端项目的全部核心设计！

### 你学到的通用能力：
- ✅ 三层架构 Handler/Service/Repository
- ✅ JWT 鉴权 + 缓存自愈
- ✅ Redis 分布式限流 + 缓存套路
- ✅ RabbitMQ 异步解耦 + 削峰填谷
- ✅ 发件箱模式 + 补偿投递 = 100% 消息可靠
- ✅ 全链路优雅降级
- ✅ 面向失败编程

---

## 📚 「项目解析」文件夹完整目录

| 序号 | 文件 | 主题 |
|------|------|------|
| 01 | [项目架构与目录结构.md](01-项目架构与目录结构.md) | 从顶层看清楚整个项目的骨架 |
| 02 | [程序启动流程详解.md](02-程序启动流程详解.md) | 一行一行读懂 main.go 的7个步骤 |
| 03 | [三层架构详解.md](03-三层架构详解.md) | 为什么代码要拆成 Handler/Service/Repo |
| 04 | [中间件设计原理.md](04-中间件设计原理.md) | 鉴权、限流、缓存、消息队列的本质 |
| 05 | [业务模块详解.md](05-业务模块详解.md) | 逐个拆解6大业务模块的每个接口流程 |
| 06 | [异步Worker与降级设计.md](06-异步Worker与降级设计.md) | 高可用架构的灵魂：面向失败编程 |
