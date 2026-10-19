# 面试问答记录 - 第15轮

## 问题
讲一下 Inbox 和 Outbox。

## 回答

Inbox 和 Outbox 是消息系统中两种常见的**可靠性保证模式**。它们解决的核心问题不同：

| 模式 | 解决的核心问题 | 类比 |
|---|---|---|
| **Outbox** | 保证"写数据库"和"发消息"的原子性 | 寄信时先写好信（DB），邮递员定时来取（MQ） |
| **Inbox** | 保证"消费消息"和"写业务数据"的幂等性 | 收信时先登记已收到，防止重复拆阅 |

---

### 一、Outbox 模式（我们项目中完整实现了）

#### 1.1 问题背景

在分布式系统中，经常需要执行"写数据库 + 发消息"这两个操作。但它们不是原子的：

```go
// ❌ 典型的分布式事务问题
func publishVideo(video *Video) {
    db.Create(video)           // 1. 写数据库成功
    mq.Publish(event)          // 2. 发消息失败 → 数据库有数据，但消息没发出去
                               //    时间线没更新，用户搜不到这个视频
}

// 或者反过来：
func publishVideo(video *Video) {
    mq.Publish(event)          // 1. 发消息成功
    db.Create(video)           // 2. 写数据库失败 → 消息发出去了，但数据库没有
                               //    时间线上多了一个不存在的视频
}
```

Outbox 模式通过**本地事务**解决了这个问题。

#### 1.2 实现原理

```sql
-- 核心思路：在同一个数据库事务中，同时写入业务数据和待投递消息
BEGIN TRANSACTION;
    INSERT INTO videos ...;                    -- 业务数据
    INSERT INTO outbox_messages (video_id, event_type, status, create_time)
    VALUES (?, 'video_published', 'pending', NOW());  -- 待投递消息
COMMIT;
```

然后由一个独立的 **OutboxPoller** 轮询 outbox 表，将 pending 状态的消息投递到 MQ。

#### 1.3 项目中的具体实现

**第1步：事务中写入业务数据 + outbox 消息**

```go
// video_service.go
func (vs *VideoService) Publish(ctx context.Context, video *Video) error {
    // 同一个事务中写入两张表
    err := vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
        // 写入视频记录
        if err := tx.Create(video).Error; err != nil {
            return err
        }

        // 写入 outbox 消息（状态为 pending）
        msg := OutboxMsg{
            VideoID:    video.ID,
            EventType:  "video_published",
            Status:     "pending",
            CreateTime: video.CreateTime,
        }
        if err := tx.Create(&msg).Error; err != nil {
            return err
        }
        return nil
    })
    return err
}
```

**第2步：OutboxPoller 轮询投递**

```go
// outboxworker.go
func StartOutboxPoller(db *gorm.DB, tmq *rabbitmq.TimelineMQ) {
    go func() {
        for {
            // 轮询 pending 消息（顺序按创建时间，每次最多 100 条）
            var messages []video.OutboxMsg
            db.Where("status = ?", "pending").
               Order("create_time ASC").
               Limit(100).
               Find(&messages)

            if len(messages) == 0 {
                time.Sleep(1 * time.Second)  // 没有消息时休眠
                continue
            }

            for _, msg := range messages {
                // 投递到 MQ
                err := tmq.PublishVideo(context.Background(), msg.VideoID, msg.CreateTime)
                if err == nil {
                    db.Delete(&msg)  // 投递成功 → 删除 outbox 记录
                } else {
                    log.Printf("投递MQ失败: VideoID: %d, err: %v", msg.VideoID, err)
                    // 失败则保留，下次轮询继续投递
                }
            }
        }
    }()
}
```

**第3步：Timeline Consumer 消费消息，写入全局 ZSET**

```go
// outboxworker.go - StartConsumer
// 消费视频发布事件，写入 Redis 全局时间线 ZSET
timelineKey := "feed:global_timeline"
redisClient.ZAdd(ctx, timelineKey, oredis.Z{
    Score:  float64(event.CreateTime),
    Member: fmt.Sprintf("%d", event.VideoID),
})
// 裁剪到 1000 条
redisClient.ZRemRangeByRank(ctx, timelineKey, 0, -1001)
```

#### 1.4 Outbox 模式的全流程

```
请求: POST /video/publish
    │
    ▼
┌─────────────────────────────────────┐
│  本地事务                            │
│  ├─ INSERT INTO videos              │
│  └─ INSERT INTO outbox_messages     │  ← 同一事务，原子性
│       (status = 'pending')          │
└─────────────────────────────────────┘
    │ 事务提交成功
    ▼
┌─────────────────────────────────────┐
│  OutboxPoller (独立 goroutine)        │
│  轮询: SELECT * FROM outbox          │
│  WHERE status = 'pending'           │
│        │                             │
│        ▼                             │
│  投递到 RabbitMQ                     │
│        │                             │
│  成功 → DELETE FROM outbox           │
│  失败 → 保留，下次重试               │
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│  Timeline Consumer                   │
│  ZADD feed:global_timeline           │
│  ZRemRangeByRank 裁剪到 1000 条       │
└─────────────────────────────────────┘
```

#### 1.5 Outbox 模式的设计要点

| 要点 | 本项目做法 |
|---|---|
| **投递保证** | At-least-once（至少一次）——失败后保留消息，持续重试 |
| **消费幂等** | 由下游保证（ZADD 是幂等的，同一个 key-score-member 重复添加不影响） |
| **顺序保证** | 不严格保证顺序（按 create_time ASC 轮询，尽量顺序） |
| **失败处理** | 只 log，不阻塞，下次轮询继续重试 |
| **删除时机** | 投递成功立即删除（不是标记 done，是直接 DELETE） |

#### 1.6 为什么不用双写（同时写 DB 和 MQ）？

双写的问题是**没有原子性**。无论先写 DB 还是先发 MQ，都可能出现部分成功。

而 Outbox 模式的关键优势就是：**通过本地事务保证了业务数据和消息的原子性**。

---

### 二、Inbox 模式（我们的项目中未实现，但值得了解）

#### 2.1 问题背景

Inbox 模式在**消息消费端**解决"消息重复投递"的问题。在 At-least-once 语义下，同一条消息可能被投递多次：

```
MQ 投递消息（处理成功，但 ACK 超时）
    ↓
MQ 重新投递同一消息（因为没收到 ACK）
    ↓
消费者再次处理 → 如果没做幂等 → 数据重复
```

#### 2.2 实现原理

消费者在处理消息前，先把消息 ID 记录到一张 **inbox 表**中（带唯一约束），如果消息已经处理过了就跳过：

```sql
-- 消息消费流程
BEGIN TRANSACTION;
    -- 1. 先查 inbox 表：这条消息处理过了吗？
    INSERT INTO inbox_messages (message_id, event_type, processed_at)
    VALUES (?, 'video_published', NOW());
    -- ↑ 如果 message_id 重复，违反唯一约束 → 跳过

    -- 2. 执行业务逻辑
    INSERT INTO videos ...;
COMMIT;
```

#### 2.3 Inbox vs. 我们当前用的幂等方式

项目中我们用了不同的幂等方式，没有单独建 inbox 表：

| 场景 | 幂等方式 | 原理 |
|---|---|---|
| **点赞** | `LikeIgnoreDuplicate` | MySQL 唯一索引 `(video_id, account_id)`，插入冲突时静默忽略 |
| **关注** | 同上 | 联合唯一索引 `(follower_id, vlogger_id)` |
| **ZSET 写入** | ZADD 天然幂等 | 相同 score + member 多次 ZADD 效果相同 |
| **视频发布** | Outbox 模式 | 先写 outbox 表，投递后删除 |

**Inbox 模式和唯一索引的本质是一样的**——都是通过"先检查再写入"或"冲突时忽略"来实现幂等。区别在于：

- **唯一索引**：适合业务约束本身就要求唯一的场景（如点赞、关注）
- **Inbox 表**：适合业务逻辑本身不要求唯一，但需要防消息重复的场景（如发短信通知、扣减库存）

#### 2.4 Inbox 模式的典型流程

```
MQ 投递消息（id = "msg_12345"）
    │
    ▼
消费者收到消息
    │
    ▼
┌─────────────────────────────────────┐
│  BEGIN TRANSACTION                   │
│                                      │
│  ├─ 尝试插入 inbox_messages           │
│  │   (message_id = 'msg_12345')      │
│  │   └─ 唯一冲突 → 已处理过 → COMMIT → 跳过 │
│  │                                      │
│  ├─ 执行业务逻辑                       │
│  │   (批量发短信、扣减库存等)            │
│  │                                      │
│  └─ COMMIT                             │
└─────────────────────────────────────┘
    │
    ▼
ACK 确认消息
```

#### 2.5 什么时候需要用 Inbox？

| 场景 | 是否需要 Inbox | 原因 |
|---|---|---|
| 点赞、关注（有唯一约束） | ❌ 不需要 | 数据库唯一约束天然幂等 |
| ZSET 写入 | ❌ 不需要 | ZADD 天然幂等 |
| **发短信通知** | ✅ 需要 | 同一条消息不能发两遍短信，没有唯一约束兜底 |
| **扣减库存** | ✅ 需要 | 重复扣减会导致超卖 |
| **发送邮件** | ✅ 需要 | 用户收到两封一样的邮件体验差 |

---

### 三、Outbox vs. Inbox 对比总结

```
            生产者端                             消费者端
    ┌──────────────────┐              ┌──────────────────┐
    │    Outbox 模式     │              │    Inbox 模式     │
    │                    │              │                    │
    │  业务数据           │     MQ       │  收到消息           │
    │  +                 │  ────────→   │  ├─ 查 Inbox       │
    │  Outbox 记录       │              │  ├─ 已处理 → 跳过  │
    │  (同一事务)         │              │  ├─ 未处理 → 执行  │
    │                    │              │  └─ 写入 Inbox     │
    └──────────────────┘              └──────────────────┘
    保证：消息不丢                        保证：不重复处理
```

| 维度 | Outbox | Inbox |
|---|---|---|
| **位置** | 生产者端（写 DB 时） | 消费者端（消费 MQ 时） |
| **解决的问题** | 事务性发消息：业务数据 + 消息不丢 | 幂等消费：同一条消息不重复处理 |
| **技术方案** | 本地事务写入 outbox 表，轮询投递 | 消息 ID 唯一约束去重 |
| **我们的项目** | ✅ 视频发布完整实现 | ❌ 未实现（用唯一索引替代） |
| **应用场景** | 任何需要"写数据 + 发消息"的场景 | 任何需要"消费消息 + 执行业务"的场景 |

### 四、Outbox 在我们项目中的改进空间

当前实现有几个可以改进的点：

**1. 删除改为标记完成**

```go
// 当前：投递成功直接 DELETE
db.Delete(&msg)

// 改进：标记为 done，定期清理
db.Model(&msg).Update("status", "done")
// 后台定时清理超过 7 天的 done 记录
```

DELETE 的问题是一旦投递成功但删除失败，下次轮询会重复投递。虽然下游（ZADD）是幂等的，但更好的做法是先标记再删除。

**2. 增加投递次数和延迟重试**

```go
type OutboxMsg struct {
    ID         uint
    VideoID    uint
    EventType  string
    Status     string      // pending / done / failed
    RetryCount int         // 重试次数
    CreateTime time.Time
    LastError  string      // 上次错误信息
}

// 超过 3 次失败的消息，放入死信队列人工处理
if msg.RetryCount > 3 {
    msg.Status = "failed"
    // 告警通知
}
```

**3. 使用消息表的自增 ID 做排序和去重**

当前 outbox 表使用自增主键 ID，轮询按 `create_time ASC`，在高并发下 `create_time` 可能重复。可以改为按 `ID ASC` 排序，更精确。
