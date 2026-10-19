# 面试问答记录 - 第7轮

## 问题
Redis 底层用了哪些措施保证读写效率高？

## 回答

Redis 的单机读写性能可以达到 **10万+ QPS**，这背后是多层设计共同作用的结果。我从数据结构、IO 模型、内存管理、持久化策略四个维度来讲。

---

### 一、纯内存操作（最根本的原因）

这是 Redis 高性能最根本的原因。内存的随机读写延迟大约是 **纳秒级**（~100ns），而磁盘的随机读写是 **毫秒级**（~10ms），两者差了 **5 个数量级**。

MySQL 等传统数据库为了保证持久化，需要把数据写入磁盘的 B+ Tree，涉及磁盘寻道和页分裂，天然就比 Redis 慢。Redis 也做持久化（RDB/AOF），但那是**异步**的——主线程只操作内存，持久化由子进程/后台线程完成。

---

### 二、高效的数据结构（节省 CPU 和内存）

Redis 实现了 8 种核心数据结构，每种底层都使用了**针对场景优化的编码方式**。以最常用的几个为例：

#### 1. String —— SDS（Simple Dynamic String）

```c
// Redis 没有直接用 C 语言的 char*
// 而是自己实现了 SDS
struct sdshdr {
    int len;    // 已用长度
    int free;   // 剩余空间
    char buf[]; // 数据
};
```

比 C 字符串好在哪：
- **O(1) 获取长度**：C 字符串要遍历 O(n)，SDS 直接读 len
- **杜绝缓冲区溢出**：修改前自动检查空间，不够就扩容
- **空间预分配**：扩容时多分配一些 free 空间，减少后续扩容次数
- **惰性空间释放**：缩短字符串时只改 len，不释放内存，避免频繁 malloc

#### 2. List —— quicklist（3.2+） / listpack（7.0+）

早期 Redis 用 ziplist（连续内存块存多个元素，节省内存） + linked list（双向指针，方便两端操作）。3.2 之后合成了 **quicklist**：

```
quicklist
├── quicklistNode (ziplist)
│   ├── [entry1] [entry2] ... [entryN]
├── quicklistNode (ziplist)
│   ├── [entry1] [entry2] ... [entryN]
└── ...
```

**本质是「分块的链表」**：
- 每个节点内部是 ziplist（连续内存，紧凑存储）
- 节点之间用双向指针串联
- 在内存紧凑和操作效率之间取得了平衡

#### 3. Hash —— 渐进式 rehash + 链地址法

```c
typedef struct dict {
    dictht ht[2];        // 两个哈希表，用于渐进式 rehash
    long rehashidx;       // rehash 进度，-1 表示未进行
    // ...
} dict;
```

**渐进式 rehash 是关键**：当哈希表需要扩容时，不是一次性把所有数据从 ht[0] 搬到 ht[1]，而是：
1. 在 ht[1] 分配好新空间
2. 每次增删改查时，顺便搬一个 bucket 的数据到 ht[1]
3. rehashidx 记录当前搬到哪里了
4. 搬完就把 ht[0] 指向 ht[1]，ht[1] 置空

这样**把 O(n) 的搬移操作分摊到了每次请求上**，避免了单次请求的耗时毛刺。

#### 4. ZSET —— 跳表（Skip List）+ 哈希表

```c
typedef struct zset {
    dict *dict;     // key->score 映射，O(1) 查分数
    zskiplist *zsl; // 按 score 排序，O(logN) 范围查询
} zset;
```

**为什么用跳表不用平衡树（如红黑树）？**

| 维度 | 跳表 | 红黑树 |
|---|---|---|
| 查询 | O(logN) | O(logN) |
| 实现 | 简单，几十行代码 | 复杂（左旋右旋、颜色翻转） |
| 范围查询 | 顺着 Level 0 链表遍历即可 | 需要中序遍历，实现复杂 |
| 内存 | 每个节点多层指针（约 1.33x 冗余） | 每个节点两个子指针 + 颜色标记 |

在 ZRANGEBYSCORE 这种范围查询场景下，跳表的优势非常明显——找到起点后，顺着最底层链表往后走就行了，而红黑树需要中序遍历。

**跳表的层高概率分布**：
```
Level 0:  100% 的节点
Level 1:  50%  的节点  (p=0.5)
Level 2:  25%  的节点
Level 3:  12.5% 的节点
...
```

每层节点数按概率减半，类似二分查找的效果，所以平均查找复杂度 O(logN)。

---

### 三、单线程 Reactor 模型（消除锁竞争）

这是 Redis 最核心的设计决策之一。Redis 的主要操作（命令处理、数据结构操作）都在**单线程**中执行。但要注意：Redis 6.0+ 引入了多线程 IO，但这是为了优化网络读写，**命令执行仍然在主线程**。

#### 为什么单线程反而更快？

**1. 消除上下文切换和锁竞争**
```
多线程模型：
线程 A: lock(mutex) → 操作数据 → unlock(mutex)  ← 消耗在锁争用
线程 B: 等待锁 → lock(mutex) → 操作数据 → unlock(mutex)

Redis 单线程模型：
主线程: READ → 解析命令 → 操作数据结构 → 写回   ← 零锁竞争
```

上下文切换的成本：一次约 1~3μs，Redis 每秒处理 10 万请求，如果每次都有锁竞争，光切换和等待就能消耗大量 CPU。

**2. Redis 的瓶颈不在 CPU 而在网络 IO 和内存**

在设计 Redis 的时代，一个简单的 SET/GET 命令的内存操作耗时约 100ns-1μs，而一次网络 IO（epoll_wait + read + write）可能耗 10-20μs。所以就算用多线程，CPU 也不是瓶颈，瓶颈在网络 IO 上。

**3. 避免了多线程编程的复杂性**
- 没有死锁、数据竞争、ABA 问题
- 所有数据结构不需要加锁
- Lua 脚本天然原子（因为是单线程执行）

#### IO 多路复用（epoll）

单线程怎么处理大量并发连接？靠的是 IO 多路复用：

```
while (1) {
    // epoll_wait 内核帮忙监控所有连接
    // 有数据来了才返回，不空转
    nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);

    for (i = 0; i < nfds; i++) {
        event = events[i];
        // 读请求 → 解析命令 → 执行 → 写响应
        process_event(event);
    }
}
```

- **select/poll**：每次要把所有 fd 从用户态复制到内核态，O(n) 遍历
- **epoll**：用事件回调机制，只返回就绪的 fd，O(1) 复杂度

这就是 Redis 能**单线程处理数万个并发连接**的原因。

---

### 四、内存优化与分配策略

#### 1. 对象共享池（小整数复用）

Redis 启动时会预创建 0~9999 的整数对象，所有用到这些数字的地方（比如 INCR 的结果、SET key 100）都**共享同一个对象**，不重复分配内存。

```c
// Redis 启动时
for (j = 0; j < OBJ_SHARED_INTEGERS; j++) {
    shared.integers[j] = createObject(OBJ_STRING, (void*)(long)j);
}
```

#### 2. jemalloc 内存分配器

Redis 默认使用 jemalloc 而不是 glibc 的 malloc。jemalloc 的优势：
- **减少内存碎片**：按 2 的幂次分桶（8B, 16B, 32B, ...），相同大小的请求复用相同 bucket
- **CPU 缓存友好**：内存连续分配，提高 cache line 命中率
- **多线程优化**：每个线程有自己的 arena，减少锁竞争

#### 3. 压缩编码（ziplist / listpack）

对小数据量自动采用压缩编码，减少内存占用：
- hash：字段数 < 512 且每个字段长度 < 64 字节 → ziplist
- list：元素数 < 512 且元素长度 < 64 字节 → ziplist（旧）/ listpack（新）
- zset：元素数 < 128 且元素长度 < 64 字节 → ziplist

压缩编码本质是**一块连续内存**，省去了指针开销。比如一个 hash 有 3 个字段，用 hashtable 需要多个 dictEntry 对象（约 48B + 额外指针），用 ziplist 可能只需要 80B 连续内存。

---

### 五、持久化不影响主线程性能

#### RDB（快照）—— Copy-on-Write

```bash
SAVE 或 BGSAVE
    ↓
fork() 出子进程
    ↓
子进程写 RDB 文件
    ↓
父进程继续处理请求（COW 技术）
```

当子进程写内存页时，如果父进程修改了该页，会触发 COW（Copy-on-Write），复制一份给子进程读。这样：
- 子进程看到的是 fork 时刻的内存快照
- 父进程正常处理写入
- **主线程完全不被阻塞**

#### AOF（追加日志）—— 三种刷盘策略

```
appendfsync always   → 每次写入都 fsync，最安全最慢
appendfsync everysec → 每秒 fsync 一次（默认），最多丢 1 秒数据
appendfsync no       → 由操作系统决定何时刷盘，最快但最不安全
```

默认的 `everysec` 策略下，AOF 写入是主线程写内存 buffer，后台线程每秒 fsync 一次，**主线程不阻塞在磁盘 IO 上**。

---

### 六、Pipeline 和批量操作

Redis 还能通过 Pipeline 进一步提升吞吐量：

```
正常模式（N 次网络往返）：
Client:  SET a 1
Server:  OK
Client:  SET b 2       ← 每次等上一次返回，浪费网络带宽
Server:  OK

Pipeline 模式（1 次网络往返）：
Client:  SET a 1 \r\n SET b 2 \r\n ...  ← 一次性发过去
Server:  OK \r\n OK \r\n ...             ← 一次性返回
```

Pipeline 消除了 RTT（Round Trip Time）的影响。在局域网中 RTT 约 0.5ms，如果 1 万条命令用 Pipeline 合并为一条，就省掉了 9999 次 RTT。

项目中我们也大量使用了 MGet、MSet 批量操作：

```go
// Feed 流中批量查 Redis
results, err := f.rediscache.MGet(cacheCtx, cacheKeys...)
// 一次网络请求拿到所有视频缓存，而不是 N 次 GET
```

以及 Lua 脚本保证原子性的同时减少网络往返：

```go
// 限流脚本：一次网络调用完成"检查 + 增加 + 设置过期"
// 不需要 GET → 判断 → INCR → EXPIRE 四次调用
const luaScript = `
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    -- ... 一次调用完成全部逻辑
`
```

---

### 七、总结

Redis 高性能的核心要素：

| 层面 | 措施 | 效果 |
|---|---|---|
| **内存** | 纯内存操作 | 比磁盘快 10^5 倍 |
| **数据结构** | SDS / 跳表 / 渐进式 rehash / quicklist | 每种场景都有最优数据结构 |
| **IO 模型** | 单线程 + epoll 多路复用 | 零锁竞争，O(1) 事件处理 |
| **内存管理** | jemalloc / 对象共享 / 压缩编码 | 减少碎片，节省内存 |
| **持久化** | 子进程 RDB / 后台线程 AOF fsync | 主线程不阻塞磁盘 IO |
| **网络** | Pipeline / 批量操作 | 消除 RTT，提高吞吐 |

最后想强调一个容易被忽视的点：**Redis 的"单线程"不是因为它不能多线程，而是因为在它的场景下单线程就是最优解**。把复杂度从并发编程转移到数据结构设计，是 Redis 最巧妙的设计哲学。
