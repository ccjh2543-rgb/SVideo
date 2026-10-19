# 面试问答记录 - 第11轮

## 问题
讲一下 JWT 认证的具体实现方式。

## 回答

我从 **令牌生成、请求验证、Token 撤销、双模式鉴权、安全措施** 五个层面来讲。

---

### 一、JWT 令牌生成

#### 1.1 密钥管理

```go
func jwtSecret() []byte {
    secret := os.Getenv("JWT_SECRET")
    if secret == "" {
        secret = "change-me-in-env"  // 开发环境兜底
    }
    return []byte(secret)
}
```

密钥通过环境变量 `JWT_SECRET` 注入，生产环境必须配置强密钥。代码中有一个默认值 `"change-me-in-env"` 仅用于本地开发。

#### 1.2 Claims 结构

```go
type Claims struct {
    AccountID uint   `json:"account_id"`
    Username  string `json:"username"`
    jwt.RegisteredClaims
}
```

相比标准 JWT，我们自定义了两个字段：`account_id`（用户 ID）和 `username`（用户名），这样在 Handler 层可以直接从 Token Claims 中获取用户信息，无需再查数据库。

#### 1.3 生成流程

```go
func GenerateToken(accountID uint, username string) (string, error) {
    now := time.Now()

    claims := Claims{
        AccountID: accountID,
        Username:  username,
        RegisteredClaims: jwt.RegisteredClaims{
            ExpiresAt: jwt.NewNumericDate(now.Add(24 * time.Hour)),  // 24小时过期
            IssuedAt:  jwt.NewNumericDate(now),                       // 签发时间
            NotBefore: jwt.NewNumericDate(now),                       // 即时生效
        },
    }

    token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
    return token.SignedString(jwtSecret())
}
```

| 字段 | 值 | 含义 |
|---|---|---|
| 签名算法 | `HS256` (HMAC-SHA256) | 对称签名 |
| `exp` (ExpiresAt) | `now + 24h` | Token 24 小时过期 |
| `iat` (IssuedAt) | `now` | 签发时间 |
| `nbf` (NotBefore) | `now` | 即时生效 |

#### 1.4 登录时完整链路

```
客户端 POST /account/login {username, password}
    │
    ▼
AccountHandler.Login
    │
    ▼
AccountService.Login
    │
    ├─ ① 查用户 → 用户不存在 → 返回错误
    ├─ ② bcrypt.CompareHashAndPassword 验证密码
    ├─ ③ auth.GenerateToken(accountID, username)
    │      └─ 生成 JWT: "eyJhbGciOiJIUzI1NiIs..."
    ├─ ④ repo.Login(id, token)     ← 写入 DB 的 account.Token 字段
    ├─ ⑤ cache.Set("account:42", token, 24h)  ← 写入 Redis 缓存
    │
    └─ 返回 token 给前端
```

---

### 二、Token 验证（中间件层）

我们实现了两种鉴权中间件，适用于不同场景。

#### 2.1 JWTAuth（强认证）—— 必须登录

```go
func JWTAuth(accountRepo, cache) gin.HandlerFunc {
    return func(c *gin.Context) {
        // 1. 提取 Authorization 头
        authHeader := c.GetHeader("Authorization")
        if authHeader == "" {
            c.AbortWithStatusJSON(401, gin.H{"error": "missing authorization header"})
            return
        }

        // 2. 解析 "Bearer <token>" 格式
        parts := strings.SplitN(authHeader, " ", 2)
        if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
            c.AbortWithStatusJSON(401, gin.H{"error": "invalid authorization header"})
            return
        }

        // 3. 解析 + 验证 JWT 签名
        claims, err := auth.ParseToken(tokenString)
        if err != nil {
            c.AbortWithStatusJSON(401, gin.H{"error": "invalid or expired token"})
            return
        }

        // 4. 验证 Token 是否已被撤销（双重校验）
        check(c, claims, tokenString, accountRepo, cache)
    }
}
```

**强认证和软认证的区别**：
- `JWTAuth`：没有 Authorization 头 → 直接 401
- `SoftJWTAuth`：没有 Authorization 头 → 放行（`c.Next()`），`accountID` 为空

#### 2.2 Token 解析（签名验证）

```go
func ParseToken(tokenString string) (*Claims, error) {
    token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
        // 防止 alg=none 攻击：严格校验签名算法
        if token.Method == nil || token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
            return nil, errors.New("unexpected signing method")
        }
        return jwtSecret(), nil
    })
    // ...
}
```

关键安全检查：**验证签名算法必须是 HS256**。如果攻击者把 JWT 头部的 `alg` 改为 `none`，这里会直接拒绝。

#### 2.3 Token 撤销验证（check 函数）—— 最核心的设计

```go
func check(c *gin.Context, claims *auth.Claims, tokenString string, accountRepo, cache) {
    key := fmt.Sprintf("account:%d", claims.AccountID)

    // 优先查 Redis（50ms 超时，快速路径）
    if cache != nil {
        b, err := cache.GetBytes(ctx, key)
        if err == nil {
            if string(b) != tokenString {
                // ❌ Redis 中的 token 和当前 token 不匹配 → 已撤销
                c.AbortWithStatusJSON(401, "token has been revoked")
                return
            }
            // ✅ Redis 匹配 → 通过
            c.Set("accountID", claims.AccountID)
            c.Set("username", claims.Username)
            c.Next()
            return
        }
    }

    // Redis 故障/未命中 → 查数据库兜底
    accountInfo, err := accountRepo.FindByID(ctx, claims.AccountID)
    if err != nil || accountInfo.Token == "" || accountInfo.Token != tokenString {
        c.AbortWithStatusJSON(401, "token has been revoked")
        return
    }

    // 回填 Redis（下次请求就不用查 DB 了）
    cache.SetBytes(ctx, key, []byte(tokenString), 24*time.Hour)

    c.Set("accountID", claims.AccountID)
    c.Set("username", claims.Username)
    c.Next()
}
```

这里有一个关键认知需要澄清：

---

### 三、Token 撤销机制

**JWT 本身一旦签发就无法撤销**（因为是无状态的）。但我们通过"有状态校验"实现了撤销能力——在 DB 和 Redis 中存储"当前有效的 token"，验证时比对。

#### 3.1 Redis/DB 双存储结构

```
Redis:  account:42 → "eyJhbGciOiJIUzI1NiIs..."  (TTL 24h)
MySQL:  accounts.token → "eyJhbGciOiJIUzI1NiIs..."
```

每次验证中间件调用 `check` 时，会校验**请求携带的 token** 是否等于 **Redis/DB 中存储的 token**。

#### 3.2 撤销时机

| 操作 | DB 操作 | Redis 操作 | 效果 |
|---|---|---|---|
| **登出** | `UPDATE token = ''` | `DEL account:42` | 旧 token 失效 |
| **改密** | 更新密码 + `UPDATE token = ''` | `DEL account:42` | 强制重新登录 |
| **重命名** | 事务更新 username + 新 token | `SET 新 token (24h)` | 旧 token 失效 |
| **Token 过期** | 不做操作 | TTL 自动过期 | 下次验证 `ParseToken` 返回过期错误 |

#### 3.3 时序示例

```
t0: 用户登录 → 生成 tokenA → 写入 DB 和 Redis
t1: 用户携带 tokenA 请求 → check 验证通过 ✅
t2: 用户登出 → DB token 清空, Redis key 删除
t3: 用户仍然携带 tokenA 请求（JWT 本身还没过期）
    → ParseToken 验证签名通过（JWT 有效）
    → check 查 Redis → 未命中
    → 查 DB → account.Token == "" → tokenA != ""
    → ❌ 401 "token has been revoked"
t4: 用户重新登录 → 生成 tokenB → 写入 DB 和 Redis
```

**这就是为什么说"JWT 撤销需要有状态存储"**——完全无状态的 JWT 是无法撤销的。我们的方案在 JWT 的基础上加了一层 Redis/DB 的 token 比对，在保持性能的同时实现了撤销能力。

---

### 四、双模式鉴权

不同接口有不同的鉴权需求，我们分了三个级别：

| 等级 | 中间件 | 无 Token 行为 | 示例接口 |
|---|---|---|---|
| **公开** | 无 | 完全开放 | 登录、注册、查用户信息 |
| **可选（软鉴权）** | `SoftJWTAuth` | 放行，`accountID=0` | Feed 流（未登录也能刷，登录后显示是否已点赞） |
| **强制（强鉴权）** | `JWTAuth` | 401 拒绝 | 上传视频、点赞、评论、关注 |

```go
// 路由注册示例
r := gin.Default()

// 公开路由——无需认证
accountGroup.POST("/login", loginLimiter, accountHandler.Login)
accountGroup.POST("/register", registerLimiter, accountHandler.Register)
videoGroup.POST("/getDetail", videoHandler.GetDetail)

// 可选认证——Feed 流用 SoftJWTAuth
feedGroup.Use(jwt.SoftJWTAuth(accountRepository, cache))
feedGroup.POST("/listLatest", feedHandler.ListLatest)
feedGroup.POST("/listLikesCount", feedHandler.ListLikesCount)

// 强制认证——写操作必须登录
protectedLikeGroup.Use(jwt.JWTAuth(accountRepository, cache))
protectedLikeGroup.POST("/like", likeLimiter, likeHandler.Like)
protectedLikeGroup.POST("/unlike", likeLimiter, likeHandler.Unlike)
```

**SoftJWTAuth 的实现细节**：

```go
func SoftJWTAuth(accountRepo, cache) gin.HandlerFunc {
    return func(c *gin.Context) {
        authHeader := c.GetHeader("Authorization")

        // 没有 Token → 放行，不设用户信息
        if authHeader == "" {
            c.Next()
            return
        }

        // 有 Token → 和 JWTAuth 完全一样的验证流程
        // 解析 → check → 设置 accountID
    }
}
```

这种设计的好处：Feed 流页面即使用户没登录也能正常浏览，但已登录用户可以看到"是否已点赞"等个性化信息。

---

### 五、Handler 层获取用户信息

验证通过后，中间件将用户信息写入 Gin 的 Context：

```go
c.Set("accountID", claims.AccountID)
c.Set("username", claims.Username)
c.Next()
```

Handler 通过辅助函数提取：

```go
func GetAccountID(c *gin.Context) (uint, error) {
    uidValue, exists := c.Get("accountID")
    if !exists {
        return 0, errors.New("accountID not found")
    }
    accountID, ok := uidValue.(uint)
    if !ok {
        return 0, errors.New("accountID has invalid type")
    }
    return accountID, nil
}

func GetUsername(c *gin.Context) (string, error) {
    val, exists := c.Get("username")
    // ... 类似逻辑
}
```

实际使用：

```go
func (vh *VideoHandler) PublishVideo(c *gin.Context) {
    authorId, _ := jwt.GetAccountID(c)       // 从中间件获取
    username, _ := jwt.GetUsername(c)         // 不需要查数据库
    video := &Video{
        AuthorID: authorId,
        Username: username,
        // ...
    }
    vh.service.Publish(c.Request.Context(), video)
}
```

---

### 六、安全性措施总结

| 措施 | 实现 | 防御目标 |
|---|---|---|
| **HS256 签名** | `jwt.SigningMethodHS256` | 防止 Token 被篡改 |
| **签名算法校验** | `token.Method.Alg() != "HS256"` | 防止 alg=none 攻击 |
| **24h 过期** | `ExpiresAt: now + 24h` | 限制 Token 泄露影响时间窗 |
| **Token 撤销** | DB/Redis 存储 + 比对 | 登出/改密后立即失效 |
| **Redis 双读双写** | Redis + DB 双重校验 | Redis 故障时 DB 兜底 |
| **密钥环境变量** | `os.Getenv("JWT_SECRET")` | 避免密钥硬编码到代码仓库 |
| **SoftJWTAuth** | 无 Token 放行但设 accountID=0 | 保护需要登录的接口不暴露给未登录用户 |

### 七、不足之处与改进方向

1. **无 Refresh Token**：Token 24 小时过期后必须重新登录。可以引入 Refresh Token 机制实现无感续期
2. **默认密钥安全隐患**：`"change-me-in-env"` 是明文硬编码，部署时如果忘记设置 `JWT_SECRET` 会导致安全风险
3. **密钥轮换**：不支持多密钥（kid），密钥轮换时需要保证旧 Token 在一段时间内仍然有效
4. **无黑名单过期清理**：Redis 中的 key 有 24h TTL 自动过期，但 DB 中的 token 字段不会自动清空
