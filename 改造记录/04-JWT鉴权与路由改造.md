# 04 - JWT 鉴权与路由改造

## 1. 改造前的问题

改造前：

- `JWTAuth` 和 `SoftJWTAuth` 已经写好了，但**没有任何路由挂载**。
- 发布、点赞、评论、关注等写接口使用的是 `MockAccountID`。
- `MockAccountID` 默认把所有请求当成 `accountID=1`。
- 前端虽然发送了 `Authorization`，后端完全没有解析。
- 关注流取不到当前用户，`viewerAccountID=0`，查询退化成全站最新视频。

结果：

- 所有用户发布的视频作者都是 1。
- 点赞、评论无法区分真实用户。
- 关注流不是真正的关注流。
- JWT 双 Token 在简历里写了，但实际没有生效。

---

## 2. 改造后的中间件

文件：`backend/internal/middleware/jwt/jwt.go`

### 2.1 JWTAuth

强制登录：

```go
func JWTAuth(tokens domainaccount.TokenService) gin.HandlerFunc {
    return func(c *gin.Context) {
        claims, err := parseAccessToken(c, tokens)
        if err != nil {
            c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
            return
        }
        setClaims(c, claims)
        c.Next()
    }
}
```

行为：

| 情况 | 结果 |
|---|---|
| 没有 `Authorization` | 401 |
| 不是 `Bearer <token>` | 401 |
| 签名无效 | 401 |
| Token 过期 | 401 |
| 使用 Refresh Token | 401 |
| 校验通过 | 写入 `accountID`、`username`，继续执行 |

### 2.2 SoftJWTAuth

允许匿名，但带了 Token 就必须合法：

```go
func SoftJWTAuth(tokens domainaccount.TokenService) gin.HandlerFunc {
    return func(c *gin.Context) {
        if strings.TrimSpace(c.GetHeader("Authorization")) == "" {
            c.Next()
            return
        }
        claims, err := parseAccessToken(c, tokens)
        if err != nil {
            c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
            return
        }
        setClaims(c, claims)
        c.Next()
    }
}
```

使用场景：

- 未登录用户也能刷最新流、热榜。
- 登录用户可以获得 `is_liked` 等个性化字段。

### 2.3 上下文键

```go
const (
    contextAccountID = "accountID"
    contextUsername  = "username"
)
```

Handler 通过以下函数读取：

```go
accountID, err := jwt.GetAccountID(c)
username, err := jwt.GetUsername(c)
```

这样现有 Video、Like、Comment、Social、Feed Handler 不需要修改导入路径。

---

## 3. 路由权限矩阵

### 3.1 账号接口

| 路由 | 鉴权 | 限流 |
|---|---|---|
| `/account/register` | 公开 | IP |
| `/account/login` | 公开 | IP |
| `/account/refresh` | 公开（携带 Refresh Token） | IP |
| `/account/logout` | 强制 JWT | 无额外限流 |
| `/account/changePassword` | 强制 JWT | 无额外限流 |
| `/account/rename` | 强制 JWT | 无额外限流 |
| `/account/findByID` | 公开 | IP |
| `/account/findByUsername` | 公开 | IP |

### 3.2 视频接口

| 路由 | 鉴权 |
|---|---|
| `/video/listByAuthorID` | 公开 |
| `/video/getDetail` | 公开 |
| `/video/uploadVideo` | 强制 JWT |
| `/video/uploadCover` | 强制 JWT |
| `/video/publish` | 强制 JWT |

### 3.3 点赞接口

| 路由 | 鉴权 |
|---|---|
| `/like/like` | 强制 JWT |
| `/like/unlike` | 强制 JWT |
| `/like/isLiked` | 强制 JWT |
| `/like/listMyLikedVideos` | 强制 JWT |

### 3.4 评论接口

| 路由 | 鉴权 |
|---|---|
| `/comment/listAll` | 公开 |
| `/comment/publish` | 强制 JWT |
| `/comment/delete` | 强制 JWT |

### 3.5 关注接口

| 路由 | 鉴权 |
|---|---|
| `/social/follow` | 强制 JWT |
| `/social/unfollow` | 强制 JWT |
| `/social/getAllFollowers` | 软鉴权 |
| `/social/getAllVloggers` | 软鉴权 |

### 3.6 Feed 接口

| 路由 | 鉴权 |
|---|---|
| `/feed/listLatest` | 软鉴权 |
| `/feed/listLikesCount` | 软鉴权 |
| `/feed/listByPopularity` | 软鉴权 |
| `/feed/listByFollowing` | 强制 JWT |

关注流必须强制登录，否则 `viewerAccountID=0`，查询会退化成全站最新视频。

---

## 4. 路由接线

文件：`backend/internal/http/router.go`

```go
func SetRouter(
    db *gorm.DB,
    cache *rediscache.Client,
    rmq *rabbitmq.RabbitMQ,
    accountApp *appaccount.Service,
    tokens domainaccount.TokenService,
) *gin.Engine {
    r := gin.Default()

    accountHandler := httpaccount.NewHandler(accountApp)
    accountGroup := r.Group("/account")
    {
        accountGroup.POST("/register", registerLimit, accountHandler.Register)
        accountGroup.POST("/login", loginLimit, accountHandler.Login)
        accountGroup.POST("/refresh", refreshLimit, accountHandler.Refresh)
        accountGroup.POST("/logout", jwt.JWTAuth(tokens), accountHandler.Logout)
        accountGroup.POST("/changePassword", jwt.JWTAuth(tokens), accountHandler.ChangePassword)
        accountGroup.POST("/rename", jwt.JWTAuth(tokens), accountHandler.Rename)
        accountGroup.POST("/findByID", readLimit, accountHandler.FindByID)
        accountGroup.POST("/findByUsername", readLimit, accountHandler.FindByUsername)
    }

    // 示例：发布视频
    videoGroup.POST("/publish", jwt.JWTAuth(tokens), publishLimit, videoHandler.PublishVideo)

    // 示例：Feed 软鉴权
    feedGroup.POST("/listLatest", jwt.SoftJWTAuth(tokens), feedLimit, feedHandler.ListLatest)

    // 示例：关注流强制登录
    feedGroup.POST("/listByFollowing", jwt.JWTAuth(tokens), feedLimit, feedHandler.ListByFollowing)
}
```

注意中间件顺序：

```text
JWTAuth -> 限流 -> Handler
```

先鉴权，限流才能按 `accountID` 维度计数。

---

## 5. 已移除的 MockAccountID

改造前：

```go
func MockAccountID(c *gin.Context) {
    accountID := uint(1)
    if v := c.GetHeader("X-Account-Id"); v != "" {
        // 从 Header 读取
    }
    c.Set("accountID", accountID)
    c.Next()
}
```

问题：

- 任何人只要设置 `X-Account-Id` 就能伪造身份。
- 无法验证 Token。
- 生产环境绝对不能使用。

改造后：

- `MockAccountID` 已从路由移除。
- 所有写接口使用 `JWTAuth`。
- 压测如果需要固定用户，应通过正常登录获取 Token，而不是伪造 Header。

---

## 6. 对现有 Handler 的影响

### 6.1 VideoHandler

发布视频时：

```go
authorID, err := jwt.GetAccountID(c)
username, err := jwt.GetUsername(c)
```

现在这两个值来自 Access Token 的 claims，不再来自 Mock。

### 6.2 CommentHandler

发布评论时仍通过旧 `AccountService.FindByID` 查询用户名。

这是迁移期兼容逻辑，后续视频/评论模块迁移到 DDD 后，可以直接使用 `jwt.GetUsername(c)`，减少一次数据库查询。

### 6.3 FeedHandler

```go
viewerAccountID, err := jwt.GetAccountID(c)
if err != nil {
    viewerAccountID = 0
}
```

- 最新流/热榜使用软鉴权，未登录时为 0。
- 关注流使用强制鉴权，必须有 `viewerAccountID`。

---

## 7. 错误响应

统一返回 JSON：

```json
{
  "error": "invalid token"
}
```

常见状态码：

| 状态码 | 场景 |
|---|---|
| 401 | 缺少 Authorization、Token 无效、Token 过期、Token 类型错误 |
| 403 | Token 属于其他账号，越权操作 |
| 404 | 账号不存在 |
| 409 | 用户名已存在 |
| 429 | 触发限流 |
| 500 | 数据库或内部错误 |

---

## 8. 边界情况

| 场景 | 行为 |
|---|---|
| `Authorization: Bearer` 后为空 | 401 |
| 使用小写 `bearer` | 允许，大小写不敏感 |
| Header 中有多个空格 | `SplitN` 后仍能解析 |
| Token 前后有空格 | `TrimSpace` 后解析 |
| 伪造 `X-Account-Id` | 不再生效 |
| 匿名访问最新流 | 允许，`viewerAccountID=0` |
| 匿名访问关注流 | 401 |
| 携带过期 Token 访问软鉴权接口 | 401，而不是当游客处理 |
| Token 中的用户名与数据库不一致 | 以 Token 为准，改密/改名会撤销旧会话 |
| 旧版单 Token | 没有 `token_type` 字段，解析失败，需要重新登录 |

### 8.1 为什么软鉴权遇到无效 Token 不降级为游客

如果携带了 Token 但 Token 无效，说明客户端状态已经异常。

此时如果降级为游客：

- 用户会看到“已登录”但个性化信息全部丢失。
- 前端无法感知需要刷新 Token。

因此软鉴权的策略是：

```text
没有 Token -> 游客
有 Token 但无效 -> 401
```

前端收到 401 后自动刷新 Access Token 并重放请求。

---

## 9. 安全注意

1. `Authorization` 头只能通过 HTTPS 传输，避免被中间人窃取。
2. 不要在前端日志中打印完整 Token。
3. `JWT_SECRET` 必须足够长且随机，不能提交到仓库。
4. 生产环境建议增加 Access Token 黑名单或使用短期 Token + Refresh Token。
5. 限流应放在鉴权之后，避免匿名请求绕过账号维度限流。
