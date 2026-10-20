# feedsystem_video_go

基于 Go 的短视频 Feed 系统（后端 + 前端），包含账号、视频、点赞、评论、关注与 Feed 流；支持 Redis 缓存与 RabbitMQ 异步 Worker（API 进程与 Worker 进程可拆分部署）。

详细设计与接口说明请阅读：`feedsystem_video_go项目设计.md`（包含模块设计、表结构、流程图与接口清单）。
## [项目演示](https://www.bilibili.com/video/BV1Dti7B9E6Y?vd_source=4b2884373b2c4c4147b10162c1709276)

## 代码结构（DDD 分层）

```text
backend/internal/
├── domain/          # 领域层：实体、值对象、领域错误、仓储接口
├── application/     # 应用层：用例编排（账号、视频、Feed、社交）
├── infrastructure/  # 基础设施层：MySQL、Redis、RabbitMQ、Worker
├── interfaces/      # 接口层：HTTP Handler、DTO、中间件、WebSocket
└── http/            # 路由装配，只注册路由与中间件
```

依赖方向为 `interfaces → application → domain`，`infrastructure` 负责实现 `domain/application` 定义的接口。
`internal/architecture` 与 `internal/http` 中的测试会校验分层依赖和路由快照。

## 环境变量与配置

仓库只提交配置模板，真实密码和密钥通过环境变量注入：

```bash
cp .env.example .env
# 修改 .env 中的 MYSQL_ROOT_PASSWORD、REDIS_PASSWORD、RABBITMQ_PASSWORD、JWT_SECRET
```

本地非 Docker 运行时，再复制一份后端配置：

```bash
cp backend/configs/config.example.yaml backend/configs/config.yaml
```

后端进程会读取同名环境变量并覆盖 YAML 中的值。`JWT_SECRET` 必须设置，否则登录和鉴权会直接报错。

## 认证接口示例

登录返回 Access Token（15 分钟）和 Refresh Token（7 天）：

```bash
curl -X POST http://localhost:8080/account/login \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"password"}'
```

响应：

```json
{
  "token": "<access_token>",
  "access_token": "<access_token>",
  "refresh_token": "<refresh_token>",
  "token_type": "Bearer",
  "expires_in": 900,
  "refresh_expires_in": 604800
}
```

刷新 Token（每次刷新都会轮换，旧 Refresh Token 立即失效）：

```bash
curl -X POST http://localhost:8080/account/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<refresh_token>"}'
```

调用受保护接口：

```bash
curl -X POST http://localhost:8080/video/publish \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -d '{"title":"demo","play_url":"http://example.com/a.mp4","cover_url":"http://example.com/a.jpg"}'
```

登出会撤销当前 Refresh Token 所属的整个登录会话：

```bash
curl -X POST http://localhost:8080/account/logout \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<refresh_token>"}'
```

## Docker Compose 一键启动（推荐）

要求：已安装 Docker Desktop / Docker Engine + Docker Compose。

```bash
docker compose up -d --build
```

访问：
- 前端：`http://localhost:5173`
- 后端 API：`http://localhost:8080`
- RabbitMQ 管理台：`http://localhost:15672`（默认账号 `admin` / `password123`）

说明：
- Compose 会启动 `mysql`、`redis`、`rabbitmq`、`backend`（API）、`worker`、`frontend`。
- 容器内后端配置使用 `backend/configs/config.example.yaml`（会挂载到 `/app/configs/config.yaml`）。

## 本地开发启动（不容器化）
1) 先启动依赖（也可以只用 compose 拉起依赖）：
```bash
docker compose up -d mysql redis rabbitmq
```

1) 用 compose 启 MySQL 时，宿主机端口是 `3308`。本地 Go 进程请使用面向 compose 依赖的配置（复制 `backend/configs/config.example.yaml` 后按需调整端口）。

2) 启动后端 API：
```bash
cd backend
CONFIG_PATH=configs/config.yaml go run ./cmd
```

1) 启动 Worker（消费 MQ、异步落库/更新 Redis 热榜）：
```bash
cd backend
CONFIG_PATH=configs/config.yaml go run ./cmd/worker
```

1) 启动前端（开发模式）：
```bash
cd frontend
npm install
npm run dev
```

前端默认使用 Vite 代理 `/api` 到 `http://127.0.0.1:8080`（见 `frontend/vite.config.ts`）。

`backend/configs/config.yaml` 继续适用于本机原生 MySQL `3306` 的场景。

---

## 🚀 纯本地环境运行（完全不用 Docker，推荐！）

如果你的电脑已经安装了 MySQL、Redis、RabbitMQ，完全不用 Docker 可以获得 3-10 倍的性能提升！

### 前置要求

| 软件 | 推荐版本 |
|--------|---------|
| Go | 1.21+ |
| MySQL | 8.0+ |
| Redis | 6.x / 7.x |
| RabbitMQ | 3.x |
| Node.js | 18+ |

---

非首次运行：
```
# 终端1：启动后端 API
cd backend
go run ./cmd

# 终端2：启动 Worker（可选，没有MQ也能跑）
cd backend
go run ./cmd/worker

# 终端3：启动前端
cd frontend
npm run dev
```


### 步骤 1：确认本地服务运行

#### MySQL
```powershell
# 验证 MySQL（默认端口 3306）
mysql -uroot -p你的密码 -e "SELECT VERSION();"

# 创建数据库
mysql -uroot -p你的密码 -e "CREATE DATABASE IF NOT EXISTS feedsystem CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
```

#### Redis
```powershell
# 验证 Redis（默认端口 6379）
redis-cli PING
# 返回 PONG 表示正常
```

#### RabbitMQ
```powershell
# 验证 RabbitMQ（默认端口 5672）
rabbitmqctl status

# 启用管理插件（可选，方便看监控）
rabbitmq-plugins enable rabbitmq_management
# 管理后台: http://localhost:15672
# 默认账号密码: guest / guest
```

---

### 步骤 2：修改配置文件

编辑 `backend/configs/config.yaml`：

```yaml
server:
  port: 8080

database:
  host: localhost
  port: 3306
  user: root            # 你的 MySQL 用户名
  password: "你的密码"   # 你的 MySQL 密码
  dbname: feedsystem

redis:
  host: localhost
  port: 6379
  password: ""         # 你的 Redis 密码，没有就空着
  db: 0

rabbitmq:
  host: localhost
  port: 5672
  username: guest     # RabbitMQ 默认账号
  password: guest     # RabbitMQ 默认密码
```

---

### 步骤 3：启动后端 API

```powershell
cd backend

# 启动！！！（表结构会由 GORM AutoMigrate 自动创建）
go run ./cmd
```

✅ 启动成功你会看到：
```
MySQL connected ✅
Redis connected (cache enabled) ✅
RabbitMQ connected ✅
Server is running on port 8080
```

---

### 步骤 4：启动 Worker 异步消费

**新开一个终端**：
```powershell
cd backend
go run ./cmd/worker
```

✅ 启动成功你会看到：
```
Worker started, consuming queue=social.events.v2
Worker started, consuming queue=like.events.v2
Worker started, consuming queue=comment.events.v2
Worker started, consuming queue=video.popularity.events.v2
```

---

### 步骤 5：启动前端

**新开一个终端**：
```powershell
cd frontend
npm install
npm run dev
```

✅ 访问：`http://localhost:5173`

---

### ✨ 本地运行的性能优势

| 操作 | Docker 运行 | 纯本地运行 | 提升倍数 |
|------|-----------|-----------|---------|
| 登录接口 | 200-500ms | 10-30ms | ~15x ⚡ |
| 发布点赞 | 150-300ms | 5-20ms | ~20x ⚡ |
| 刷新 Feed 流 | 500-2000ms | 30-100ms | ~15x ⚡ |
| 视频上传 | 取决于文件+挂载同步 | 仅取决于磁盘IO | 差异巨大 |

---

### 📝 常见问题排查

#### 1. RabbitMQ 连接失败？
- 检查是否安装了 RabbitMQ
- 本地默认账号密码是 `guest` / `guest`，不是 `admin` / `password123`（那是 Docker 用的）

#### 2. Redis 连接失败？
- 检查 Redis 是否设置了密码
- Windows 安装的 Redis 默认无密码，配置文件里 `password: ""` 留空即可

#### 3. 第一次运行慢？
- 第一次 AutoMigrate 建表会慢一点，第二次就好了
- Go 首次运行需要编译，后续有缓存会很快

#### 4. 登录提示 record not found？
- 数据库是空的！先点击「注册」创建一个账号再登录
- 或者直接调用接口注册：
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/account/register" -Method Post -Body (@{username="test"; password="123456"} | ConvertTo-Json) -ContentType "application/json"
```

---

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=LeoninCS/feedsystem_video_go&type=Date)](https://www.star-history.com/#LeoninCS/feedsystem_video_go&Date)
