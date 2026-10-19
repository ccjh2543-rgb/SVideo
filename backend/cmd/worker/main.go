package main

import (
	"context"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	socialExchange   = "social.events"
	socialQueue      = "social.events"
	socialBindingKey = "social.*"

	likeExchange   = "like.events"
	likeQueue      = "like.events"
	likeBindingKey = "like.*"

	commentExchange   = "comment.events"
	commentQueue      = "comment.events"
	commentBindingKey = "comment.*"

	popularityExchange   = "video.popularity.events"
	popularityQueue      = "video.popularity.events"
	popularityBindingKey = "video.popularity.*"
)

// 加载配置
// 连接数据库
// 连接 Redis（用于流行度更新）
// 连接 RabbitMQ
// 启动工作进程
func main() {
	// ============================================================
	// 1. 加载配置
	// ============================================================
	// 从环境变量 CONFIG_PATH 读取配置文件路径，若未设置则使用默认路径
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		// 统一相对于 backend 工作目录解析，兼容本地 go run 与容器内 /app。
		configPath = "configs/config.yaml"
	}
	log.Printf("Loading config from %s", configPath)

	// 调用 config.Load 加载配置（生产级，配置不存在或解析失败直接 Fatal）
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Printf("Config loaded from file: %s", configPath)

	// ============================================================
	// 2. 初始化数据库连接
	// ============================================================
	// db.NewDB 根据配置创建 *sql.DB 对象（例如 MySQL / PostgreSQL）
	sqlDB, err := db.NewDB(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	defer db.CloseDB(sqlDB) // 确保程序退出时关闭数据库连接

	// ============================================================
	// 3. 初始化 Redis 连接（用于热度/流行度更新）
	// ============================================================
	// rediscache.NewFromEnv 尝试从配置创建 Redis 客户端
	var cache *rediscache.Client // 假设类型
	cache, err = rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		// Redis 配置有误，则将 cache 置空，后续 PopularityWorker 不会被创建
		log.Printf("Redis config error (popularity worker disabled): %v", err)
		cache = nil
	} else {
		// 测试 Redis 是否可达，设置短超时（300ms）避免长时间阻塞启动
		pingCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := cache.Ping(pingCtx); err != nil {
			log.Printf("Redis not available (popularity worker disabled): %v", err)
			_ = cache.Close() // 连接不可用，关闭客户端
			cache = nil
		} else {
			// Redis 可用，defer 关闭，防止资源泄露
			defer cache.Close()
			log.Printf("Redis connected (popularity worker enabled)")
		}
	}

	// ============================================================
	// 4. 连接 RabbitMQ 并声明拓扑（交换机、队列、绑定）
	// ============================================================
	var ch *amqp.Channel // RabbitMQ 通道，用于后续发送/消费消息
	// 拼接 AMQP 连接地址（amqp://user:pass@host:port/）
	url := "amqp://" + cfg.RabbitMQ.Username + ":" + cfg.RabbitMQ.Password + "@" +
		cfg.RabbitMQ.Host + ":" + strconv.Itoa(cfg.RabbitMQ.Port) + "/"

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Printf("Failed to connect rabbitmq (worker disabled): %v", err)
		log.Printf("Worker process will idle - no RabbitMQ connection available")
		// RabbitMQ 不可用时，conn 为 nil，后续 Worker 均不会被创建
	} else {
		defer conn.Close() // 确保退出时关闭连接
		// 创建 AMQP 通道（Channel），大多数操作都在 Channel 上进行
		ch, err = conn.Channel()
		if err != nil {
			log.Printf("Failed to open rabbitmq channel (worker disabled): %v", err)
			ch = nil
		} else {
			defer ch.Close() // 确保退出时关闭通道

			// ---- 声明各个业务的拓扑结构 ----
			// 每个 declare...Topology 函数内部会创建对应的 Exchange（交换机）和 Queue（队列），
			// 并使用 Bind 将队列绑定到交换机上，设置合适的 routing key。
			// 这是 AMQP 中所谓的“Topology”（拓扑）—— 消息流动的静态结构。

			// Social 业务相关的交换机和队列
			if err := declareSocialTopology(ch); err != nil {
				log.Printf("Failed to declare social topology: %v", err)
			}
			// Like 业务相关的交换机和队列
			if err := declareLikeTopology(ch); err != nil {
				log.Printf("Failed to declare like topology: %v", err)
			}
			// Comment 业务相关的交换机和队列
			if err := declareCommentTopology(ch); err != nil {
				log.Printf("Failed to declare comment topology: %v", err)
			}
			// 仅当 Redis 可用时，才声明 Popularity 业务的拓扑
			if cache != nil {
				if err := declarePopularityTopology(ch); err != nil {
					log.Printf("Failed to declare popularity topology: %v", err)
				}
			}

			// 设置 QoS（Quality of Service）：prefetch count = 50
			// 表示每个 Worker 最多同时接收 50 条未确认的消息，用于公平分发和流控
			if err := ch.Qos(50, 0, false); err != nil {
				log.Printf("Failed to set qos: %v", err)
			}
		}
	}

	// ============================================================
	// 5. 创建数据仓库（Repository）实例
	// ============================================================
	// 这些 Repository 封装了与数据库的交互逻辑，供 Worker 在处理消息时使用
	repo := social.NewSocialRepository(sqlDB)
	videoRepo := video.NewVideoRepository(sqlDB)
	likeRepo := video.NewLikeRepository(sqlDB)
	commentRepo := video.NewCommentRepository(sqlDB)

	// ============================================================
	// 6. 创建具体的 Worker（消费者）
	// ============================================================
	// Worker 负责从指定队列消费消息，并调用相应的业务逻辑处理
	var socialWorker *worker.SocialWorker
	var likeWorker *worker.LikeWorker
	var commentWorker *worker.CommentWorker
	var popularityWorker *worker.PopularityWorker

	// 只有当 RabbitMQ 通道（ch）不为 nil 时，才创建工作线程
	// 队列名称（如 socialQueue）应当是在其他地方定义的常量，代表需要监听的队列名
	if ch != nil {
		socialWorker = worker.NewSocialWorker(ch, repo, socialQueue)
		likeWorker = worker.NewLikeWorker(ch, likeRepo, videoRepo, likeQueue)
		commentWorker = worker.NewCommentWorker(ch, commentRepo, videoRepo, commentQueue)

		// PopularityWorker 仅在 Redis 客户端也有效时才创建
		if cache != nil {
			popularityWorker = worker.NewPopularityWorker(ch, cache, popularityQueue)
		}
	}

	// ============================================================
	// 7. 启动 Worker 并管理生命周期
	// ============================================================
	// 创建一个可被系统信号（SIGINT, SIGTERM）取消的 context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // 确保在函数退出时取消信号监听

	// 启动 pprof 性能分析 HTTP 服务（用于调试）
	pprofServer, err := observability.NewPprofServer(
		"Worker",
		cfg.ObservabilityConfig.Pprof.Enabled,
		cfg.ObservabilityConfig.Pprof.WorkerAddr,
	)
	if err != nil {
		log.Printf("Failed to start worker pprof server: %v", err)
	}
	defer pprofServer.Close()

	// 错误通道，用于捕获各个 Worker 运行时的错误
	errCh := make(chan error, 4)
	workerCount := 0 // 实际启动的 Worker 数量

	// 依次开启 Social、Like、Comment、Popularity 四个 Worker 的 goroutine
	if socialWorker != nil {
		log.Printf("Worker started, consuming queue=%s", socialQueue)
		go func() { errCh <- socialWorker.Run(ctx) }()
		workerCount++
	}
	if likeWorker != nil {
		log.Printf("Worker started, consuming queue=%s", likeQueue)
		go func() { errCh <- likeWorker.Run(ctx) }()
		workerCount++
	}
	if commentWorker != nil {
		log.Printf("Worker started, consuming queue=%s", commentQueue)
		go func() { errCh <- commentWorker.Run(ctx) }()
		workerCount++
	}
	if popularityWorker != nil {
		log.Printf("Worker started, consuming queue=%s", popularityQueue)
		go func() { errCh <- popularityWorker.Run(ctx) }()
		workerCount++
	}

	// ============================================================
	// 8. 等待 Worker 结束或收到退出信号
	// ============================================================
	if workerCount == 0 {
		// 没有启动任何 Worker，进程进入空闲等待，直到接收到关闭信号
		log.Printf("No workers running - waiting for shutdown signal")
		<-ctx.Done()
		err = ctx.Err() // 通常为 context.Canceled
	} else {
		// 有 Worker 运行，则阻塞等待其中任意一个 Worker 返回错误（或 context 取消）
		// 一旦收到第一个错误，主 goroutine 便会退出，并通过 ctx 取消其他 Worker
		err = <-errCh
	}

	// 判断退出原因，如果是 context.Canceled（正常收到信号），则视为正常退出
	if err != nil && err != context.Canceled {
		log.Fatalf("Worker stopped: %v", err)
	}
	log.Printf("Worker stopped")
}
func declareSocialTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		socialExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		socialQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	if err := ch.QueueBind(
		q.Name,
		socialBindingKey,
		socialExchange,
		false,
		nil,
	); err != nil {
		return err
	}
	return nil
}

func declarePopularityTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		popularityExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		popularityQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return ch.QueueBind(
		q.Name,
		popularityBindingKey,
		popularityExchange,
		false,
		nil,
	)
}

func declareLikeTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		likeExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		likeQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return ch.QueueBind(
		q.Name,
		likeBindingKey,
		likeExchange,
		false,
		nil,
	)
}

func declareCommentTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		commentExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		commentQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return ch.QueueBind(
		q.Name,
		commentBindingKey,
		commentExchange,
		false,
		nil,
	)
}
