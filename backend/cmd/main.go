package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	appaccount "feedsystem_video_go/internal/application/account"
	appfeed "feedsystem_video_go/internal/application/feed"
	appsocial "feedsystem_video_go/internal/application/social"
	appvideo "feedsystem_video_go/internal/application/video"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	apphttp "feedsystem_video_go/internal/http"
	cachelib "feedsystem_video_go/internal/infrastructure/cache"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
	feedcache "feedsystem_video_go/internal/infrastructure/cache/redis/feed"
	videocache "feedsystem_video_go/internal/infrastructure/cache/redis/video"
	rabbitmq "feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
	accountmysql "feedsystem_video_go/internal/infrastructure/persistence/mysql"
	feedmysql "feedsystem_video_go/internal/infrastructure/persistence/mysql/feed"
	socialmysql "feedsystem_video_go/internal/infrastructure/persistence/mysql/social"
	videomysql "feedsystem_video_go/internal/infrastructure/persistence/mysql/video"
	"feedsystem_video_go/internal/infrastructure/security"
	feedworkers "feedsystem_video_go/internal/infrastructure/workers/feed"
	httpaccount "feedsystem_video_go/internal/interfaces/http/account"
	httpfeed "feedsystem_video_go/internal/interfaces/http/feed"
	httpsocial "feedsystem_video_go/internal/interfaces/http/social"
	httpvideo "feedsystem_video_go/internal/interfaces/http/video"
	"feedsystem_video_go/internal/interfaces/ws"
	"feedsystem_video_go/internal/observability"
)

// 加载配置
// 连接数据库
// 连接 Redis (可选，用于缓存)
// 连接 RabbitMQ (可选，用于消息队列)
// 启动 HTTP 服务器
// 启动 Pprof 服务器
func main() {
	// 加载配置
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		// 统一相对于 backend 工作目录解析，兼容本地 go run 与容器内 /app。
		configPath = "configs/config.yaml"
	}
	cfg, usedDefault, err := config.LoadLocalDev(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	if usedDefault {
		log.Printf("Config File %s not found, using default local config", configPath)
	} else {
		log.Printf("Config loaded from file: %s", configPath)
	}

	// 连接数据库
	sqlDB, err := db.NewDB(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	if err := db.AutoMigrate(sqlDB); err != nil {
		log.Fatalf("Failed to auto migrate database: %v", err)
	}
	defer db.CloseDB(sqlDB)

	// 连接 Redis (可选，用于缓存)
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Redis config error (cache disabled): %v", err)
		cache = nil
	} else {
		pingCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := cache.Ping(pingCtx); err != nil {
			log.Printf("Redis not available (cache disabled): %v", err)
			_ = cache.Close()
			cache = nil
		} else {
			defer cache.Close()
			log.Printf("Redis connected (cache enabled)")
		}
	}

	// 连接 RabbitMQ (可选，用于消息队列)
	rmq, err := rabbitmq.NewRabbitMQ(&cfg.RabbitMQ)
	if err != nil {
		log.Printf("RabbitMQ config error (disabled): %v", err)
		rmq = nil
	} else {
		defer rmq.Close()
		log.Printf("RabbitMQ connected")
	}

	// Pprof
	pprofServer, err := observability.NewPprofServer(
		"API",
		cfg.ObservabilityConfig.Pprof.Enabled,
		cfg.ObservabilityConfig.Pprof.ApiAddr,
	)
	if err != nil {
		log.Printf("Failed to start API pprof server: %v", err)
	}
	defer pprofServer.Close()

	// ============================================================
	// 组合根：账号
	// ============================================================
	tokens, err := security.NewJWTServiceFromEnv()
	if err != nil {
		log.Fatalf("Failed to initialize JWT service: %v", err)
	}
	accountRepository := accountmysql.NewAccountRepository(sqlDB)
	refreshTokenRepository := accountmysql.NewRefreshTokenRepository(sqlDB)
	accountApp := appaccount.NewService(
		accountRepository,
		refreshTokenRepository,
		tokens,
		security.NewBcryptHasher(),
	)
	accountHandler := httpaccount.NewHandler(accountApp)

	// ============================================================
	// 组合根：视频、点赞、评论
	// ============================================================
	videoRepository := videomysql.NewVideoRepository(sqlDB)
	popularityMQ, err := rabbitmq.NewPopularityMQ(rmq)
	if err != nil {
		log.Printf("PopularityMQ init failed (mq disabled): %v", err)
		popularityMQ = nil
	}

	bloom := cachelib.NewBloomFilter(cache, "bloom:video_ids", 0, 0)
	detailCache := videocache.NewDetailCache(cache, bloom)
	videoService := appvideo.NewVideoService(videoRepository, detailCache)
	if cache != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := videoService.RebuildBloom(ctx); err != nil {
				log.Printf("Rebuild video bloom filter failed: %v", err)
			}
		}()
	}
	videoHandler := httpvideo.NewVideoHandler(videoService)

	likeMQ, err := rabbitmq.NewLikeMQ(rmq)
	if err != nil {
		log.Printf("LikeMQ init failed (mq disabled): %v", err)
		likeMQ = nil
	}
	likeRepository := videomysql.NewLikeRepository(sqlDB)
	var likePublisher appvideo.LikeEventPublisher
	if likeMQ != nil {
		likePublisher = likeMQ
	}
	var popularityPublisher appvideo.PopularityUpdater
	if popularityMQ != nil {
		popularityPublisher = popularityMQ
	}
	popularityCache := videocache.NewPopularityCache(cache)
	likeService := appvideo.NewLikeService(likeRepository, videoRepository, likePublisher, popularityPublisher, popularityCache)
	likeHandler := httpvideo.NewLikeHandler(likeService)

	commentMQ, err := rabbitmq.NewCommentMQ(rmq)
	if err != nil {
		log.Printf("CommentMQ init failed (mq disabled): %v", err)
		commentMQ = nil
	}
	commentRepository := videomysql.NewCommentRepository(sqlDB)
	var commentPublisher appvideo.CommentEventPublisher
	if commentMQ != nil {
		commentPublisher = commentMQ
	}
	commentService := appvideo.NewCommentService(commentRepository, videoRepository, commentPublisher, popularityPublisher, popularityCache)
	commentHandler := httpvideo.NewCommentHandler(commentService)

	// ============================================================
	// 组合根：社交
	// ============================================================
	socialMQ, err := rabbitmq.NewSocialMQ(rmq)
	if err != nil {
		log.Printf("SocialMQ init failed (mq disabled): %v", err)
		socialMQ = nil
	}
	socialRepository := socialmysql.NewRepository(sqlDB)
	creatorStatsRepository := socialmysql.NewStatsRepository(sqlDB)
	var socialPublisher appsocial.EventPublisher
	if socialMQ != nil {
		socialPublisher = socialMQ
	}
	socialService := appsocial.NewSocialService(socialRepository, accountRepository, socialPublisher)
	socialHandler := httpsocial.NewSocialHandler(socialService)

	// ============================================================
	// 组合根：Feed
	// ============================================================
	feedRepository := feedmysql.NewRepository(sqlDB)
	feedService := appfeed.NewFeedService(
		feedRepository,
		likeRepository,
		feedcache.NewDetailCache(cache),
		feedcache.NewRankingCache(cache),
		rediscache.NewTimelineRepository(cache),
		creatorStatsRepository,
	)
	feedHandler := httpfeed.NewFeedHandler(feedService)

	// ============================================================
	// 组合根：时间线消费者
	// ============================================================
	timelineMQ, err := rabbitmq.NewTimelineMQ(rmq)
	if err != nil {
		log.Printf("timelineMQ init failed (mq disabled): %v", err)
		timelineMQ = nil
	}
	if timelineMQ != nil {
		feedworkers.StartOutboxPoller(videomysql.NewOutboxRepository(sqlDB), timelineMQ)
		feedworkers.StartConsumer(timelineMQ, rabbitmq.TimelineQueueName, cache)
	}

	// ============================================================
	// 组合根：WebSocket 通知
	// ============================================================
	wsHub := ws.NewHub()
	wsNotifier := ws.NewRedisNotifier(cache, wsHub)
	wsCtx, wsCancel := context.WithCancel(context.Background())
	defer wsCancel()
	go func() {
		if err := wsNotifier.Run(wsCtx); err != nil && err != context.Canceled {
			log.Printf("WebSocket notifier stopped: %v", err)
		}
	}()
	wsHandler := ws.NewHandler(tokens, wsHub)

	// ============================================================
	// 路由装配
	// ============================================================
	r := apphttp.SetRouter(apphttp.Dependencies{
		AccountHandler: accountHandler,
		VideoHandler:   videoHandler,
		LikeHandler:    likeHandler,
		CommentHandler: commentHandler,
		SocialHandler:  socialHandler,
		FeedHandler:    feedHandler,
		WSHandler:      wsHandler,
		Tokens:         tokens,
		Cache:          cache,
	})
	log.Printf("Server is running on port %d", cfg.Server.Port)
	if err := r.Run(":" + strconv.Itoa(cfg.Server.Port)); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
