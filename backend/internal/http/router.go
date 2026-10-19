package http

import (
	"log"

	"feedsystem_video_go/internal/account"
	appaccount "feedsystem_video_go/internal/application/account"
	domainaccount "feedsystem_video_go/internal/domain/account"
	"feedsystem_video_go/internal/feed"
	httpaccount "feedsystem_video_go/internal/interfaces/http/account"
	"feedsystem_video_go/internal/middleware/jwt"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/middleware/ratelimit"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetRouter 组装 HTTP 路由。
// accountApp 与 tokens 由 main 作为组合根注入，路由层不负责创建基础设施。
func SetRouter(
	db *gorm.DB,
	cache *rediscache.Client,
	rmq *rabbitmq.RabbitMQ,
	accountApp *appaccount.Service,
	tokens domainaccount.TokenService,
) *gin.Engine {
	r := gin.Default()
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Printf("SetTrustedProxies failed: %v", err)
	}
	r.MaxMultipartMemory = 256 << 20      // 256MB
	r.Static("/static", "./.run/uploads") // 静态文件映射

	// 通用限流器：登录/注册按 IP，业务写操作按账号，Feed 按账号或 IP。
	registerLimit := ratelimit.TokenBucket(cache, "account:register", ratelimit.TokenBucketConfig{
		Capacity: 10, RefillPerSecond: 0.2,
	}, ratelimit.KeyByIP)
	loginLimit := ratelimit.TokenBucket(cache, "account:login", ratelimit.TokenBucketConfig{
		Capacity: 10, RefillPerSecond: 1,
	}, ratelimit.KeyByIP)
	refreshLimit := ratelimit.TokenBucket(cache, "account:refresh", ratelimit.TokenBucketConfig{
		Capacity: 10, RefillPerSecond: 1,
	}, ratelimit.KeyByIP)
	readLimit := ratelimit.TokenBucket(cache, "account:read", ratelimit.TokenBucketConfig{
		Capacity: 30, RefillPerSecond: 5,
	}, ratelimit.KeyByIP)
	uploadLimit := ratelimit.TokenBucket(cache, "video:upload", ratelimit.TokenBucketConfig{
		Capacity: 5, RefillPerSecond: 0.2,
	}, ratelimit.KeyByAccount)
	publishLimit := ratelimit.TokenBucket(cache, "video:publish", ratelimit.TokenBucketConfig{
		Capacity: 5, RefillPerSecond: 0.2,
	}, ratelimit.KeyByAccount)
	likeLimit := ratelimit.TokenBucket(cache, "video:like", ratelimit.TokenBucketConfig{
		Capacity: 30, RefillPerSecond: 5,
	}, ratelimit.KeyByAccount)
	commentLimit := ratelimit.TokenBucket(cache, "video:comment", ratelimit.TokenBucketConfig{
		Capacity: 10, RefillPerSecond: 0.5,
	}, ratelimit.KeyByAccount)
	socialLimit := ratelimit.TokenBucket(cache, "social:write", ratelimit.TokenBucketConfig{
		Capacity: 10, RefillPerSecond: 0.5,
	}, ratelimit.KeyByAccount)
	feedLimit := ratelimit.TokenBucket(cache, "feed:read", ratelimit.TokenBucketConfig{
		Capacity: 30, RefillPerSecond: 10,
	}, ratelimit.KeyByAccountOrIP)

	// account
	accountRepository := account.NewAccountRepository(db)
	accountService := account.NewAccountService(accountRepository, cache)
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

	// video
	videoRepository := video.NewVideoRepository(db)
	popularityMQ, err := rabbitmq.NewPopularityMQ(rmq)
	if err != nil {
		log.Printf("PopularityMQ init failed (mq disabled): %v", err)
		popularityMQ = nil
	}
	videoService := video.NewVideoService(videoRepository, cache, popularityMQ)
	videoHandler := video.NewVideoHandler(videoService, accountService)
	videoGroup := r.Group("/video")
	{
		videoGroup.POST("/listByAuthorID", videoHandler.ListByAuthorID)
		videoGroup.POST("/getDetail", videoHandler.GetDetail)
		videoGroup.POST("/uploadVideo", jwt.JWTAuth(tokens), uploadLimit, videoHandler.UploadVideo)
		videoGroup.POST("/uploadCover", jwt.JWTAuth(tokens), uploadLimit, videoHandler.UploadCover)
		videoGroup.POST("/publish", jwt.JWTAuth(tokens), publishLimit, videoHandler.PublishVideo)
	}

	// like
	likeMQ, err := rabbitmq.NewLikeMQ(rmq)
	if err != nil {
		log.Printf("LikeMQ init failed (mq disabled): %v", err)
		likeMQ = nil
	}
	likeRepository := video.NewLikeRepository(db)
	likeService := video.NewLikeService(likeRepository, videoRepository, cache, likeMQ, popularityMQ)
	likeHandler := video.NewLikeHandler(likeService)
	likeGroup := r.Group("/like")
	{
		likeGroup.POST("/like", jwt.JWTAuth(tokens), likeLimit, likeHandler.Like)
		likeGroup.POST("/unlike", jwt.JWTAuth(tokens), likeLimit, likeHandler.Unlike)
		likeGroup.POST("/isLiked", jwt.JWTAuth(tokens), likeHandler.IsLiked)
		likeGroup.POST("/listMyLikedVideos", jwt.JWTAuth(tokens), likeHandler.ListMyLikedVideos)
	}

	// comment
	commentRepository := video.NewCommentRepository(db)
	commentMQ, err := rabbitmq.NewCommentMQ(rmq)
	if err != nil {
		log.Printf("CommentMQ init failed (mq disabled): %v", err)
		commentMQ = nil
	}
	commentService := video.NewCommentService(commentRepository, videoRepository, cache, commentMQ, popularityMQ)
	commentHandler := video.NewCommentHandler(commentService, accountService)
	commentGroup := r.Group("/comment")
	{
		commentGroup.POST("/listAll", commentHandler.GetAllComments)
		commentGroup.POST("/publish", jwt.JWTAuth(tokens), commentLimit, commentHandler.PublishComment)
		commentGroup.POST("/delete", jwt.JWTAuth(tokens), commentHandler.DeleteComment)
	}

	// social
	socialMQ, err := rabbitmq.NewSocialMQ(rmq)
	if err != nil {
		log.Printf("SocialMQ init failed (mq disabled): %v", err)
		socialMQ = nil
	}
	socialRepository := social.NewSocialRepository(db)
	socialService := social.NewSocialService(socialRepository, accountRepository, socialMQ)
	socialHandler := social.NewSocialHandler(socialService)
	socialGroup := r.Group("/social")
	{
		socialGroup.POST("/follow", jwt.JWTAuth(tokens), socialLimit, socialHandler.Follow)
		socialGroup.POST("/unfollow", jwt.JWTAuth(tokens), socialLimit, socialHandler.Unfollow)
		socialGroup.POST("/getAllFollowers", jwt.SoftJWTAuth(tokens), socialHandler.GetAllFollowers)
		socialGroup.POST("/getAllVloggers", jwt.SoftJWTAuth(tokens), socialHandler.GetAllVloggers)
	}

	// feed
	feedRepository := feed.NewFeedRepository(db)
	feedService := feed.NewFeedService(feedRepository, likeRepository, cache)
	feedHandler := feed.NewFeedHandler(feedService)
	feedGroup := r.Group("/feed")
	{
		feedGroup.POST("/listLatest", jwt.SoftJWTAuth(tokens), feedLimit, feedHandler.ListLatest)
		feedGroup.POST("/listLikesCount", jwt.SoftJWTAuth(tokens), feedLimit, feedHandler.ListLikesCount)
		feedGroup.POST("/listByPopularity", jwt.SoftJWTAuth(tokens), feedLimit, feedHandler.ListByPopularity)
		feedGroup.POST("/listByFollowing", jwt.JWTAuth(tokens), feedLimit, feedHandler.ListByFollowing)
	}

	// worker
	timelineMQ, err := rabbitmq.NewTimelineMQ(rmq)
	if err != nil {
		log.Printf("timelineMQ init failed (mq disabled): %v", err)
		timelineMQ = nil
	}
	if timelineMQ != nil {
		worker.StartOutboxPoller(db, timelineMQ)
		worker.StartConsumer(timelineMQ, "video.timeline.update.queue", cache)
	}
	return r
}
