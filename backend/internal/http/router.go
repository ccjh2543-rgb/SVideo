package http

import (
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/feed"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	"log"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// MockAccountID 是一个用于压测环境的中间件，跳过 JWT 鉴权，
// 统一注入 accountID=1（测试用户），让后续 handler 能正常获取到用户身份。
// 如需测试多个用户，可在请求 Header 中设置 X-Account-Id。
func MockAccountID(c *gin.Context) {
	accountID := uint(1)
	if v := c.GetHeader("X-Account-Id"); v != "" {
		if id, err := strconv.ParseUint(v, 10, 64); err == nil {
			accountID = uint(id)
		}
	}
	c.Set("accountID", accountID)
	c.Next()
}

func SetRouter(db *gorm.DB, cache *rediscache.Client, rmq *rabbitmq.RabbitMQ) *gin.Engine {

	r := gin.Default()
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Printf("SetTrustedProxies failed: %v", err)
	}
	r.MaxMultipartMemory = 256 << 20      //256MB
	r.Static("/static", "./.run/uploads") // 静态文件映射

	// account
	accountRepository := account.NewAccountRepository(db)
	accountService := account.NewAccountService(accountRepository, cache)
	accountHandler := account.NewAccountHandler(accountService)
	accountGroup := r.Group("/account")
	{
		accountGroup.POST("/register", accountHandler.CreateAccount)
		accountGroup.POST("/login", accountHandler.Login)
		accountGroup.POST("/changePassword", accountHandler.ChangePassword)
		accountGroup.POST("/findByID", accountHandler.FindByID)
		accountGroup.POST("/findByUsername", accountHandler.FindByUsername)
		accountGroup.POST("/logout", accountHandler.Logout)
		accountGroup.POST("/rename", accountHandler.Rename)
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
		videoGroup.POST("/uploadVideo", MockAccountID, videoHandler.UploadVideo)
		videoGroup.POST("/uploadCover", MockAccountID, videoHandler.UploadCover)
		videoGroup.POST("/publish", MockAccountID, videoHandler.PublishVideo)
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
		likeGroup.POST("/like", MockAccountID, likeHandler.Like)
		likeGroup.POST("/unlike", MockAccountID, likeHandler.Unlike)
		likeGroup.POST("/isLiked", MockAccountID, likeHandler.IsLiked)
		likeGroup.POST("/listMyLikedVideos", MockAccountID, likeHandler.ListMyLikedVideos)
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
		commentGroup.POST("/publish", MockAccountID, commentHandler.PublishComment)
		commentGroup.POST("/delete", MockAccountID, commentHandler.DeleteComment)
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
		socialGroup.POST("/follow", MockAccountID, socialHandler.Follow)
		socialGroup.POST("/unfollow", MockAccountID, socialHandler.Unfollow)
		socialGroup.POST("/getAllFollowers", MockAccountID, socialHandler.GetAllFollowers)
		socialGroup.POST("/getAllVloggers", MockAccountID, socialHandler.GetAllVloggers)
	}

	// feed
	feedRepository := feed.NewFeedRepository(db)
	feedService := feed.NewFeedService(feedRepository, likeRepository, cache)
	feedHandler := feed.NewFeedHandler(feedService)
	feedGroup := r.Group("/feed")
	{
		feedGroup.POST("/listLatest", feedHandler.ListLatest)
		feedGroup.POST("/listLikesCount", feedHandler.ListLikesCount)
		feedGroup.POST("/listByPopularity", feedHandler.ListByPopularity)
		feedGroup.POST("/listByFollowing", feedHandler.ListByFollowing)
	}

	//worker
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
