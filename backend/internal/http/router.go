package http

import (
	"log"
	"time"

	"feedsystem_video_go/internal/middleware/idempotency"
	"feedsystem_video_go/internal/middleware/jwt"
	"feedsystem_video_go/internal/middleware/ratelimit"

	"github.com/gin-gonic/gin"
)

// SetRouter 只负责注册路由和接口层中间件。
// Repository、Service、Handler、MQ 的统一装配在 cmd/api/main.go 完成。
func SetRouter(deps Dependencies) *gin.Engine {
	r := gin.Default()
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Printf("SetTrustedProxies failed: %v", err)
	}
	r.MaxMultipartMemory = 256 << 20      // 256MB
	r.Static("/static", "./.run/uploads") // 静态文件映射
	if deps.WSHandler != nil {
		r.GET("/ws", deps.WSHandler.Handle)
	}

	cache := deps.Cache
	tokens := deps.Tokens

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

	publishIdempotency := idempotency.Require(cache, "video:publish", 24*time.Hour)
	likeIdempotency := idempotency.Require(cache, "video:like", 24*time.Hour)
	unlikeIdempotency := idempotency.Require(cache, "video:unlike", 24*time.Hour)
	commentIdempotency := idempotency.Require(cache, "video:comment", 24*time.Hour)

	// account
	accountGroup := r.Group("/account")
	{
		accountGroup.POST("/register", registerLimit, deps.AccountHandler.Register)
		accountGroup.POST("/login", loginLimit, deps.AccountHandler.Login)
		accountGroup.POST("/refresh", refreshLimit, deps.AccountHandler.Refresh)
		accountGroup.POST("/logout", jwt.JWTAuth(tokens), deps.AccountHandler.Logout)
		accountGroup.POST("/changePassword", jwt.JWTAuth(tokens), deps.AccountHandler.ChangePassword)
		accountGroup.POST("/rename", jwt.JWTAuth(tokens), deps.AccountHandler.Rename)
		accountGroup.POST("/findByID", readLimit, deps.AccountHandler.FindByID)
		accountGroup.POST("/findByUsername", readLimit, deps.AccountHandler.FindByUsername)
	}

	// video
	videoGroup := r.Group("/video")
	{
		videoGroup.POST("/listByAuthorID", deps.VideoHandler.ListByAuthorID)
		videoGroup.POST("/getDetail", deps.VideoHandler.GetDetail)
		videoGroup.POST("/uploadVideo", jwt.JWTAuth(tokens), uploadLimit, deps.VideoHandler.UploadVideo)
		videoGroup.POST("/uploadCover", jwt.JWTAuth(tokens), uploadLimit, deps.VideoHandler.UploadCover)
		videoGroup.POST("/publish", jwt.JWTAuth(tokens), publishLimit, publishIdempotency, deps.VideoHandler.PublishVideo)
	}

	// like
	likeGroup := r.Group("/like")
	{
		likeGroup.POST("/like", jwt.JWTAuth(tokens), likeLimit, likeIdempotency, deps.LikeHandler.Like)
		likeGroup.POST("/unlike", jwt.JWTAuth(tokens), likeLimit, unlikeIdempotency, deps.LikeHandler.Unlike)
		likeGroup.POST("/isLiked", jwt.JWTAuth(tokens), deps.LikeHandler.IsLiked)
		likeGroup.POST("/listMyLikedVideos", jwt.JWTAuth(tokens), deps.LikeHandler.ListMyLikedVideos)
	}

	// comment
	commentGroup := r.Group("/comment")
	{
		commentGroup.POST("/listAll", deps.CommentHandler.GetAllComments)
		commentGroup.POST("/publish", jwt.JWTAuth(tokens), commentLimit, commentIdempotency, deps.CommentHandler.PublishComment)
		commentGroup.POST("/delete", jwt.JWTAuth(tokens), deps.CommentHandler.DeleteComment)
	}

	// social
	socialGroup := r.Group("/social")
	{
		socialGroup.POST("/follow", jwt.JWTAuth(tokens), socialLimit, deps.SocialHandler.Follow)
		socialGroup.POST("/unfollow", jwt.JWTAuth(tokens), socialLimit, deps.SocialHandler.Unfollow)
		socialGroup.POST("/getAllFollowers", jwt.SoftJWTAuth(tokens), deps.SocialHandler.GetAllFollowers)
		socialGroup.POST("/getAllVloggers", jwt.SoftJWTAuth(tokens), deps.SocialHandler.GetAllVloggers)
	}

	// feed
	feedGroup := r.Group("/feed")
	{
		feedGroup.POST("/listLatest", jwt.SoftJWTAuth(tokens), feedLimit, deps.FeedHandler.ListLatest)
		feedGroup.POST("/listLikesCount", jwt.SoftJWTAuth(tokens), feedLimit, deps.FeedHandler.ListLikesCount)
		feedGroup.POST("/listByPopularity", jwt.SoftJWTAuth(tokens), feedLimit, deps.FeedHandler.ListByPopularity)
		feedGroup.POST("/listByFollowing", jwt.JWTAuth(tokens), feedLimit, deps.FeedHandler.ListByFollowing)
	}

	return r
}
