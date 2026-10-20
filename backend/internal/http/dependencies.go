package http

import (
	domainaccount "feedsystem_video_go/internal/domain/account"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
	httpaccount "feedsystem_video_go/internal/interfaces/http/account"
	httpfeed "feedsystem_video_go/internal/interfaces/http/feed"
	httpsocial "feedsystem_video_go/internal/interfaces/http/social"
	httpvideo "feedsystem_video_go/internal/interfaces/http/video"
	"feedsystem_video_go/internal/interfaces/ws"
)

// Dependencies HTTP 层依赖集合。
// 所有 Repository、Service、Handler 的创建都在 cmd/api 组合根完成，
// Router 只负责注册路由，不再承担对象构造职责。
type Dependencies struct {
	AccountHandler *httpaccount.Handler
	VideoHandler   *httpvideo.VideoHandler
	LikeHandler    *httpvideo.LikeHandler
	CommentHandler *httpvideo.CommentHandler
	SocialHandler  *httpsocial.SocialHandler
	FeedHandler    *httpfeed.FeedHandler
	WSHandler      *ws.Handler

	Tokens domainaccount.TokenService
	Cache  *rediscache.Client
}
