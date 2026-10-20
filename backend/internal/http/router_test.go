package http

import "testing"

// TestRoutesRegistered 保证重构过程中路由不会意外丢失。
// 这里只检查路由是否注册，不执行 Handler，因此可以使用空 Dependencies。
func TestRoutesRegistered(t *testing.T) {
	r := SetRouter(Dependencies{})

	registered := make(map[string]struct{}, len(r.Routes()))
	for _, route := range r.Routes() {
		registered[route.Method+" "+route.Path] = struct{}{}
	}

	expected := []string{
		"POST /account/register",
		"POST /account/login",
		"POST /account/refresh",
		"POST /account/logout",
		"POST /account/changePassword",
		"POST /account/rename",
		"POST /account/findByID",
		"POST /account/findByUsername",
		"POST /video/listByAuthorID",
		"POST /video/getDetail",
		"POST /video/uploadVideo",
		"POST /video/uploadCover",
		"POST /video/publish",
		"POST /like/like",
		"POST /like/unlike",
		"POST /like/isLiked",
		"POST /like/listMyLikedVideos",
		"POST /comment/listAll",
		"POST /comment/publish",
		"POST /comment/delete",
		"POST /social/follow",
		"POST /social/unfollow",
		"POST /social/getAllFollowers",
		"POST /social/getAllVloggers",
		"POST /feed/listLatest",
		"POST /feed/listLikesCount",
		"POST /feed/listByPopularity",
		"POST /feed/listByFollowing",
	}
	for _, route := range expected {
		if _, ok := registered[route]; !ok {
			t.Errorf("route %s is not registered", route)
		}
	}
}
