package bench

import (
	"encoding/json"
	"fmt"
	"time"
)

// LikeBenchmark 点赞模块压测
type LikeBenchmark struct {
	cfg   Config
	users []PreparedUser // 预置的用户列表
}

// NewLikeBenchmark 创建点赞压测实例
func NewLikeBenchmark(cfg Config, users []PreparedUser) *LikeBenchmark {
	return &LikeBenchmark{cfg: cfg, users: users}
}

// Run 执行点赞模块压测
func (b *LikeBenchmark) Run() []Stats {
	if len(b.users) == 0 {
		return []Stats{{Name: "Like (skipped: no users)"}}
	}

	var allStats []Stats

	// 准备测试视频
	videoIDs := b.prepareVideos()

	allStats = append(allStats, b.runLike(videoIDs))
	allStats = append(allStats, b.runUnlike(videoIDs))
	allStats = append(allStats, b.runIsLiked(videoIDs))
	allStats = append(allStats, b.runListMyLikedVideos())

	return allStats
}

func (b *LikeBenchmark) prepareVideos() []uint {
	// 使用第一个用户发布视频
	if len(b.users) == 0 {
		return nil
	}
	user := b.users[0]
	client := NewHTTPClientWithToken(b.cfg.BaseURL, user.Token)

	var videoIDs []uint
	for i := 0; i < 5; i++ {
		body := map[string]string{
			"title":       fmt.Sprintf("bench_video_%d", i),
			"description": "benchmark test video",
			"play_url":    "https://example.com/video.mp4",
			"cover_url":   "https://example.com/cover.jpg",
		}
		respBody, _, err := client.PostJSON("/video/publish", body)
		if err == nil {
			var video struct {
				ID uint `json:"id"`
			}
			json.Unmarshal(respBody, &video)
			if video.ID > 0 {
				videoIDs = append(videoIDs, video.ID)
			}
		}
	}
	return videoIDs
}

func (b *LikeBenchmark) runLike(videoIDs []uint) Stats {
	if len(videoIDs) == 0 {
		return Stats{Name: "Like.Like (skipped: no videos)"}
	}

	runner := NewRunner(b.cfg.LikeConcurrency)
	start := time.Now()
	videoIDsCopy := make([]uint, len(videoIDs))
	copy(videoIDsCopy, videoIDs)

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.LikeRequests / b.cfg.LikeConcurrency
		for i := 0; i < count; i++ {
			vid := videoIDsCopy[(workerID*count+i)%len(videoIDsCopy)]
			body := map[string]uint{"video_id": vid}
			_, lat, err := client.PostJSON("/like/like", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Like.Like", results, dur)
}

func (b *LikeBenchmark) runUnlike(videoIDs []uint) Stats {
	if len(videoIDs) == 0 {
		return Stats{Name: "Like.Unlike (skipped: no videos)"}
	}

	runner := NewRunner(b.cfg.LikeConcurrency)
	start := time.Now()
	videoIDsCopy := make([]uint, len(videoIDs))
	copy(videoIDsCopy, videoIDs)

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.LikeRequests / b.cfg.LikeConcurrency
		for i := 0; i < count; i++ {
			vid := videoIDsCopy[(workerID*count+i)%len(videoIDsCopy)]
			body := map[string]uint{"video_id": vid}
			_, lat, err := client.PostJSON("/like/unlike", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Like.Unlike", results, dur)
}

func (b *LikeBenchmark) runIsLiked(videoIDs []uint) Stats {
	if len(videoIDs) == 0 {
		return Stats{Name: "Like.IsLiked (skipped: no videos)"}
	}

	runner := NewRunner(b.cfg.LikeConcurrency)
	start := time.Now()
	videoIDsCopy := make([]uint, len(videoIDs))
	copy(videoIDsCopy, videoIDs)

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.LikeRequests / b.cfg.LikeConcurrency
		for i := 0; i < count; i++ {
			vid := videoIDsCopy[(workerID*count+i)%len(videoIDsCopy)]
			body := map[string]uint{"video_id": vid}
			_, lat, err := client.PostJSON("/like/isLiked", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Like.IsLiked", results, dur)
}

func (b *LikeBenchmark) runListMyLikedVideos() Stats {
	runner := NewRunner(b.cfg.LikeConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.LikeRequests / b.cfg.LikeConcurrency
		for i := 0; i < count; i++ {
			_, lat, err := client.PostJSON("/like/listMyLikedVideos", nil)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Like.ListMyLikedVideos", results, dur)
}