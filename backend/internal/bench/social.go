package bench

import (
	"encoding/json"
	"fmt"
	"time"
)

// SocialBenchmark 关注模块压测
type SocialBenchmark struct {
	cfg   Config
	users []PreparedUser
}

// NewSocialBenchmark 创建关注压测实例
func NewSocialBenchmark(cfg Config, users []PreparedUser) *SocialBenchmark {
	return &SocialBenchmark{cfg: cfg, users: users}
}

// Run 执行关注模块压测
func (b *SocialBenchmark) Run() []Stats {
	if len(b.users) < 2 {
		return []Stats{{Name: "Social (skipped: need >= 2 users)"}}
	}

	var allStats []Stats

	allStats = append(allStats, b.runFollow())
	allStats = append(allStats, b.runUnfollow())
	allStats = append(allStats, b.runGetAllFollowers())
	allStats = append(allStats, b.runGetAllVloggers())

	return allStats
}

func (b *SocialBenchmark) runFollow() Stats {
	runner := NewRunner(b.cfg.SocialConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		// 每个 worker 关注不同用户
		followerIdx := workerID % len(b.users)
		vloggerIdx := (workerID + 1) % len(b.users)
		if followerIdx == vloggerIdx {
			vloggerIdx = (vloggerIdx + 1) % len(b.users)
		}

		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[followerIdx].Token)
		var res []Result
		count := b.cfg.SocialRequests / b.cfg.SocialConcurrency
		for i := 0; i < count; i++ {
			body := map[string]uint{"vlogger_id": b.users[vloggerIdx].ID}
			_, lat, err := client.PostJSON("/social/follow", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Social.Follow", results, dur)
}

func (b *SocialBenchmark) runUnfollow() Stats {
	runner := NewRunner(b.cfg.SocialConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		followerIdx := workerID % len(b.users)
		vloggerIdx := (workerID + 1) % len(b.users)
		if followerIdx == vloggerIdx {
			vloggerIdx = (vloggerIdx + 1) % len(b.users)
		}

		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[followerIdx].Token)
		var res []Result
		count := b.cfg.SocialRequests / b.cfg.SocialConcurrency
		for i := 0; i < count; i++ {
			body := map[string]uint{"vlogger_id": b.users[vloggerIdx].ID}
			_, lat, err := client.PostJSON("/social/unfollow", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Social.Unfollow", results, dur)
}

func (b *SocialBenchmark) runGetAllFollowers() Stats {
	runner := NewRunner(b.cfg.SocialConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.SocialRequests / b.cfg.SocialConcurrency
		for i := 0; i < count; i++ {
			_, lat, err := client.PostJSON("/social/getAllFollowers", nil)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Social.GetAllFollowers", results, dur)
}

func (b *SocialBenchmark) runGetAllVloggers() Stats {
	runner := NewRunner(b.cfg.SocialConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.SocialRequests / b.cfg.SocialConcurrency
		for i := 0; i < count; i++ {
			_, lat, err := client.PostJSON("/social/getAllVloggers", nil)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Social.GetAllVloggers", results, dur)
}

// VideoBenchmark 视频模块压测
type VideoBenchmark struct {
	cfg   Config
	users []PreparedUser
}

// NewVideoBenchmark 创建视频压测实例
func NewVideoBenchmark(cfg Config, users []PreparedUser) *VideoBenchmark {
	return &VideoBenchmark{cfg: cfg, users: users}
}

// Run 执行视频模块压测
func (b *VideoBenchmark) Run() []Stats {
	if len(b.users) == 0 {
		return []Stats{{Name: "Video (skipped: no users)"}}
	}

	var allStats []Stats

	// 先发布一些视频
	videoIDs := b.prepareVideos()

	allStats = append(allStats, b.runPublish())
	allStats = append(allStats, b.runGetDetail(videoIDs))
	allStats = append(allStats, b.runListByAuthorID())

	return allStats
}

func (b *VideoBenchmark) prepareVideos() []uint {
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

func (b *VideoBenchmark) runPublish() Stats {
	runner := NewRunner(b.cfg.VideoConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.VideoRequests / b.cfg.VideoConcurrency
		for i := 0; i < count; i++ {
			body := map[string]string{
				"title":       fmt.Sprintf("bench_pub_%d_%d", workerID, i),
				"description": "benchmark publish test",
				"play_url":    "https://example.com/video.mp4",
				"cover_url":   "https://example.com/cover.jpg",
			}
			_, lat, err := client.PostJSON("/video/publish", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Video.Publish", results, dur)
}

func (b *VideoBenchmark) runGetDetail(videoIDs []uint) Stats {
	if len(videoIDs) == 0 {
		return Stats{Name: "Video.GetDetail (skipped: no videos)"}
	}

	runner := NewRunner(b.cfg.VideoConcurrency)
	start := time.Now()
	videoIDsCopy := make([]uint, len(videoIDs))
	copy(videoIDsCopy, videoIDs)

	results := runner.Run(func(workerID int) []Result {
		client := NewHTTPClient(b.cfg.BaseURL)
		var res []Result
		count := b.cfg.VideoRequests / b.cfg.VideoConcurrency
		for i := 0; i < count; i++ {
			vid := videoIDsCopy[(workerID*count+i)%len(videoIDsCopy)]
			body := map[string]uint{"id": vid}
			_, lat, err := client.PostJSON("/video/getDetail", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Video.GetDetail", results, dur)
}

func (b *VideoBenchmark) runListByAuthorID() Stats {
	runner := NewRunner(b.cfg.VideoConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClient(b.cfg.BaseURL)
		var res []Result
		count := b.cfg.VideoRequests / b.cfg.VideoConcurrency
		for i := 0; i < count; i++ {
			body := map[string]uint{"author_id": b.users[userIdx].ID}
			_, lat, err := client.PostJSON("/video/listByAuthorID", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Video.ListByAuthorID", results, dur)
}