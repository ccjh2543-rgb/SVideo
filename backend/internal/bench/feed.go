package bench

import (
	"fmt"
	"time"
)

// FeedBenchmark Feed 流模块压测
type FeedBenchmark struct {
	cfg   Config
	users []PreparedUser
}

// NewFeedBenchmark 创建 Feed 流压测实例
func NewFeedBenchmark(cfg Config, users []PreparedUser) *FeedBenchmark {
	return &FeedBenchmark{cfg: cfg, users: users}
}

// Run 执行 Feed 流模块压测
func (b *FeedBenchmark) Run() []Stats {
	var allStats []Stats

	allStats = append(allStats, b.runListLatest())
	allStats = append(allStats, b.runListLikesCount())
	allStats = append(allStats, b.runListByPopularity())

	if len(b.users) > 0 {
		allStats = append(allStats, b.runListByFollowing())
	}

	return allStats
}

func (b *FeedBenchmark) runListLatest() Stats {
	runner := NewRunner(b.cfg.FeedConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		client := NewHTTPClient(b.cfg.BaseURL)
		// 如果有用户，使用用户的 token 以获得 is_liked 信息
		if len(b.users) > 0 {
			userIdx := workerID % len(b.users)
			client.SetToken(b.users[userIdx].Token)
		}

		var res []Result
		count := b.cfg.FeedRequests / b.cfg.FeedConcurrency
		latestTime := int64(0)
		for i := 0; i < count; i++ {
			body := map[string]interface{}{
				"limit":       10,
				"latest_time": latestTime,
			}
			_, lat, err := client.PostJSON("/feed/listLatest", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Feed.ListLatest", results, dur)
}

func (b *FeedBenchmark) runListLikesCount() Stats {
	runner := NewRunner(b.cfg.FeedConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		client := NewHTTPClient(b.cfg.BaseURL)
		if len(b.users) > 0 {
			userIdx := workerID % len(b.users)
			client.SetToken(b.users[userIdx].Token)
		}

		var res []Result
		count := b.cfg.FeedRequests / b.cfg.FeedConcurrency
		for i := 0; i < count; i++ {
			body := map[string]interface{}{
				"limit": 10,
			}
			_, lat, err := client.PostJSON("/feed/listLikesCount", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Feed.ListLikesCount", results, dur)
}

func (b *FeedBenchmark) runListByPopularity() Stats {
	runner := NewRunner(b.cfg.FeedConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		client := NewHTTPClient(b.cfg.BaseURL)
		if len(b.users) > 0 {
			userIdx := workerID % len(b.users)
			client.SetToken(b.users[userIdx].Token)
		}

		var res []Result
		count := b.cfg.FeedRequests / b.cfg.FeedConcurrency
		for i := 0; i < count; i++ {
			body := map[string]interface{}{
				"limit": 10,
			}
			_, lat, err := client.PostJSON("/feed/listByPopularity", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Feed.ListByPopularity", results, dur)
}

func (b *FeedBenchmark) runListByFollowing() Stats {
	if len(b.users) == 0 {
		return Stats{Name: "Feed.ListByFollowing (skipped: no users)"}
	}

	runner := NewRunner(b.cfg.FeedConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.FeedRequests / b.cfg.FeedConcurrency
		for i := 0; i < count; i++ {
			body := map[string]interface{}{
				"limit": 10,
			}
			_, lat, err := client.PostJSON("/feed/listByFollowing", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Feed.ListByFollowing", results, dur)
}

// ScenarioBenchmark 真实场景混合压测：模拟一个用户完整的操作流程
type ScenarioBenchmark struct {
	cfg   Config
	users []PreparedUser
}

// NewScenarioBenchmark 创建场景混合压测实例
func NewScenarioBenchmark(cfg Config, users []PreparedUser) *ScenarioBenchmark {
	return &ScenarioBenchmark{cfg: cfg, users: users}
}

// Run 执行混合场景压测
func (b *ScenarioBenchmark) Run() []Stats {
	if len(b.users) == 0 {
		return []Stats{{Name: "Scenario (skipped: no users)"}}
	}

	var allStats []Stats

	allStats = append(allStats, b.runReadOnlyScenario())
	allStats = append(allStats, b.runWriteScenario())

	return allStats
}

// runReadOnlyScenario 模拟"刷视频"场景：浏览 Feed + 查看详情 + 判断是否点赞
func (b *ScenarioBenchmark) runReadOnlyScenario() Stats {
	runner := NewRunner(b.cfg.FeedConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.FeedRequests / b.cfg.FeedConcurrency
		for i := 0; i < count; i++ {
			// 1. 拉取 Feed 流
			_, lat, err := client.PostJSON("/feed/listLatest", map[string]interface{}{
				"limit": 10,
			})
			res = append(res, Result{Latency: lat, Error: err, Name: "feed"})

			// 2. 获取作者信息
			_, lat, err = client.PostJSON("/account/findByID", map[string]uint{
				"id": b.users[userIdx].ID,
			})
			res = append(res, Result{Latency: lat, Error: err, Name: "account"})

			// 3. 查看热门
			_, lat, err = client.PostJSON("/feed/listByPopularity", map[string]interface{}{
				"limit": 10,
			})
			res = append(res, Result{Latency: lat, Error: err, Name: "popularity"})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Scenario.ReadOnly", results, dur)
}

// runWriteScenario 模拟"互动"场景：点赞 + 评论 + 关注
func (b *ScenarioBenchmark) runWriteScenario() Stats {
	runner := NewRunner(b.cfg.LikeConcurrency)
	start := time.Now()

	results := runner.Run(func(workerID int) []Result {
		followerIdx := workerID % len(b.users)
		vloggerIdx := (workerID + 1) % len(b.users)
		if followerIdx == vloggerIdx {
			vloggerIdx = (vloggerIdx + 1) % len(b.users)
		}

		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[followerIdx].Token)
		var res []Result
		count := b.cfg.LikeRequests / b.cfg.LikeConcurrency
		for i := 0; i < count; i++ {
			vid := uint(workerID*100 + i + 1)

			// 1. 点赞
			_, lat, err := client.PostJSON("/like/like", map[string]uint{"video_id": vid})
			res = append(res, Result{Latency: lat, Error: err, Name: "like"})

			// 2. 评论
			_, lat, err = client.PostJSON("/comment/publish", map[string]interface{}{
				"video_id": vid,
				"content":  fmt.Sprintf("scenario comment %d", i),
			})
			res = append(res, Result{Latency: lat, Error: err, Name: "comment"})

			// 3. 关注作者
			_, lat, err = client.PostJSON("/social/follow", map[string]uint{
				"vlogger_id": b.users[vloggerIdx].ID,
			})
			res = append(res, Result{Latency: lat, Error: err, Name: "social"})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Scenario.Write", results, dur)
}