package bench

import (
	"encoding/json"
	"fmt"
	"time"
)

// CommentBenchmark 评论模块压测
type CommentBenchmark struct {
	cfg   Config
	users []PreparedUser
}

// NewCommentBenchmark 创建评论压测实例
func NewCommentBenchmark(cfg Config, users []PreparedUser) *CommentBenchmark {
	return &CommentBenchmark{cfg: cfg, users: users}
}

// Run 执行评论模块压测
func (b *CommentBenchmark) Run() []Stats {
	if len(b.users) == 0 {
		return []Stats{{Name: "Comment (skipped: no users)"}}
	}

	var allStats []Stats

	// 准备测试视频
	videoIDs := b.prepareVideos()

	allStats = append(allStats, b.runPublishComment(videoIDs))
	allStats = append(allStats, b.runDeleteComment(videoIDs))
	allStats = append(allStats, b.runListAllComments(videoIDs))

	return allStats
}

func (b *CommentBenchmark) prepareVideos() []uint {
	if len(b.users) == 0 {
		return nil
	}
	user := b.users[0]
	client := NewHTTPClientWithToken(b.cfg.BaseURL, user.Token)

	var videoIDs []uint
	for i := 0; i < 3; i++ {
		body := map[string]string{
			"title":       fmt.Sprintf("bench_comment_video_%d", i),
			"description": "benchmark comment test",
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

func (b *CommentBenchmark) runPublishComment(videoIDs []uint) Stats {
	if len(videoIDs) == 0 {
		return Stats{Name: "Comment.Publish (skipped: no videos)"}
	}

	runner := NewRunner(b.cfg.CommentConcurrency)
	start := time.Now()
	videoIDsCopy := make([]uint, len(videoIDs))
	copy(videoIDsCopy, videoIDs)

	results := runner.Run(func(workerID int) []Result {
		userIdx := workerID % len(b.users)
		client := NewHTTPClientWithToken(b.cfg.BaseURL, b.users[userIdx].Token)
		var res []Result
		count := b.cfg.CommentRequests / b.cfg.CommentConcurrency
		for i := 0; i < count; i++ {
			vid := videoIDsCopy[(workerID*count+i)%len(videoIDsCopy)]
			body := map[string]interface{}{
				"video_id": vid,
				"content":  fmt.Sprintf("bench comment %d from worker %d", i, workerID),
			}
			_, lat, err := client.PostJSON("/comment/publish", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Comment.Publish", results, dur)
}

func (b *CommentBenchmark) runDeleteComment(videoIDs []uint) Stats {
	if len(videoIDs) == 0 {
		return Stats{Name: "Comment.Delete (skipped: no videos)"}
	}

	// 先发布一些评论用于删除
	user := b.users[0]
	client := NewHTTPClientWithToken(b.cfg.BaseURL, user.Token)
	var commentIDs []uint
	for i := 0; i < 10; i++ {
		body := map[string]interface{}{
			"video_id": videoIDs[0],
			"content":  fmt.Sprintf("to_delete_%d", i),
		}
		_, _, err := client.PostJSON("/comment/publish", body)
		_ = err // 忽略错误，用已有的评论
	}

	// 列出所有评论获取 ID
	respBody, _, err := client.PostJSON("/comment/listAll", map[string]uint{"video_id": videoIDs[0]})
	if err == nil {
		var comments []struct {
			ID uint `json:"id"`
		}
		json.Unmarshal(respBody, &comments)
		for _, c := range comments {
			commentIDs = append(commentIDs, c.ID)
		}
	}

	if len(commentIDs) == 0 {
		return Stats{Name: "Comment.Delete (skipped: no comments)"}
	}

	runner := NewRunner(b.cfg.CommentConcurrency)
	start := time.Now()
	commentIDsCopy := make([]uint, len(commentIDs))
	copy(commentIDsCopy, commentIDs)

	results := runner.Run(func(workerID int) []Result {
		// 只有发布评论的用户才能删除
		client := NewHTTPClientWithToken(b.cfg.BaseURL, user.Token)
		var res []Result
		count := b.cfg.CommentRequests / b.cfg.CommentConcurrency
		for i := 0; i < count; i++ {
			cid := commentIDsCopy[(workerID*count+i)%len(commentIDsCopy)]
			body := map[string]uint{"comment_id": cid}
			_, lat, err := client.PostJSON("/comment/delete", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Comment.Delete", results, dur)
}

func (b *CommentBenchmark) runListAllComments(videoIDs []uint) Stats {
	if len(videoIDs) == 0 {
		return Stats{Name: "Comment.ListAll (skipped: no videos)"}
	}

	runner := NewRunner(b.cfg.CommentConcurrency)
	start := time.Now()
	videoIDsCopy := make([]uint, len(videoIDs))
	copy(videoIDsCopy, videoIDs)

	results := runner.Run(func(workerID int) []Result {
		client := NewHTTPClient(b.cfg.BaseURL)
		var res []Result
		count := b.cfg.CommentRequests / b.cfg.CommentConcurrency
		for i := 0; i < count; i++ {
			vid := videoIDsCopy[(workerID*count+i)%len(videoIDsCopy)]
			body := map[string]uint{"video_id": vid}
			_, lat, err := client.PostJSON("/comment/listAll", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Comment.ListAll", results, dur)
}