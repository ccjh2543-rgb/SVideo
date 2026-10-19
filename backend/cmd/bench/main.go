package main

import (
	"flag"
	"fmt"
	"os"

	"feedsystem_video_go/internal/bench"
)

func main() {
	cfg := bench.DefaultConfig()

	flag.StringVar(&cfg.BaseURL, "url", cfg.BaseURL, "目标服务器 URL，如 http://localhost:8080")
	flag.IntVar(&cfg.AccountConcurrency, "account-c", cfg.AccountConcurrency, "账号模块并发数")
	flag.IntVar(&cfg.AccountRequests, "account-n", cfg.AccountRequests, "账号模块请求总数")
	flag.IntVar(&cfg.VideoConcurrency, "video-c", cfg.VideoConcurrency, "视频模块并发数")
	flag.IntVar(&cfg.VideoRequests, "video-n", cfg.VideoRequests, "视频模块请求总数")
	flag.IntVar(&cfg.LikeConcurrency, "like-c", cfg.LikeConcurrency, "点赞模块并发数")
	flag.IntVar(&cfg.LikeRequests, "like-n", cfg.LikeRequests, "点赞模块请求总数")
	flag.IntVar(&cfg.CommentConcurrency, "comment-c", cfg.CommentConcurrency, "评论模块并发数")
	flag.IntVar(&cfg.CommentRequests, "comment-n", cfg.CommentRequests, "评论模块请求总数")
	flag.IntVar(&cfg.SocialConcurrency, "social-c", cfg.SocialConcurrency, "关注模块并发数")
	flag.IntVar(&cfg.SocialRequests, "social-n", cfg.SocialRequests, "关注模块请求总数")
	flag.IntVar(&cfg.FeedConcurrency, "feed-c", cfg.FeedConcurrency, "Feed 模块并发数")
	flag.IntVar(&cfg.FeedRequests, "feed-n", cfg.FeedRequests, "Feed 模块请求总数")

	module := flag.String("module", "all", "压测模块: all|account|video|like|comment|social|feed|scenario")

	flag.Parse()

	fmt.Printf("╔══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║          FeedSystem Video Benchmark Tool v1.0                ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Target: %-50s ║\n", cfg.BaseURL)
	fmt.Printf("║  Module: %-50s ║\n", *module)
	fmt.Printf("╚══════════════════════════════════════════════════════════════╝\n")

	// 检查服务健康状态
	checkHealth(cfg.BaseURL)

	var allStats []bench.Stats

	switch *module {
	case "all":
		// 先准备用户数据
		fmt.Println("\n>>> Phase 1: Preparing test users...")
		accBench := bench.NewAccountBenchmark(cfg)
		users := accBench.PrepareBenchUsers(20)

		fmt.Println("\n>>> Phase 2: Running all benchmarks...")
		allStats = append(allStats, runAll(cfg, users)...)
	case "account":
		allStats = append(allStats, bench.NewAccountBenchmark(cfg).Run()...)
	case "video":
		users := bench.NewAccountBenchmark(cfg).PrepareBenchUsers(5)
		allStats = append(allStats, bench.NewVideoBenchmark(cfg, users).Run()...)
	case "like":
		users := bench.NewAccountBenchmark(cfg).PrepareBenchUsers(10)
		allStats = append(allStats, bench.NewLikeBenchmark(cfg, users).Run()...)
	case "comment":
		users := bench.NewAccountBenchmark(cfg).PrepareBenchUsers(5)
		allStats = append(allStats, bench.NewCommentBenchmark(cfg, users).Run()...)
	case "social":
		users := bench.NewAccountBenchmark(cfg).PrepareBenchUsers(10)
		allStats = append(allStats, bench.NewSocialBenchmark(cfg, users).Run()...)
	case "feed":
		users := bench.NewAccountBenchmark(cfg).PrepareBenchUsers(5)
		allStats = append(allStats, bench.NewFeedBenchmark(cfg, users).Run()...)
	case "scenario":
		users := bench.NewAccountBenchmark(cfg).PrepareBenchUsers(20)
		allStats = append(allStats, bench.NewScenarioBenchmark(cfg, users).Run()...)
	default:
		fmt.Fprintf(os.Stderr, "Unknown module: %s\n", *module)
		os.Exit(1)
	}

	bench.PrintStats(allStats)
}

func runAll(cfg bench.Config, users []bench.PreparedUser) []bench.Stats {
	var allStats []bench.Stats

	fmt.Println("  [1/7] Account...")
	allStats = append(allStats, bench.NewAccountBenchmark(cfg).Run()...)

	fmt.Println("  [2/7] Video...")
	allStats = append(allStats, bench.NewVideoBenchmark(cfg, users).Run()...)

	fmt.Println("  [3/7] Like...")
	allStats = append(allStats, bench.NewLikeBenchmark(cfg, users).Run()...)

	fmt.Println("  [4/7] Comment...")
	allStats = append(allStats, bench.NewCommentBenchmark(cfg, users).Run()...)

	fmt.Println("  [5/7] Social...")
	allStats = append(allStats, bench.NewSocialBenchmark(cfg, users).Run()...)

	fmt.Println("  [6/7] Feed...")
	allStats = append(allStats, bench.NewFeedBenchmark(cfg, users).Run()...)

	fmt.Println("  [7/7] Scenario...")
	allStats = append(allStats, bench.NewScenarioBenchmark(cfg, users).Run()...)

	return allStats
}

func checkHealth(baseURL string) {
	client := bench.NewHTTPClient(baseURL)
	_, _, err := client.PostJSON("/account/findByID", map[string]uint{"id": 1})
	if err != nil {
		fmt.Printf("\n  [WARN] Cannot reach server at %s: %v\n", baseURL, err)
		fmt.Println("  Make sure the server is running before benchmarking.")
	} else {
		fmt.Println("  [OK] Server is reachable.")
	}
}
