package bench

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Result 单次请求的结果
type Result struct {
	Name    string
	Latency time.Duration
	Error   error
}

// Stats 压测统计结果
type Stats struct {
	Name       string
	Total      int
	Success    int
	Errors     int
	Dur        time.Duration
	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
	P50Latency time.Duration
	P95Latency time.Duration
	P99Latency time.Duration
	QPS        float64
}

// CollectStats 从结果切片中计算统计信息
func CollectStats(name string, results []Result, dur time.Duration) Stats {
	s := Stats{
		Name:  name,
		Total: len(results),
		Dur:   dur,
	}

	if len(results) == 0 {
		return s
	}

	latencies := make([]time.Duration, 0, len(results))
	for _, r := range results {
		if r.Error != nil {
			s.Errors++
		} else {
			s.Success++
			latencies = append(latencies, r.Latency)
		}
	}

	if len(latencies) == 0 {
		return s
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	s.MinLatency = latencies[0]
	s.MaxLatency = latencies[len(latencies)-1]

	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	s.AvgLatency = total / time.Duration(len(latencies))

	s.P50Latency = latencies[percentileIdx(len(latencies), 50)]
	s.P95Latency = latencies[percentileIdx(len(latencies), 95)]
	s.P99Latency = latencies[percentileIdx(len(latencies), 99)]

	s.QPS = float64(len(latencies)) / dur.Seconds()

	return s
}

func percentileIdx(n int, p int) int {
	idx := int(float64(n) * float64(p) / 100.0)
	if idx >= n {
		idx = n - 1
	}
	return idx
}

// PrintStats 打印压测结果表格
func PrintStats(stats []Stats) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                               BENCHMARK RESULTS REPORT                                   ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ %-30s │ %8s │ %8s │ %8s │ %8s │ %8s │ %8s │ %10s ║\n",
		"Test", "Total", "Success", "Errors", "P50", "P95", "P99", "QPS")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════════════════╣")

	for _, s := range stats {
		fmt.Printf("║ %-30s │ %8d │ %8d │ %8d │ %8s │ %8s │ %8s │ %9.1f ║\n",
			s.Name, s.Total, s.Success, s.Errors,
			shortDur(s.P50Latency), shortDur(s.P95Latency), shortDur(s.P99Latency),
			s.QPS)
	}
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════════════════╝")
}

func shortDur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	case d >= time.Microsecond:
		return fmt.Sprintf("%.2fμs", float64(d.Nanoseconds())/1000.0)
	default:
		return d.String()
	}
}

// Runner 并发压测执行器
type Runner struct {
	concurrency int
}

// NewRunner 创建压测执行器
func NewRunner(concurrency int) *Runner {
	return &Runner{concurrency: concurrency}
}

// Run 并发执行压测任务
// fn 接收一个 worker ID（0-based），返回该 worker 执行的所有结果
func (r *Runner) Run(fn func(workerID int) []Result) []Result {
	var wg sync.WaitGroup
	resultsCh := make(chan []Result, r.concurrency)

	for i := 0; i < r.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			results := fn(workerID)
			resultsCh <- results
		}(i)
	}

	wg.Wait()
	close(resultsCh)

	var all []Result
	for res := range resultsCh {
		all = append(all, res...)
	}
	return all
}
