package bench

import (
	"encoding/json"
	"fmt"
	"time"
)

// AccountBenchmark 账号模块压测
type AccountBenchmark struct {
	cfg     Config
	clients []*HTTPClient // 每个并发一个独立客户端
}

// NewAccountBenchmark 创建账号压测实例
func NewAccountBenchmark(cfg Config) *AccountBenchmark {
	return &AccountBenchmark{cfg: cfg}
}

// Run 执行账号模块压测
func (b *AccountBenchmark) Run() []Stats {
	var allStats []Stats

	// 1. 注册压测
	allStats = append(allStats, b.runRegister())

	// 2. 登录压测
	allStats = append(allStats, b.runLogin())

	// 3. FindByID 压测
	allStats = append(allStats, b.runFindByID())

	return allStats
}

func (b *AccountBenchmark) runRegister() Stats {
	runner := NewRunner(b.cfg.AccountConcurrency)
	start := time.Now()
	results := runner.Run(func(workerID int) []Result {
		client := NewHTTPClient(b.cfg.BaseURL)
		var res []Result
		count := b.cfg.AccountRequests / b.cfg.AccountConcurrency
		if count <= 0 {
			count = 1
		}
		for i := 0; i < count; i++ {
			username := fmt.Sprintf("bench_user_%d_%d", workerID, i)
			body := map[string]string{
				"username": username,
				"password": "bench123456",
			}
			_, lat, err := client.PostJSON("/account/register", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Account.Register", results, dur)
}

func (b *AccountBenchmark) runLogin() Stats {
	// 先提前注册一批用户供登录测试
	users := b.prepareUsers(100)
	if len(users) == 0 {
		return Stats{Name: "Account.Login (skipped: no users prepared)"}
	}

	runner := NewRunner(b.cfg.AccountConcurrency)
	start := time.Now()
	results := runner.Run(func(workerID int) []Result {
		client := NewHTTPClient(b.cfg.BaseURL)
		var res []Result
		count := b.cfg.AccountRequests / b.cfg.AccountConcurrency
		for i := 0; i < count; i++ {
			idx := (workerID*count + i) % len(users)
			body := map[string]string{
				"username": users[idx].Username,
				"password": users[idx].Password,
			}
			_, lat, err := client.PostJSON("/account/login", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Account.Login", results, dur)
}

func (b *AccountBenchmark) runFindByID() Stats {
	// 先用一个已知 ID 的用户
	users := b.prepareUsers(1)
	if len(users) == 0 {
		return Stats{Name: "Account.FindByID (skipped: no users prepared)"}
	}
	userID := users[0].ID

	runner := NewRunner(b.cfg.AccountConcurrency)
	start := time.Now()
	results := runner.Run(func(workerID int) []Result {
		client := NewHTTPClient(b.cfg.BaseURL)
		var res []Result
		count := b.cfg.AccountRequests / b.cfg.AccountConcurrency
		if count <= 0 {
			count = 1
		}
		for i := 0; i < count; i++ {
			body := map[string]interface{}{"id": userID}
			_, lat, err := client.PostJSON("/account/findByID", body)
			res = append(res, Result{Latency: lat, Error: err})
		}
		return res
	})
	dur := time.Since(start)
	return CollectStats("Account.FindByID", results, dur)
}

type PreparedUser struct {
	ID       uint
	Username string
	Password string
	Token    string
}

func (b *AccountBenchmark) prepareUsers(n int) []PreparedUser {
	client := NewHTTPClient(b.cfg.BaseURL)

	// 限流上限：注册 5次/小时，所以最多用 3 个用户
	if n > 3 {
		n = 3
	}

	var users []PreparedUser
	for i := 0; i < n; i++ {
		username := fmt.Sprintf("bench_prep_%d_%d", time.Now().UnixNano(), i)
		password := "bench123456"

		// 注册（顺序执行，避免触发限流）
		_, _, err := client.PostJSON("/account/register", map[string]string{
			"username": username,
			"password": password,
		})
		if err != nil {
			fmt.Printf("  [WARN] prepareUsers: register failed: %v\n", err)
			continue
		}

		// 登录获取 token
		respBody, _, err := client.PostJSON("/account/login", map[string]string{
			"username": username,
			"password": password,
		})
		if err != nil {
			fmt.Printf("  [WARN] prepareUsers: login failed: %v\n", err)
			continue
		}

		var loginInfo struct {
			Token string `json:"token"`
		}
		json.Unmarshal(respBody, &loginInfo)

		// 查询 ID
		idRespBody, _, err := client.PostJSON("/account/findByUsername", map[string]string{
			"username": username,
		})
		if err != nil {
			fmt.Printf("  [WARN] prepareUsers: findByUsername failed: %v\n", err)
			continue
		}

		var accountInfo struct {
			ID uint `json:"id"`
		}
		json.Unmarshal(idRespBody, &accountInfo)

		users = append(users, PreparedUser{
			ID:       accountInfo.ID,
			Username: username,
			Password: password,
			Token:    loginInfo.Token,
		})
	}

	if len(users) > 0 {
		fmt.Printf("  [INFO] Prepared %d users for benchmark\n", len(users))
	}
	return users
}

// PrepareBenchUsers 公开方法：准备指定数量的测试用户（注册 + 登录获取 Token）
func (b *AccountBenchmark) PrepareBenchUsers(n int) []PreparedUser {
	return b.prepareUsers(n)
}
