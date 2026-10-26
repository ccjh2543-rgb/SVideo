// Package resilience 提供轻量的依赖熔断能力。
//
// 设计目标：当 Redis / MQ 等外部依赖持续失败时，快速失败并交给上层降级，
// 避免每个请求都先等待一次超时，把故障放大成连接池耗尽。
package resilience

import (
	"sync"
	"time"
)

// State 熔断器状态。
type State int

const (
	// StateClosed 正常放行，并统计失败率。
	StateClosed State = iota
	// StateOpen 直接拒绝，等待冷却时间后进入半开。
	StateOpen
	// StateHalfOpen 少量放行探测，成功足够次数后恢复。
	StateHalfOpen
)

// String 返回状态名称，便于日志与指标输出。
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Config 熔断器配置。
type Config struct {
	Window         time.Duration // 统计窗口
	MinRequests    int           // 窗口内最少请求数，避免低流量误熔断
	FailureRatio   float64       // 失败率阈值，0~1
	OpenTimeout    time.Duration // open 状态冷却时间
	HalfOpenProbes int           // 半开状态最多同时放行的探测数
	CloseSuccesses int           // 半开状态成功多少次后恢复
}

// DefaultConfig 返回推荐默认配置。
func DefaultConfig() Config {
	return Config{
		Window:         10 * time.Second,
		MinRequests:    20,
		FailureRatio:   0.5,
		OpenTimeout:    5 * time.Second,
		HalfOpenProbes: 3,
		CloseSuccesses: 2,
	}
}

// Breaker 单依赖熔断器。
type Breaker struct {
	mu   sync.Mutex
	name string
	cfg  Config
	now  func() time.Time

	state       State
	windowStart time.Time
	requests    int
	failures    int

	openedAt  time.Time
	probes    int
	successes int
}

// NewBreaker 创建熔断器。
func NewBreaker(name string, cfg Config) *Breaker {
	return newBreaker(name, cfg, time.Now)
}

// newBreaker 允许测试注入时钟。
func newBreaker(name string, cfg Config, now func() time.Time) *Breaker {
	cfg = normalizeConfig(cfg)
	return &Breaker{
		name:        name,
		cfg:         cfg,
		now:         now,
		state:       StateClosed,
		windowStart: now(),
	}
}

// normalizeConfig 填充非法配置，避免除零或永久熔断。
func normalizeConfig(cfg Config) Config {
	defaults := DefaultConfig()
	if cfg.Window <= 0 {
		cfg.Window = defaults.Window
	}
	if cfg.MinRequests <= 0 {
		cfg.MinRequests = defaults.MinRequests
	}
	if cfg.FailureRatio <= 0 || cfg.FailureRatio > 1 {
		cfg.FailureRatio = defaults.FailureRatio
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = defaults.OpenTimeout
	}
	if cfg.HalfOpenProbes <= 0 {
		cfg.HalfOpenProbes = defaults.HalfOpenProbes
	}
	if cfg.CloseSuccesses <= 0 {
		cfg.CloseSuccesses = defaults.CloseSuccesses
	}
	return cfg
}

// Allow 判断当前是否放行。
func (b *Breaker) Allow() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	switch b.state {
	case StateOpen:
		if now.Sub(b.openedAt) < b.cfg.OpenTimeout {
			return false
		}
		// 冷却结束，进入半开状态并放行一次探测。
		b.state = StateHalfOpen
		b.probes = 0
		b.successes = 0
		fallthrough
	case StateHalfOpen:
		if b.probes >= b.cfg.HalfOpenProbes {
			return false
		}
		b.probes++
		return true
	default:
		return true
	}
}

// Record 记录一次调用的结果。
// 只有依赖错误才应该传入 error；业务错误不应计入熔断。
func (b *Breaker) Record(err error) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	switch b.state {
	case StateOpen:
		return
	case StateHalfOpen:
		if err != nil {
			b.trip(now)
			return
		}
		b.successes++
		if b.successes >= b.cfg.CloseSuccesses {
			b.reset(now)
		}
		return
	}

	// closed 状态：滚动窗口统计。
	if now.Sub(b.windowStart) >= b.cfg.Window {
		b.windowStart = now
		b.requests = 0
		b.failures = 0
	}
	b.requests++
	if err != nil {
		b.failures++
	}
	if b.requests >= b.cfg.MinRequests {
		ratio := float64(b.failures) / float64(b.requests)
		if ratio >= b.cfg.FailureRatio {
			b.trip(now)
		}
	}
}

// State 返回当前状态。
func (b *Breaker) State() State {
	if b == nil {
		return StateClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Name 返回熔断器名称。
func (b *Breaker) Name() string {
	if b == nil {
		return ""
	}
	return b.name
}

// trip 进入熔断状态。
func (b *Breaker) trip(now time.Time) {
	b.state = StateOpen
	b.openedAt = now
	b.probes = 0
	b.successes = 0
}

// reset 恢复到正常状态并清空统计。
func (b *Breaker) reset(now time.Time) {
	b.state = StateClosed
	b.windowStart = now
	b.requests = 0
	b.failures = 0
	b.probes = 0
	b.successes = 0
}

// Guard 按依赖名称管理多个熔断器。
// 粒度必须分开，避免一个依赖故障把所有能力一起关闭。
type Guard struct {
	mu       sync.RWMutex
	breakers map[string]*Breaker
}

// NewGuard 创建熔断器注册表。
func NewGuard(names ...string) *Guard {
	guard := &Guard{breakers: make(map[string]*Breaker, len(names))}
	for _, name := range names {
		guard.breakers[name] = NewBreaker(name, DefaultConfig())
	}
	return guard
}

// Allow 判断指定依赖是否放行；未注册的依赖默认放行。
func (g *Guard) Allow(name string) bool {
	if g == nil {
		return true
	}
	breaker := g.get(name)
	if breaker == nil {
		return true
	}
	return breaker.Allow()
}

// Record 记录指定依赖的调用结果。
func (g *Guard) Record(name string, err error) {
	if g == nil {
		return
	}
	if breaker := g.get(name); breaker != nil {
		breaker.Record(err)
	}
}

// State 返回指定依赖的熔断状态。
func (g *Guard) State(name string) State {
	if g == nil {
		return StateClosed
	}
	if breaker := g.get(name); breaker != nil {
		return breaker.State()
	}
	return StateClosed
}

// get 读取熔断器；未注册时动态补一个，避免调用方漏注册导致空指针。
func (g *Guard) get(name string) *Breaker {
	g.mu.RLock()
	breaker := g.breakers[name]
	g.mu.RUnlock()
	if breaker != nil {
		return breaker
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if breaker = g.breakers[name]; breaker != nil {
		return breaker
	}
	breaker = NewBreaker(name, DefaultConfig())
	g.breakers[name] = breaker
	return breaker
}
