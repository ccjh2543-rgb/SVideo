package video

import (
	"context"
	"log"
	"time"

	appvideo "feedsystem_video_go/internal/application/video"
)

// defaultCounterReconcileInterval 计数对账扫描间隔。
const defaultCounterReconcileInterval = 10 * time.Minute

// CounterReconciler 定期执行计数对账的后台任务。
type CounterReconciler struct {
	service  *appvideo.CounterReconcileService
	interval time.Duration
}

// NewCounterReconciler 创建计数对账任务。
// interval <= 0 时使用默认间隔。
func NewCounterReconciler(service *appvideo.CounterReconcileService, interval time.Duration) *CounterReconciler {
	if interval <= 0 {
		interval = defaultCounterReconcileInterval
	}
	return &CounterReconciler{service: service, interval: interval}
}

// Run 启动对账循环：启动时先执行一次，之后按间隔执行。
func (r *CounterReconciler) Run(ctx context.Context) error {
	if r == nil || r.service == nil {
		return nil
	}

	r.reconcile(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

// reconcile 执行一次对账，失败只记录日志，不影响后续周期。
func (r *CounterReconciler) reconcile(ctx context.Context) {
	report, err := r.service.ReconcileOnce(ctx)
	if err != nil {
		log.Printf("counter reconciler: reconcile failed: %v", err)
	}
	if report.LikesFixed > 0 || report.FansFixed > 0 {
		log.Printf("counter reconciler: fixed likes=%d/%d fans=%d/%d",
			report.LikesFixed, report.LikesChecked, report.FansFixed, report.FansChecked)
	}
}
