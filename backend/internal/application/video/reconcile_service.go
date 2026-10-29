package video

import (
	"context"
	"log"

	domainsocial "feedsystem_video_go/internal/domain/social"
	domainvideo "feedsystem_video_go/internal/domain/video"
)

const (
	// defaultReconcileBatch 单轮对账处理的差异行数。
	defaultReconcileBatch = 200
	// maxReconcileRounds 单次对账最多循环轮数，避免修正失败时无限重试。
	maxReconcileRounds = 10
)

// ReconcileReport 一轮对账的结果统计。
type ReconcileReport struct {
	LikesChecked int
	LikesFixed   int
	FansChecked  int
	FansFixed    int
}

// CounterReconcileService 计数对账应用服务。
//
// 以明细表为准修正 videos.likes_count 与 creator_stats.fans_count。
// 只修正 MySQL 计数，不直接改 Redis 热度桶；热度按事件时间重放属于独立任务。
type CounterReconcileService struct {
	likes domainvideo.LikesCounterReconcileRepository
	fans  domainsocial.FansCounterReconcileRepository
	batch int
}

// NewCounterReconcileService 创建计数对账服务。
func NewCounterReconcileService(
	likes domainvideo.LikesCounterReconcileRepository,
	fans domainsocial.FansCounterReconcileRepository,
) *CounterReconcileService {
	return &CounterReconcileService{
		likes: likes,
		fans:  fans,
		batch: defaultReconcileBatch,
	}
}

// ReconcileOnce 执行一轮对账：分批发现差异并按明细值修正。
// 单条修正失败不会中断整轮，返回第一条错误供调用方记录告警。
func (s *CounterReconcileService) ReconcileOnce(ctx context.Context) (ReconcileReport, error) {
	var report ReconcileReport
	if s == nil {
		return report, nil
	}

	var firstErr error
	if s.likes != nil {
		for round := 0; round < maxReconcileRounds; round++ {
			drifts, err := s.likes.FindLikesCountDrifts(ctx, s.batch)
			if err != nil {
				return report, err
			}
			if len(drifts) == 0 {
				break
			}
			for _, drift := range drifts {
				report.LikesChecked++
				if err := s.likes.FixLikesCount(ctx, drift.VideoID, drift.Actual); err != nil {
					log.Printf("reconcile: fix likes_count failed video=%d stored=%d actual=%d: %v",
						drift.VideoID, drift.Stored, drift.Actual, err)
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				report.LikesFixed++
			}
			if len(drifts) < s.batch {
				break
			}
		}
	}

	if s.fans != nil {
		for round := 0; round < maxReconcileRounds; round++ {
			drifts, err := s.fans.FindFansCountDrifts(ctx, s.batch)
			if err != nil {
				return report, err
			}
			if len(drifts) == 0 {
				break
			}
			for _, drift := range drifts {
				report.FansChecked++
				if err := s.fans.FixFansCount(ctx, drift.AuthorID, drift.Actual); err != nil {
					log.Printf("reconcile: fix fans_count failed author=%d stored=%d actual=%d: %v",
						drift.AuthorID, drift.Stored, drift.Actual, err)
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				report.FansFixed++
			}
			if len(drifts) < s.batch {
				break
			}
		}
	}

	return report, firstErr
}
