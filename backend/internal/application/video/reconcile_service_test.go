package video

import (
	"context"
	"errors"
	"testing"

	domainsocial "feedsystem_video_go/internal/domain/social"
	domainvideo "feedsystem_video_go/internal/domain/video"
)

type fakeLikesReconcileRepo struct {
	drifts   []domainvideo.LikesCountDrift
	fixed    map[uint]int64
	failOnID uint
}

func (f *fakeLikesReconcileRepo) FindLikesCountDrifts(_ context.Context, _ int) ([]domainvideo.LikesCountDrift, error) {
	rows := f.drifts
	f.drifts = nil
	return rows, nil
}

func (f *fakeLikesReconcileRepo) FixLikesCount(_ context.Context, videoID uint, expected int64) error {
	if videoID == f.failOnID {
		return errors.New("injected fix failure")
	}
	f.fixed[videoID] = expected
	return nil
}

type fakeFansReconcileRepo struct {
	drifts   []domainsocial.FansCountDrift
	fixed    map[uint]int64
	failOnID uint
}

func (f *fakeFansReconcileRepo) FindFansCountDrifts(_ context.Context, _ int) ([]domainsocial.FansCountDrift, error) {
	rows := f.drifts
	f.drifts = nil
	return rows, nil
}

func (f *fakeFansReconcileRepo) FixFansCount(_ context.Context, authorID uint, expected int64) error {
	if authorID == f.failOnID {
		return errors.New("injected fix failure")
	}
	f.fixed[authorID] = expected
	return nil
}

func TestReconcileOnceFixesLikesAndFans(t *testing.T) {
	likes := &fakeLikesReconcileRepo{
		drifts: []domainvideo.LikesCountDrift{
			{VideoID: 1, Stored: 10, Actual: 7},
			{VideoID: 2, Stored: 0, Actual: 3},
		},
		fixed: make(map[uint]int64),
	}
	fans := &fakeFansReconcileRepo{
		drifts: []domainsocial.FansCountDrift{
			{AuthorID: 9, Stored: 100, Actual: 95},
		},
		fixed: make(map[uint]int64),
	}

	report, err := NewCounterReconcileService(likes, fans).ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if report.LikesChecked != 2 || report.LikesFixed != 2 {
		t.Fatalf("unexpected likes report: %+v", report)
	}
	if report.FansChecked != 1 || report.FansFixed != 1 {
		t.Fatalf("unexpected fans report: %+v", report)
	}
	if likes.fixed[1] != 7 || likes.fixed[2] != 3 {
		t.Fatalf("unexpected fixed likes: %+v", likes.fixed)
	}
	if fans.fixed[9] != 95 {
		t.Fatalf("unexpected fixed fans: %+v", fans.fixed)
	}
}

func TestReconcileOnceContinuesOnFixFailure(t *testing.T) {
	likes := &fakeLikesReconcileRepo{
		drifts: []domainvideo.LikesCountDrift{
			{VideoID: 1, Stored: 10, Actual: 7},
			{VideoID: 2, Stored: 0, Actual: 3},
		},
		fixed:    make(map[uint]int64),
		failOnID: 1,
	}
	fans := &fakeFansReconcileRepo{fixed: make(map[uint]int64)}

	report, err := NewCounterReconcileService(likes, fans).ReconcileOnce(context.Background())
	if err == nil {
		t.Fatal("expected fix failure to be reported")
	}
	if report.LikesFixed != 1 {
		t.Fatalf("expected one video fixed, got %+v", report)
	}
	if _, ok := likes.fixed[1]; ok {
		t.Fatal("failed video should not be marked fixed")
	}
	if likes.fixed[2] != 3 {
		t.Fatalf("second video should still be fixed: %+v", likes.fixed)
	}
}
