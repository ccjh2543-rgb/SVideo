package feed

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
)

// heavyPushCall 记录一次推送调用。
type heavyPushCall struct {
	VideoID  uint
	AuthorID uint
	Score    int64
}

// fakeHeavyPushExecutor 记录推送调用。
type fakeHeavyPushExecutor struct {
	calls []heavyPushCall
	err   error
}

func (f *fakeHeavyPushExecutor) Push(_ context.Context, videoID, authorID uint, score int64) error {
	f.calls = append(f.calls, heavyPushCall{VideoID: videoID, AuthorID: authorID, Score: score})
	return f.err
}

// fakeEventIdempotency 内存幂等实现。
type fakeEventIdempotency struct {
	marked   map[string]bool
	unmarked []string
}

func newFakeEventIdempotency() *fakeEventIdempotency {
	return &fakeEventIdempotency{marked: make(map[string]bool)}
}

func (f *fakeEventIdempotency) TryMark(_ context.Context, consumer string, eventID string) (bool, error) {
	key := consumer + ":" + eventID
	if f.marked[key] {
		return false, nil
	}
	f.marked[key] = true
	return true, nil
}

func (f *fakeEventIdempotency) Unmark(_ context.Context, consumer string, eventID string) error {
	f.unmarked = append(f.unmarked, consumer+":"+eventID)
	delete(f.marked, consumer+":"+eventID)
	return nil
}

func marshalHeavyPushEvent(t *testing.T, event rabbitmq.HeavyPushEvent) []byte {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return body
}

// TestHeavyPushWorkerProcessesTask 正常任务只推送一次。
func TestHeavyPushWorkerProcessesTask(t *testing.T) {
	executor := &fakeHeavyPushExecutor{}
	events := newFakeEventIdempotency()
	worker := NewHeavyPushWorker(nil, executor, events, rabbitmq.HeavyPushQueueName)

	body := marshalHeavyPushEvent(t, rabbitmq.HeavyPushEvent{EventID: "e1", VideoID: 9, AuthorID: 1, Score: 1760000000000})
	if err := worker.process(context.Background(), body); err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(executor.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(executor.calls))
	}
	if executor.calls[0].VideoID != 9 || executor.calls[0].Score != 1760000000000 {
		t.Fatalf("unexpected call: %+v", executor.calls[0])
	}
}

// TestHeavyPushWorkerSkipsDuplicate 同一 eventID 重复投递不会重复推送。
func TestHeavyPushWorkerSkipsDuplicate(t *testing.T) {
	executor := &fakeHeavyPushExecutor{}
	events := newFakeEventIdempotency()
	worker := NewHeavyPushWorker(nil, executor, events, rabbitmq.HeavyPushQueueName)
	body := marshalHeavyPushEvent(t, rabbitmq.HeavyPushEvent{EventID: "e1", VideoID: 9, AuthorID: 1})

	for i := 0; i < 2; i++ {
		if err := worker.process(context.Background(), body); err != nil {
			t.Fatalf("process %d: %v", i, err)
		}
	}
	if len(executor.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(executor.calls))
	}
}

// TestHeavyPushWorkerUnmarksOnFailure 失败时清幂等标记，保证死信重放能重跑。
func TestHeavyPushWorkerUnmarksOnFailure(t *testing.T) {
	executor := &fakeHeavyPushExecutor{err: errors.New("redis down")}
	events := newFakeEventIdempotency()
	worker := NewHeavyPushWorker(nil, executor, events, rabbitmq.HeavyPushQueueName)
	body := marshalHeavyPushEvent(t, rabbitmq.HeavyPushEvent{EventID: "e2", VideoID: 9, AuthorID: 1})

	if err := worker.process(context.Background(), body); err == nil {
		t.Fatal("expected error, got nil")
	}
	if len(events.unmarked) != 1 || events.unmarked[0] != "heavy_push:e2" {
		t.Fatalf("unmarked = %v, want [heavy_push:e2]", events.unmarked)
	}
}
