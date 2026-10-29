package feed

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"feedsystem_video_go/internal/infrastructure/messaging/rabbitmq"
)

// largeFanoutCall 记录一次大粉丝量写扩散调用。
type largeFanoutCall struct {
	EventID  string
	VideoID  uint
	AuthorID uint
	Score    int64
}

// fakeLargeFanoutExecutor 记录写扩散调用。
type fakeLargeFanoutExecutor struct {
	calls []largeFanoutCall
	err   error
}

func (f *fakeLargeFanoutExecutor) FanoutFollowers(_ context.Context, eventID string, videoID, authorID uint, score int64) error {
	f.calls = append(f.calls, largeFanoutCall{EventID: eventID, VideoID: videoID, AuthorID: authorID, Score: score})
	return f.err
}

func marshalLargeFanoutEvent(t *testing.T, event rabbitmq.LargeFanoutEvent) []byte {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return body
}

// TestLargeFanoutWorkerProcessesTask 正常任务按事件 ID 执行一次写扩散。
func TestLargeFanoutWorkerProcessesTask(t *testing.T) {
	executor := &fakeLargeFanoutExecutor{}
	events := newFakeEventIdempotency()
	worker := NewLargeFanoutWorker(nil, executor, events, rabbitmq.LargeFanoutQueueName)

	body := marshalLargeFanoutEvent(t, rabbitmq.LargeFanoutEvent{EventID: "e1", VideoID: 9, AuthorID: 1, Score: 1760000000000})
	if err := worker.process(context.Background(), body); err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(executor.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(executor.calls))
	}
	if executor.calls[0].EventID != "e1" || executor.calls[0].Score != 1760000000000 {
		t.Fatalf("unexpected call: %+v", executor.calls[0])
	}
}

// TestLargeFanoutWorkerSkipsDuplicate 重复投递不会重复扩散。
func TestLargeFanoutWorkerSkipsDuplicate(t *testing.T) {
	executor := &fakeLargeFanoutExecutor{}
	events := newFakeEventIdempotency()
	worker := NewLargeFanoutWorker(nil, executor, events, rabbitmq.LargeFanoutQueueName)
	body := marshalLargeFanoutEvent(t, rabbitmq.LargeFanoutEvent{EventID: "e1", VideoID: 9, AuthorID: 1})

	for i := 0; i < 2; i++ {
		if err := worker.process(context.Background(), body); err != nil {
			t.Fatalf("process %d: %v", i, err)
		}
	}
	if len(executor.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(executor.calls))
	}
}

// TestLargeFanoutWorkerUnmarksOnFailure 失败时清幂等标记，死信重放可以从断点继续。
func TestLargeFanoutWorkerUnmarksOnFailure(t *testing.T) {
	executor := &fakeLargeFanoutExecutor{err: errors.New("redis down")}
	events := newFakeEventIdempotency()
	worker := NewLargeFanoutWorker(nil, executor, events, rabbitmq.LargeFanoutQueueName)
	body := marshalLargeFanoutEvent(t, rabbitmq.LargeFanoutEvent{EventID: "e2", VideoID: 9, AuthorID: 1})

	if err := worker.process(context.Background(), body); err == nil {
		t.Fatal("expected error, got nil")
	}
	if len(events.unmarked) != 1 || events.unmarked[0] != "fanout_large:e2" {
		t.Fatalf("unmarked = %v, want [fanout_large:e2]", events.unmarked)
	}
}
