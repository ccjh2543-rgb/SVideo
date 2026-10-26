package redis

import "testing"

// TestTimelineMemberKeepsNumericOrder 校验固定宽度编码后，字典序与数字序一致。
func TestTimelineMemberKeepsNumericOrder(t *testing.T) {
	if timelineMember(10) <= timelineMember(9) {
		t.Fatal("member 10 should sort after member 9")
	}
	if timelineMember(100) <= timelineMember(99) {
		t.Fatal("member 100 should sort after member 99")
	}
	if got := len(timelineMember(1)); got != 20 {
		t.Fatalf("expected fixed width 20, got %d", got)
	}
}
