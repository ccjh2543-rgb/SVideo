package ws

import "testing"

func TestHubSendToAccount(t *testing.T) {
	hub := NewHub()
	client := &Client{hub: hub, accountID: 1, send: make(chan []byte, 1)}
	other := &Client{hub: hub, accountID: 2, send: make(chan []byte, 1)}

	hub.Register(client)
	hub.Register(other)
	defer hub.Unregister(client)
	defer hub.Unregister(other)

	hub.SendToAccount(1, []byte(`{"title":"hello"}`))

	select {
	case payload := <-client.send:
		if string(payload) != `{"title":"hello"}` {
			t.Fatalf("unexpected payload: %s", payload)
		}
	default:
		t.Fatal("expected client to receive payload")
	}

	select {
	case payload := <-other.send:
		t.Fatalf("other account should not receive payload: %s", payload)
	default:
	}
}

func TestHubUnregisterClosesSend(t *testing.T) {
	hub := NewHub()
	client := &Client{hub: hub, accountID: 1, send: make(chan []byte, 1)}
	hub.Register(client)
	hub.Unregister(client)

	if _, ok := <-client.send; ok {
		t.Fatal("expected send channel to be closed")
	}
	if hub.OnlineCount() != 0 {
		t.Fatalf("expected no online clients, got %d", hub.OnlineCount())
	}
}
