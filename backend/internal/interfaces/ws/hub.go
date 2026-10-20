// Package ws 提供 WebSocket 连接管理和即时通知推送。
package ws

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 30 * time.Second
	maxMessage = 4096
)

// Client 单个 WebSocket 连接。
type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	accountID uint

	mu     sync.Mutex
	closed bool
}

// sendMessage 线程安全地写入发送缓冲。
// 返回 false 表示连接已关闭或缓冲已满。
func (c *Client) sendMessage(payload []byte) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	select {
	case c.send <- payload:
		return true
	default:
		return false
	}
}

// closeSend 线程安全地关闭发送通道，重复调用不会 panic。
func (c *Client) closeSend() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.send)
}

// Hub 按账号管理 WebSocket 连接。
type Hub struct {
	mu      sync.RWMutex
	clients map[uint]map[*Client]struct{}
}

// NewHub 创建连接管理器。
func NewHub() *Hub {
	return &Hub{clients: make(map[uint]map[*Client]struct{})}
}

// Register 注册连接。
func (h *Hub) Register(client *Client) {
	if h == nil || client == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[client.accountID] == nil {
		h.clients[client.accountID] = make(map[*Client]struct{})
	}
	h.clients[client.accountID][client] = struct{}{}
}

// Unregister 注销连接并关闭发送通道。
func (h *Hub) Unregister(client *Client) {
	if h == nil || client == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	conns, ok := h.clients[client.accountID]
	if !ok {
		return
	}
	if _, exists := conns[client]; !exists {
		return
	}
	delete(conns, client)
	client.closeSend()
	if len(conns) == 0 {
		delete(h.clients, client.accountID)
	}
}

// SendToAccount 向账号的全部连接推送消息。
func (h *Hub) SendToAccount(accountID uint, payload []byte) {
	if h == nil || len(payload) == 0 {
		return
	}
	h.mu.RLock()
	conns := make([]*Client, 0, len(h.clients[accountID]))
	for client := range h.clients[accountID] {
		conns = append(conns, client)
	}
	h.mu.RUnlock()

	for _, client := range conns {
		if !client.sendMessage(payload) {
			// 连接已关闭或发送缓冲已满，关闭该连接，避免阻塞其他用户。
			h.Unregister(client)
		}
	}
}

// OnlineCount 返回当前在线连接数，用于监控。
func (h *Hub) OnlineCount() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	total := 0
	for _, conns := range h.clients {
		total += len(conns)
	}
	return total
}
