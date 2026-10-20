package ws

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
)

// newClient 创建连接并启动读写协程。
func newClient(hub *Hub, conn *websocket.Conn, accountID uint) *Client {
	client := &Client{
		hub:       hub,
		conn:      conn,
		send:      make(chan []byte, 64),
		accountID: accountID,
	}
	hub.Register(client)
	go client.writePump()
	go client.readPump()
	return client
}

// readPump 读取客户端消息并处理心跳。
func (c *Client) readPump() {
	defer func() {
		c.hub.Unregister(c)
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessage)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
		// 当前客户端不需要向服务端发送业务消息，忽略内容即可。
	}
}

// writePump 推送消息并定时发送 ping。
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case payload, ok := <-c.send:
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// close 安全关闭连接。
func (c *Client) close() {
	if c == nil || c.conn == nil {
		return
	}
	if err := c.conn.Close(); err != nil {
		log.Printf("websocket close failed: %v", err)
	}
}
