package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Config 压测配置
type Config struct {
	BaseURL string

	// 账号模块
	AccountConcurrency int
	AccountRequests    int

	// 视频模块
	VideoConcurrency int
	VideoRequests    int

	// 点赞模块
	LikeConcurrency int
	LikeRequests    int

	// 评论模块
	CommentConcurrency int
	CommentRequests    int

	// 关注模块
	SocialConcurrency int
	SocialRequests    int

	// Feed 模块
	FeedConcurrency int
	FeedRequests    int
}

// DefaultConfig 返回默认压测配置
func DefaultConfig() Config {
	return Config{
		BaseURL:            "http://localhost:8080",
		AccountConcurrency: 10,
		AccountRequests:    100,
		VideoConcurrency:   5,
		VideoRequests:      50,
		LikeConcurrency:    10,
		LikeRequests:       100,
		CommentConcurrency: 5,
		CommentRequests:    50,
		SocialConcurrency:  5,
		SocialRequests:     50,
		FeedConcurrency:    10,
		FeedRequests:       100,
	}
}

// HTTPClient 封装 HTTP 请求和 JWT Token 管理
type HTTPClient struct {
	baseURL string
	token   string
	client  *http.Client
}

// NewHTTPClient 创建 HTTP 客户端
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// NewHTTPClientWithToken 创建带 Token 的 HTTP 客户端
func NewHTTPClientWithToken(baseURL, token string) *HTTPClient {
	return &HTTPClient{
		baseURL: baseURL,
		token:   token,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// SetToken 设置 JWT Token
func (c *HTTPClient) SetToken(token string) {
	c.token = token
}

// PostJSON 发送 POST JSON 请求，返回响应体和耗时
func (c *HTTPClient) PostJSON(path string, body interface{}) ([]byte, time.Duration, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal body: %w", err)
	}

	url := c.baseURL + path
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, 0, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		return nil, latency, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, latency, fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return respBody, latency, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, latency, nil
}

// PostJSONRaw 发送 POST JSON 请求，返回原始响应
func (c *HTTPClient) PostJSONRaw(path string, body interface{}) (*http.Response, time.Duration, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal body: %w", err)
	}

	url := c.baseURL + path
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, 0, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		return nil, latency, fmt.Errorf("do request: %w", err)
	}

	return resp, latency, nil
}