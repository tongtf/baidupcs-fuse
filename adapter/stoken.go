package adapter

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// StokenRefresher 后台自动刷新 STOKEN。
// 百度 STOKEN 有效期约 2 小时，过期后 PCS API 会报 errno=-6。
// 刷新机制：用 BDUSS 请求 Pan API，从 Set-Cookie 中提取新 STOKEN。
type StokenRefresher struct {
	mu       sync.RWMutex
	bduss    string
	stoken   string
	client   *http.Client
	interval time.Duration
	stopCh   chan struct{}
	// OnRefresh 回调，stoken 刷新后通知调用方更新引用。
	OnRefresh func(newStoken string)
}

// NewStokenRefresher 创建 STOKEN 自动刷新器。
// interval: 刷新间隔（建议 60-90 分钟，百度有效期约 2 小时）。
func NewStokenRefresher(bduss, stoken string, interval time.Duration) *StokenRefresher {
	return &StokenRefresher{
		bduss:    bduss,
		stoken:   stoken,
		client:   &http.Client{Timeout: 15 * time.Second},
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Token 返回当前 STOKEN（线程安全）。
func (r *StokenRefresher) Token() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stoken
}

// Start 启动后台刷新 goroutine。
func (r *StokenRefresher) Start() {
	go r.loop()
	log.Printf("STOKEN 自动刷新已启动（间隔 %v）", r.interval)
}

// Stop 停止后台刷新。
func (r *StokenRefresher) Stop() {
	close(r.stopCh)
}

func (r *StokenRefresher) loop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// 启动后立即检查一次
	r.refresh()

	for {
		select {
		case <-ticker.C:
			r.refresh()
		case <-r.stopCh:
			return
		}
	}
}

// refresh 执行一次 STOKEN 刷新。
func (r *StokenRefresher) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 请求 Pan API（任意轻量端点），用 BDUSS 认证
	// 服务端会在 Set-Cookie 中返回新 STOKEN
	apiURL := "https://pan.baidu.com/api/gettemplatevariable?clienttype=0&app_id=250528&fields=%5B%22uk%22%5D"
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		log.Printf("STOKEN 刷新: 创建请求失败: %v", err)
		return
	}
	req.Header.Set("Cookie", "BDUSS="+r.bduss)

	resp, err := r.client.Do(req)
	if err != nil {
		log.Printf("STOKEN 刷新: 请求失败: %v", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	// 从 Set-Cookie 中提取新 STOKEN
	newStoken := extractSToken(resp)
	if newStoken == "" {
		// 没有新 STOKEN，可能未过期（正常）
		return
	}
	if newStoken == r.stoken {
		return // 未变化
	}

	r.mu.Lock()
	r.stoken = newStoken
	r.mu.Unlock()

	log.Printf("STOKEN 已刷新（新值长度=%d）", len(newStoken))
	if r.OnRefresh != nil {
		r.OnRefresh(newStoken)
	}
}

// extractSToken 从 HTTP 响应的 Set-Cookie 中提取 STOKEN。
func extractSToken(resp *http.Response) string {
	for _, sc := range resp.Cookies() {
		if strings.EqualFold(sc.Name, "STOKEN") && sc.Value != "" {
			return sc.Value
		}
	}
	// 也检查 raw Set-Cookie header（某些情况下 cookiejar 解析不到）
	for _, raw := range resp.Header.Values("Set-Cookie") {
		if idx := strings.Index(strings.ToLower(raw), "stoken="); idx >= 0 {
			val := raw[idx+8:]
			if end := strings.IndexAny(val, ";, "); end > 0 {
				val = val[:end]
			}
			if val != "" {
				return val
			}
		}
	}
	return ""
}

// RefreshNow 手动触发一次刷新（供错误恢复调用）。
func (r *StokenRefresher) RefreshNow() error {
	old := r.Token()
	r.refresh()
	new := r.Token()
	if new == old {
		return fmt.Errorf("STOKEN 未变化（可能 BDUSS 已过期）")
	}
	return nil
}
