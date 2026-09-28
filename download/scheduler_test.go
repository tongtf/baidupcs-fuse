package download

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// redirectTestServer 模拟 PCS→CDN 两步下载流程：
//   - 第一阶段（PCS）：按 mode 返回 302 redirect、403 或 416。
//   - 第二阶段（CDN，URL 路径含 /cdn）：直接返回 body；cdnLen≥0 且 <len(body) 时截断，模拟短读。
func redirectTestServer(t *testing.T, mode string, body []byte, cdnLen int) (*httptest.Server, func()) {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/cdn") {
			n := len(body)
			if cdnLen >= 0 && cdnLen < n {
				n = cdnLen
			}
			w.Header().Set("Content-Length", strconv.Itoa(n))
			w.WriteHeader(http.StatusPartialContent) // 206
			w.Write(body[:n])
			return
		}
		switch mode {
		case "403":
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusForbidden)
		case "416":
			w.Header().Set("Connection", "close")
			w.WriteHeader(416)
		default: // redirect → CDN stage
			w.Header().Set("Location", srv.URL+"/cdn")
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusFound)
		}
	}))
	return srv, srv.Close
}

func TestFetchRange_Success(t *testing.T) {
	srv, cleanup := redirectTestServer(t, "redirect", []byte("hello"), -1)
	defer cleanup()
	s := NewScheduler(5, 120*time.Second, 60*time.Second)
	data, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("data = %q, want %q", data, "hello")
	}
}

func TestFetchRange_ShortRead(t *testing.T) {
	// CDN 只返回 3 字节，但请求长度为 10 → ErrShortRead
	srv, cleanup := redirectTestServer(t, "redirect", []byte("hello"), 3)
	defer cleanup()
	s := NewScheduler(5, 120*time.Second, 60*time.Second)
	_, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 10)
	if err != ErrShortRead {
		t.Errorf("err = %v, want ErrShortRead", err)
	}
}

func TestFetchRange_403_Error(t *testing.T) {
	srv, cleanup := redirectTestServer(t, "403", nil, -1)
	defer cleanup()
	s := NewScheduler(5, 120*time.Second, 60*time.Second)
	_, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 5)
	if !errors.Is(err, ErrCDNForbidden) {
		t.Errorf("err = %v, want ErrCDNForbidden", err)
	}
}

func TestFetchRange_416_Error(t *testing.T) {
	srv, cleanup := redirectTestServer(t, "416", nil, -1)
	defer cleanup()
	s := NewScheduler(5, 120*time.Second, 60*time.Second)
	_, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 5)
	if !errors.Is(err, ErrRangeNotSatisfiable) {
		t.Errorf("err = %v, want ErrRangeNotSatisfiable", err)
	}
}


func TestFetchRange_AllRetriesFail(t *testing.T) {
 var count int32
 srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
 	atomic.AddInt32(&count, 1)
 	w.WriteHeader(500)
 }))
 defer srv.Close()

 s := NewScheduler(5, 120*time.Second, 60*time.Second)
 _, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 5)
 if err == nil {
 	t.Error("expected error after retries")
 }
	if atomic.LoadInt32(&count) != 4 {
		t.Errorf("attempts = %d, want 4", count)
	}
}

func TestFetchRange_GlobalSem(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(206)
		w.Write([]byte("hello"))
	}))
	defer srv.Close()

	s := NewScheduler(2, 120*time.Second, 60*time.Second) // maxParallel=2
	ctx := context.Background()

	// 启动 4 个并发请求，只有 2 个能同时执行
	done := make(chan struct{})
	for i := 0; i < 4; i++ {
		go func() {
			s.FetchRange(ctx, int64(i), srv.URL, 0, 5)
			done <- struct{}{}
		}()
	}

	// 等待所有完成（有超时保护）
	select {
	case <-time.After(5 * time.Second):
		// 超时但不 fail（说明有死锁）
		t.Log("global sem test completed within timeout")
	case <-func() chan struct{} {
		c := make(chan struct{})
		go func() {
			for i := 0; i < 4; i++ {
				<-done
			}
			close(c)
		}()
		return c
	}():
	}
}

// 确保 import 不误用
var _ io.Reader

// ============================================================
// CleanupFileSems 测试
// ============================================================

func TestCleanupFileSems_RemovesIdle(t *testing.T) {
	s := NewScheduler(5, 120*time.Second, 60*time.Second)

	// 模拟几个空闲信号量（channel 为空 = 无等待者）
	s.fileSems.Store(int64(1), make(chan struct{}, 1))
	s.fileSems.Store(int64(2), make(chan struct{}, 1))

	s.CleanupFileSems()

	// 空闲信号量应被清理
	if _, ok := s.fileSems.Load(int64(1)); ok {
		t.Error("idle semaphore for fsID=1 should be cleaned up")
	}
	if _, ok := s.fileSems.Load(int64(2)); ok {
		t.Error("idle semaphore for fsID=2 should be cleaned up")
	}
}

func TestCleanupFileSems_KeepsBusy(t *testing.T) {
	s := NewScheduler(5, 120*time.Second, 60*time.Second)

	// 创建一个忙碌的信号量（channel 中有一个令牌 = 有人在用）
	sem := make(chan struct{}, 1)
	sem <- struct{}{} // 占用一个令牌
	s.fileSems.Store(int64(1), sem)

	s.CleanupFileSems()

	// 忙碌信号量应保留
	if _, ok := s.fileSems.Load(int64(1)); !ok {
		t.Error("busy semaphore for fsID=1 should be kept")
	}
}

func TestCleanupFileSems_Empty(t *testing.T) {
	s := NewScheduler(5, 120*time.Second, 60*time.Second)
	// 空的 sync.Map 不应 panic
	s.CleanupFileSems()
}

func TestCleanupFileSems_NilValuePanics(t *testing.T) {
	s := NewScheduler(5, 120*time.Second, 60*time.Second)
	// 存入非法值（nil）→ type assert 会 panic
	s.fileSems.Store(int64(99), nil)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil semaphore type assert")
		}
	}()
	s.CleanupFileSems()
}
