package download

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newRangeServer 构造 PCS→CDN 两步 server。
// cdnStatus 为 CDN 阶段返回的状态码；body 为完整对象内容。
func newRangeServer(t *testing.T, cdnStatus int, body string, seenCookie *atomic.Value) *httptest.Server {
	t.Helper()
	var baseURL atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/cdn") {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(cdnStatus)
			_, _ = w.Write([]byte(body))
			return
		}
		if seenCookie != nil {
			seenCookie.Store(r.Header.Get("Cookie"))
		}
		w.Header().Set("Location", baseURL.Load().(string)+"/cdn")
		w.WriteHeader(http.StatusFound)
	}))
	baseURL.Store(srv.URL)
	return srv
}

// 目的：HTTP 200 + 非零 offset 时，只有响应体覆盖 off+length 才允许切片返回正确数据。
func TestFetchRange_HTTP200_RangeSlice(t *testing.T) {
	srv := newRangeServer(t, http.StatusOK, "abcdef", nil)
	defer srv.Close()

	s := NewScheduler(1, 5*time.Second, 5*time.Second)
	data, err := s.FetchRange(context.Background(), 1, srv.URL, 2, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "cde" {
		t.Fatalf("data=%q, want cde", data)
	}
}

// 目的：HTTP 200 且响应体不足 off+length 时必须报 ErrShortRead，不能返回错位数据。
func TestFetchRange_HTTP200_ShortBodyForOffset(t *testing.T) {
	srv := newRangeServer(t, http.StatusOK, "abc", nil)
	defer srv.Close()

	s := NewScheduler(1, 5*time.Second, 5*time.Second)
	_, err := s.FetchRange(context.Background(), 1, srv.URL, 2, 3)
	if !errors.Is(err, ErrShortRead) {
		t.Fatalf("err=%v, want ErrShortRead", err)
	}
}

// 目的：HTTP 200 + offset 0 必须按 length 截断，避免把整个文件交给调用方。
func TestFetchRange_HTTP200_ZeroOffsetTruncates(t *testing.T) {
	srv := newRangeServer(t, http.StatusOK, "abcdef", nil)
	defer srv.Close()

	s := NewScheduler(1, 5*time.Second, 5*time.Second)
	data, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "abc" {
		t.Fatalf("data=%q, want abc", data)
	}
}

// 目的：2xx 但非 200/206 的状态属于协议异常，必须报 unexpected status。
func TestFetchRange_CDN201_UnexpectedStatus(t *testing.T) {
	// 201 能通过 curl -f，但不是合法的 Range 响应，必须落到 default 分支报错。
	srv := newRangeServer(t, http.StatusCreated, "boom", nil)
	defer srv.Close()

	s := NewScheduler(1, 2*time.Second, 3*time.Second)
	_, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 4)
	if err == nil {
		t.Fatal("expected error for unexpected HTTP status")
	}
	if !strings.Contains(err.Error(), "unexpected HTTP status") {
		t.Fatalf("err=%v, want unexpected HTTP status", err)
	}
}

// 目的：5xx 必须失败，不能把错误页当作文件数据。
func TestFetchRange_CDN500_Error(t *testing.T) {
	srv := newRangeServer(t, http.StatusInternalServerError, "boom", nil)
	defer srv.Close()

	s := NewScheduler(1, 2*time.Second, 3*time.Second)
	if _, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 4); err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}

// 目的：length<=0 直接返回空，不发起无意义下载。
func TestFetchRange_LengthZero(t *testing.T) {
	s := NewScheduler(1, time.Second, time.Second)
	for _, n := range []int64{0, -1} {
		data, err := s.FetchRange(context.Background(), 1, "http://invalid", 0, n)
		if err != nil {
			t.Fatalf("length=%d err=%v", n, err)
		}
		if len(data) != 0 {
			t.Fatalf("length=%d data=%q, want empty", n, data)
		}
	}
}

// 目的：Shutdown 幂等且置位 IsShutdown，供卸载流程判断。
func TestScheduler_ShutdownAndState(t *testing.T) {
	s := NewScheduler(2, time.Second, time.Second)
	if s.IsShutdown() {
		t.Fatal("new scheduler should not be shutdown")
	}
	s.Shutdown()
	if !s.IsShutdown() {
		t.Fatal("IsShutdown=false after Shutdown")
	}
	// 幂等：再次 Shutdown 不应 panic。
	s.Shutdown()
}

// 目的：注入的 Cookie 等额外头必须出现在取链请求上，保证鉴权生效。
func TestScheduler_ExtraHeadersSent(t *testing.T) {
	var seen atomic.Value
	srv := newRangeServer(t, http.StatusPartialContent, "hello", &seen)
	defer srv.Close()

	s := NewScheduler(1, 5*time.Second, 5*time.Second)
	s.SetExtraHeaders(map[string]string{"Cookie": "BDUSS=test-cookie"})
	if _, err := s.FetchRange(context.Background(), 1, srv.URL, 0, 5); err != nil {
		t.Fatalf("FetchRange: %v", err)
	}
	got, _ := seen.Load().(string)
	if got != "BDUSS=test-cookie" {
		t.Fatalf("Cookie header = %q, want BDUSS=test-cookie", got)
	}
}

// 目的：并发上限只接受合法值，非法配置被忽略以避免并发降为 0。
func TestScheduler_PerFileCapAndMaxSems(t *testing.T) {
	s := NewScheduler(3, time.Second, time.Second)
	s.SetPerFileCap(4)
	sem := s.getFileSem(42)
	if cap(sem) != 4 {
		t.Fatalf("per-file cap = %d, want 4", cap(sem))
	}
	// 非法值应被忽略（保持 4），避免配置错误把并发降到 0 或负值。
	s.SetPerFileCap(0)
	if cap(s.getFileSem(42)) != 4 {
		t.Fatalf("cap changed by invalid value: %d", cap(s.getFileSem(42)))
	}
	s.SetMaxFileSems(8)
	if s.maxFileSems != 8 {
		t.Fatalf("maxFileSems = %d, want 8", s.maxFileSems)
	}
	s.SetMaxFileSems(-1)
	if s.maxFileSems != 8 {
		t.Fatalf("maxFileSems changed by invalid value: %d", s.maxFileSems)
	}
}
