package adapter

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"baidupcs-fuse/mount"
)

func testPanClient(t *testing.T, h http.HandlerFunc) (*panClient, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &panClient{
		bduss:   "test-bduss",
		stoken:  "test-stoken",
		client:  &http.Client{Timeout: 3 * time.Second},
		ua:      "test-ua",
		uid:     42,
		panBase: srv.URL,
		pcsBase: srv.URL,
	}, srv
}

// 目的：非正超时归一为默认值，避免无超时连接卡死 FUSE 请求。
func TestNormalizeHTTPTimeout(t *testing.T) {
	if got := normalizeHTTPTimeout(0); got != defaultHTTPTimeout {
		t.Fatalf("0 => %v, want %v", got, defaultHTTPTimeout)
	}
	if got := normalizeHTTPTimeout(-time.Second); got != defaultHTTPTimeout {
		t.Fatalf("negative => %v", got)
	}
	if got := normalizeHTTPTimeout(5 * time.Second); got != 5*time.Second {
		t.Fatalf("positive => %v", got)
	}
}

// 目的：生产默认域名与测试注入 URL 的优先级。
func TestBaseURLs(t *testing.T) {
	c := &panClient{}
	if c.panBaseURL() != "https://pan.baidu.com" || c.pcsBaseURL() != "https://pcs.baidu.com" {
		t.Fatalf("defaults: %q %q", c.panBaseURL(), c.pcsBaseURL())
	}
	c.SetBaseURL("http://127.0.0.1:1")
	if c.panBaseURL() != "http://127.0.0.1:1" || c.pcsBaseURL() != "http://127.0.0.1:1" {
		t.Fatalf("override failed: %q %q", c.panBaseURL(), c.pcsBaseURL())
	}
}

// 目的：构造后异步获取 UID，STOKEN 更新对客户端立即生效。
func TestNewPanClientWithBase(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/gettemplatevariable") {
			io.WriteString(w, `{"errno":0,"result":{"uk":9988}}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := newPanClientWithBase("b", "s", 0, srv.URL)
	defer c.client.CloseIdleConnections()
	if c.client.Timeout != defaultHTTPTimeout {
		t.Fatalf("timeout=%v, want default", c.client.Timeout)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.uidMu.RLock()
		uid := c.uid
		c.uidMu.RUnlock()
		if uid == 9988 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.uidMu.RLock()
	uid := c.uid
	c.uidMu.RUnlock()
	if uid != 9988 {
		t.Fatalf("async uid=%d, want 9988", uid)
	}
	if c.getStoken() != "s" {
		t.Fatalf("stoken=%q", c.getStoken())
	}

	// 空 base 时使用生产域名由 TestBaseURLs 覆盖；此处只验证 STOKEN 更新。
	c2 := newPanClientWithBase("b", "", time.Second, srv.URL)
	c2.UpdateStoken("new")
	if c2.getStoken() != "new" {
		t.Fatalf("stoken=%q", c2.getStoken())
	}
}

// 目的：UID 接口的成功/errno/零值/坏 JSON/HTTP 错误语义。
func TestFetchUIDContext(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		status  int
		wantUID uint64
		wantErr bool
	}{
		{"ok", `{"errno":0,"result":{"uk":123}}`, 200, 123, false},
		{"errno", `{"errno":-6}`, 200, 0, true},
		{"zero", `{"errno":0,"result":{"uk":0}}`, 200, 0, true},
		{"bad json", `not json`, 200, 0, true},
		{"http 500", `boom`, 500, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Path, "/api/gettemplatevariable") {
					t.Errorf("unexpected path %s", r.URL.Path)
				}
				w.WriteHeader(tt.status)
				io.WriteString(w, tt.body)
			})
			uid, err := c.fetchUIDContext(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, tt.wantErr)
			}
			if uid != tt.wantUID {
				t.Fatalf("uid=%d, want %d", uid, tt.wantUID)
			}
		})
	}
}

// 目的：UID 缓存、30s 负缓存、懒重试，以及调用方取消不污染缓存。
func TestGetUID(t *testing.T) {
	t.Run("cached", func(t *testing.T) {
		c := &panClient{uid: 7}
		uid, err := c.getUID(context.Background())
		if err != nil || uid != 7 {
			t.Fatalf("uid=%d err=%v", uid, err)
		}
	})

	t.Run("negative cache", func(t *testing.T) {
		c := &panClient{uidAt: time.Now(), uidErr: errors.New("previous failure")}
		if _, err := c.getUID(context.Background()); err == nil || !strings.Contains(err.Error(), "previous") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("not ready", func(t *testing.T) {
		c := &panClient{uidAt: time.Now()}
		if _, err := c.getUID(context.Background()); err == nil || !strings.Contains(err.Error(), "not ready") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("lazy retry success", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"result":{"uk":555}}`)
		})
		c.uid = 0
		c.uidAt = time.Now().Add(-time.Minute)
		uid, err := c.getUID(context.Background())
		if err != nil || uid != 555 {
			t.Fatalf("uid=%d err=%v", uid, err)
		}
		if c.uid != 555 {
			t.Fatalf("uid not cached: %d", c.uid)
		}
	})

	t.Run("lazy retry failure cached", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-6}`)
		})
		c.uid = 0
		c.uidAt = time.Now().Add(-time.Minute)
		if _, err := c.getUID(context.Background()); err == nil {
			t.Fatal("expected error")
		}
		c.uidMu.RLock()
		defer c.uidMu.RUnlock()
		if c.uidErr == nil || c.uidAt.IsZero() {
			t.Fatal("failure should be negative-cached")
		}
	})

	t.Run("caller cancel not cached", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
		})
		c.uid = 0
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := c.getUID(ctx); err == nil {
			t.Fatal("expected error")
		}
		c.uidMu.RLock()
		defer c.uidMu.RUnlock()
		if !c.uidAt.IsZero() || c.uidErr != nil {
			t.Fatal("caller cancel should not be cached")
		}
	})
}

// 目的：设备标识格式与 locatedownload 签名的不变量（长度/大写/确定性）。
func TestDevUIDAndSign(t *testing.T) {
	d := devUID("bduss")
	if d != strings.ToUpper(d) || !strings.HasSuffix(d, "|0") || len(d) != 34 {
		t.Fatalf("devUID=%q", d)
	}
	ts, rand, dev := locateDownloadSign(9, "bduss")
	if len(ts) < 10 || len(rand) != 40 || dev != d {
		t.Fatalf("ts=%q rand=%q dev=%q", ts, rand, dev)
	}
}

// 目的：验证 locatedownload 的优先选择非加密链接、无链接/全加密/errno/HTTP 错误的失败语义。
func TestGetDlinkLocateDownload(t *testing.T) {
	t.Run("success prefers unencrypted", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || !strings.Contains(r.URL.RawQuery, "method=locatedownload") {
				t.Errorf("unexpected request %s %s", r.Method, r.URL)
			}
			if !strings.Contains(r.Header.Get("Cookie"), "BDUSS=test-bduss") {
				t.Errorf("missing cookie: %q", r.Header.Get("Cookie"))
			}
			io.WriteString(w, `{"errno":0,"urls":[{"url":"http://enc","encrypt":1},{"url":"http://plain","encrypt":0}]}`)
		})
		dlink, err := c.getDlinkLocateDownload(context.Background(), "/a b")
		if err != nil || dlink != "http://plain" {
			t.Fatalf("dlink=%q err=%v", dlink, err)
		}
	})

	t.Run("all encrypted", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"urls":[{"url":"http://enc","encrypt":1}]}`)
		})
		if _, err := c.getDlinkLocateDownload(context.Background(), "/a"); err == nil || !strings.Contains(err.Error(), "encrypted") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("no urls", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"urls":[]}`)
		})
		if _, err := c.getDlinkLocateDownload(context.Background(), "/a"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("errno", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-9}`)
		})
		_, err := c.getDlinkLocateDownload(context.Background(), "/a")
		var be *mount.BaiduError
		if !errors.As(err, &be) || be.Errno != -9 {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("http and bad json", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(502)
			io.WriteString(w, strings.Repeat("x", 300))
		})
		if _, err := c.getDlinkLocateDownload(context.Background(), "/a"); err == nil || !strings.Contains(err.Error(), "HTTP 502") {
			t.Fatalf("err=%v", err)
		}

		c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "bad json")
		})
		if _, err := c2.getDlinkLocateDownload(context.Background(), "/a"); err == nil || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("uid unavailable", func(t *testing.T) {
		c := &panClient{uidAt: time.Now(), uidErr: errors.New("no uid")}
		if _, err := c.getDlinkLocateDownload(context.Background(), "/a"); err == nil || !strings.Contains(err.Error(), "UID") {
			t.Fatalf("err=%v", err)
		}
	})
}

// 目的：验证下载链接获取的请求构造（method=download + 路径转义）以及 ctx 取消立即失败。
func TestGetDlinkDownload(t *testing.T) {
	c := &panClient{pcsBase: "http://pcs"}
	dlink, err := c.getDlinkDownload(context.Background(), "/a b")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dlink, "method=download") || !strings.Contains(dlink, "path=%2Fa+b") {
		t.Fatalf("dlink=%q", dlink)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.getDlinkDownload(ctx, "/a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

// 目的：根目录、filemetas 命中、目录/文件 mode、回退 list 与 ENOENT 语义。
func TestStat(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		c := &panClient{}
		for _, p := range []string{"/", ""} {
			e, err := c.Stat(context.Background(), p)
			if err != nil || !e.IsDir || e.Mode != 0755 || e.Path != "/" {
				t.Fatalf("root %q => %+v, %v", p, e, err)
			}
		}
	})

	t.Run("filemetas file", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, "/api/filemetas") {
				t.Errorf("unexpected %s", r.URL.Path)
			}
			io.WriteString(w, `{"errno":0,"info":[{"fs_id":7,"server_filename":"a.txt","path":"/a.txt","isdir":0,"size":12,"server_mtime":100}]}`)
		})
		e, err := c.Stat(context.Background(), "/a.txt")
		if err != nil {
			t.Fatal(err)
		}
		if e.FSID != 7 || e.Name != "a.txt" || e.IsDir || e.Size != 12 || e.MtimeSec != 100 || e.Mode != 0644 {
			t.Fatalf("entry=%+v", e)
		}
	})

	t.Run("filemetas dir", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"info":[{"fs_id":1,"server_filename":"d","path":"/d","isdir":1}]}`)
		})
		e, err := c.Stat(context.Background(), "/d")
		if err != nil || !e.IsDir || e.Mode != 0755 {
			t.Fatalf("entry=%+v err=%v", e, err)
		}
	})

	t.Run("fallback to list", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/filemetas") {
				io.WriteString(w, `{"errno":-9}`)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/api/list") {
				io.WriteString(w, `{"errno":0,"list":[{"fs_id":3,"server_filename":"b","path":"/b","size":5}]}`)
				return
			}
			http.NotFound(w, r)
		})
		e, err := c.Stat(context.Background(), "/b")
		if err != nil || e.FSID != 3 {
			t.Fatalf("entry=%+v err=%v", e, err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/api/filemetas") {
				io.WriteString(w, `{"errno":0,"info":[]}`)
				return
			}
			io.WriteString(w, `{"errno":0,"list":[]}`)
		})
		if _, err := c.Stat(context.Background(), "/missing"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v", err)
		}
	})
}

// 目的：通过父目录列表查找文件，覆盖命中与未命中。
func TestStatByList(t *testing.T) {
	c := &panClient{}
	e, err := c.statByList(context.Background(), "/")
	if err != nil || !e.IsDir {
		t.Fatalf("root=%+v err=%v", e, err)
	}

	c, _ = testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"errno":0,"list":[{"fs_id":1,"server_filename":"x","path":"/x"}]}`)
	})
	if _, err := c.statByList(context.Background(), "/nope"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err=%v", err)
	}
}

// 目的：分页、去重、排序、服务端忽略 start 的防御，以及解码/errno/网络错误。
func TestReadDir(t *testing.T) {
	t.Run("pagination and sort", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			start := r.URL.Query().Get("start")
			if start == "0" {
				var b strings.Builder
				b.WriteString(`{"errno":0,"list":[`)
				for i := 0; i < 1000; i++ {
					if i > 0 {
						b.WriteByte(',')
					}
					fmt.Fprintf(&b, `{"fs_id":%d,"server_filename":"f%04d","path":"/f%04d"}`, i+1, i, i)
				}
				b.WriteString(`]}`)
				io.WriteString(w, b.String())
				return
			}
			io.WriteString(w, `{"errno":0,"list":[{"fs_id":1001,"server_filename":"zzz","path":"/zzz"}]}`)
		})
		entries, err := c.ReadDir(context.Background(), "/")
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1001 {
			t.Fatalf("len=%d, want 1001", len(entries))
		}
		if entries[len(entries)-1].Name != "zzz" {
			t.Fatalf("not sorted: last=%q", entries[len(entries)-1].Name)
		}
	})

	t.Run("dedup", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"list":[{"fs_id":1,"server_filename":"a"},{"fs_id":1,"server_filename":"a2"}]}`)
		})
		entries, err := c.ReadDir(context.Background(), "/")
		if err != nil || len(entries) != 1 {
			t.Fatalf("entries=%d err=%v", len(entries), err)
		}
	})

	t.Run("repeated page breaks", func(t *testing.T) {
		var calls int32
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			var b strings.Builder
			b.WriteString(`{"errno":0,"list":[`)
			for i := 0; i < 1000; i++ {
				if i > 0 {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, `{"fs_id":%d,"server_filename":"f%04d"}`, i+1, i)
			}
			b.WriteString(`]}`)
			io.WriteString(w, b.String())
		})
		entries, err := c.ReadDir(context.Background(), "/")
		if err != nil || len(entries) != 1000 {
			t.Fatalf("entries=%d err=%v", len(entries), err)
		}
		if calls != 2 {
			t.Fatalf("calls=%d, want 2 (second page all duplicates)", calls)
		}
	})

	t.Run("decode error and errno", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "bad")
		})
		if _, err := c.ReadDir(context.Background(), "/"); err == nil || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("err=%v", err)
		}

		c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-6}`)
		})
		if _, err := c2.ReadDir(context.Background(), "/"); err == nil {
			t.Fatal("expected errno error")
		}
	})

	t.Run("network error", func(t *testing.T) {
		c := &panClient{client: &http.Client{Timeout: 100 * time.Millisecond}, panBase: "http://127.0.0.1:1"}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := c.ReadDir(ctx, "/"); err == nil {
			t.Fatal("expected error")
		}
	})
}

// 目的：Stat 与取链组合的成功路径及错误传播。
func TestLocateDownload(t *testing.T) {
	c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/filemetas"):
			io.WriteString(w, `{"errno":0,"info":[{"fs_id":9,"size":10,"server_mtime":3}]}`)
		case strings.HasPrefix(r.URL.Path, "/rest/2.0/pcs/file"):
			io.WriteString(w, `{"errno":0,"urls":[{"url":"http://cdn","encrypt":0}]}`)
		default:
			http.NotFound(w, r)
		}
	})
	dlink, fsid, size, mtime, err := c.LocateDownload(context.Background(), "/a")
	if err != nil || dlink != "http://cdn" || fsid != 9 || size != 10 || mtime != 3 {
		t.Fatalf("got %q %d %d %d %v", dlink, fsid, size, mtime, err)
	}

	c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"errno":-9}`)
	})
	if _, _, _, _, err := c2.LocateDownload(context.Background(), "/a"); err == nil {
		t.Fatal("expected stat error")
	}
}

// 目的：配额成功路径，以及网络/解码/errno 失败时降级默认值。
func TestQuotaInfo(t *testing.T) {
	c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"errno":0,"quota":{"total":100,"used":40}}`)
	})
	total, used, err := c.QuotaInfo(context.Background())
	if err != nil || total != 100 || used != 40 {
		t.Fatalf("quota=%d/%d err=%v", total, used, err)
	}

	const fallback = 5 * 1024 * 1024 * 1024 * 1024
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if total, used, err := c.QuotaInfo(ctx); err != nil || total != fallback || used != 0 {
		t.Fatalf("network fallback=%d/%d err=%v", total, used, err)
	}

	c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "bad")
	})
	if total, _, err := c2.QuotaInfo(context.Background()); err != nil || total != fallback {
		t.Fatalf("decode fallback=%d err=%v", total, err)
	}

	c3, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"errno":-6}`)
	})
	if total, _, err := c3.QuotaInfo(context.Background()); err != nil || total != fallback {
		t.Fatalf("errno fallback=%d err=%v", total, err)
	}
}

// 目的：目录创建的幂等 EEXIST、其他 errno、HTTP/解码/网络错误与 Cookie 注入。
func TestMkdir(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var gotCookie, gotMethod string
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			gotCookie = r.Header.Get("Cookie")
			io.WriteString(w, `{"errno":0}`)
		})
		if err := c.Mkdir(context.Background(), "/d"); err != nil {
			t.Fatal(err)
		}
		if gotMethod != http.MethodPost || !strings.Contains(gotCookie, "STOKEN=test-stoken") {
			t.Fatalf("method=%q cookie=%q", gotMethod, gotCookie)
		}
	})

	t.Run("exists is idempotent", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-8}`)
		})
		if err := c.Mkdir(context.Background(), "/d"); err != nil {
			t.Fatalf("EEXIST should be nil, got %v", err)
		}
	})

	t.Run("other errno", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-9}`)
		})
		var be *mount.BaiduError
		if err := c.Mkdir(context.Background(), "/d"); !errors.As(err, &be) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("http error and bad json", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(503)
			io.WriteString(w, strings.Repeat("e", 300))
		})
		if err := c.Mkdir(context.Background(), "/d"); err == nil || !strings.Contains(err.Error(), "HTTP 503") {
			t.Fatalf("err=%v", err)
		}

		c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "bad")
		})
		if err := c2.Mkdir(context.Background(), "/d"); err == nil || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("network error", func(t *testing.T) {
		c := &panClient{client: &http.Client{Timeout: 100 * time.Millisecond}, pcsBase: "http://127.0.0.1:1", bduss: "b"}
		if err := c.Mkdir(context.Background(), "/d"); err == nil {
			t.Fatal("expected error")
		}
	})
}

// 目的：批量删除参数、ENOENT 幂等、其他 errno 与 HTTP/解码/网络错误。
func TestRemove(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var body string
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			if err := r.ParseMultipartForm(1 << 20); err == nil {
				body = r.FormValue("param")
			}
			io.WriteString(w, `{"errno":0}`)
		})
		if err := c.Remove(context.Background(), "/a", "/b"); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(body, `"/a"`) || !strings.Contains(body, `"/b"`) {
			t.Fatalf("param=%q", body)
		}
	})

	t.Run("not found is idempotent", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-9}`)
		})
		if err := c.Remove(context.Background(), "/a"); err != nil {
			t.Fatalf("ENOENT should be nil, got %v", err)
		}
	})

	t.Run("other errno and http error", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-6}`)
		})
		if err := c.Remove(context.Background(), "/a"); err == nil {
			t.Fatal("expected error")
		}

		c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			io.WriteString(w, strings.Repeat("x", 300))
		})
		if err := c2.Remove(context.Background(), "/a"); err == nil || !strings.Contains(err.Error(), "HTTP 500") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("bad json", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "bad")
		})
		if err := c.Remove(context.Background(), "/a"); err == nil || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("network error", func(t *testing.T) {
		c := &panClient{client: &http.Client{Timeout: 100 * time.Millisecond}, pcsBase: "http://127.0.0.1:1", bduss: "b"}
		if err := c.Remove(context.Background(), "/a"); err == nil {
			t.Fatal("expected error")
		}
	})
}

// 目的：重命名表单请求、errno、解码错误及 ctx 取消。
func TestRename(t *testing.T) {
	c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.Contains(r.Header.Get("Content-Type"), "form-urlencoded") {
			t.Errorf("unexpected request %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		io.WriteString(w, `{"errno":0}`)
	})
	if err := c.Rename(context.Background(), "/a", "/b"); err != nil {
		t.Fatal(err)
	}

	c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"errno":-8}`)
	})
	if err := c2.Rename(context.Background(), "/a", "/b"); err == nil {
		t.Fatal("expected error")
	}

	c3, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "bad")
	})
	if err := c3.Rename(context.Background(), "/a", "/b"); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("err=%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c3.Rename(ctx, "/a", "/b"); err == nil {
		t.Fatal("expected network/context error")
	}
}

// 目的：秒传带/不带 uploadid、未命中需真实上传、errno/解码/网络错误。
func TestRapidUpload(t *testing.T) {
	t.Run("success with uploadID", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"return_type":2}`)
		})
		if err := c.RapidUpload(context.Background(), "/a", 10, "cm", "sm", "", "overwrite", "uid", []string{"b1"}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("success without uploadID", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"return_type":2}`)
		})
		if err := c.RapidUpload(context.Background(), "/a", 10, "cm", "sm", "", "skip", "", nil); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("need upload", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"return_type":1}`)
		})
		if err := c.RapidUpload(context.Background(), "/a", 10, "cm", "sm", "", "overwrite", "", nil); !errors.Is(err, mount.ErrNeedUpload) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("errno and bad json", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-9}`)
		})
		if err := c.RapidUpload(context.Background(), "/a", 10, "cm", "sm", "", "overwrite", "", nil); err == nil {
			t.Fatal("expected errno error")
		}

		c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "bad")
		})
		if err := c2.RapidUpload(context.Background(), "/a", 10, "cm", "sm", "", "overwrite", "", nil); err == nil || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("network error", func(t *testing.T) {
		c := &panClient{client: &http.Client{Timeout: 100 * time.Millisecond}, panBase: "http://127.0.0.1:1", bduss: "b"}
		if err := c.RapidUpload(context.Background(), "/a", 1, "c", "s", "", "overwrite", "", nil); err == nil {
			t.Fatal("expected error")
		}
	})
}

// 目的：预创建返回 uploadid、秒传成功空 id、空 id 错误与失败路径。
func TestPrecreateUpload(t *testing.T) {
	t.Run("uploadid", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"return_type":1,"uploadid":"uid123"}`)
		})
		id, err := c.PrecreateUpload(context.Background(), "/a", 10, []string{"b"})
		if err != nil || id != "uid123" {
			t.Fatalf("id=%q err=%v", id, err)
		}
	})

	t.Run("rapid success", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"return_type":2}`)
		})
		id, err := c.PrecreateUpload(context.Background(), "/a", 10, nil)
		if err != nil || id != "" {
			t.Fatalf("id=%q err=%v", id, err)
		}
	})

	t.Run("empty uploadid", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0,"return_type":1}`)
		})
		if _, err := c.PrecreateUpload(context.Background(), "/a", 10, nil); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("errno, bad json, network", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-6}`)
		})
		if _, err := c.PrecreateUpload(context.Background(), "/a", 10, nil); err == nil {
			t.Fatal("expected errno error")
		}

		c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "bad")
		})
		if _, err := c2.PrecreateUpload(context.Background(), "/a", 10, nil); err == nil || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("err=%v", err)
		}

		c3 := &panClient{client: &http.Client{Timeout: 100 * time.Millisecond}, panBase: "http://127.0.0.1:1", bduss: "b"}
		if _, err := c3.PrecreateUpload(context.Background(), "/a", 10, nil); err == nil {
			t.Fatal("expected network error")
		}
	})
}

// 目的：分片上传参数/Cookie/multipart、MD5 校验及 API/HTTP/解码/网络错误。
func TestUploadSlice(t *testing.T) {
	data := []byte("hello-slice")
	sum := md5.Sum(data)
	wantMD5 := hex.EncodeToString(sum[:])

	t.Run("success", func(t *testing.T) {
		var gotPath, gotCookie, gotCT string
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.RawQuery
			gotCookie = r.Header.Get("Cookie")
			gotCT = r.Header.Get("Content-Type")
			_ = r.ParseMultipartForm(1 << 20)
			io.WriteString(w, `{"error_code":0,"md5":"`+wantMD5+`"}`)
		})
		md5Hex, err := c.UploadSlice(context.Background(), "uid", "/a", 2, 512, data)
		if err != nil || md5Hex != wantMD5 {
			t.Fatalf("md5=%q err=%v", md5Hex, err)
		}
		if !strings.Contains(gotPath, "partseq=2") || !strings.Contains(gotPath, "partoffset=512") {
			t.Fatalf("query=%q", gotPath)
		}
		if !strings.Contains(gotCookie, "STOKEN=") || !strings.HasPrefix(gotCT, "multipart/form-data") {
			t.Fatalf("cookie=%q ct=%q", gotCookie, gotCT)
		}
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"error_code":0,"md5":"deadbeef"}`)
		})
		if _, err := c.UploadSlice(context.Background(), "uid", "/a", 0, 0, data); err == nil || !strings.Contains(err.Error(), "mismatch") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("missing md5", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"error_code":0}`)
		})
		if _, err := c.UploadSlice(context.Background(), "uid", "/a", 0, 0, data); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("error_code", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"error_code":-6,"error_msg":"denied"}`)
		})
		var be *mount.BaiduError
		if _, err := c.UploadSlice(context.Background(), "uid", "/a", 0, 0, data); !errors.As(err, &be) || be.Msg != "denied" {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("http json error", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			io.WriteString(w, `{"errno":-8,"errmsg":"exists"}`)
		})
		var be *mount.BaiduError
		if _, err := c.UploadSlice(context.Background(), "uid", "/a", 0, 0, data); !errors.As(err, &be) || be.Errno != -8 {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("http plain error", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(502)
			io.WriteString(w, "bad gateway")
		})
		if _, err := c.UploadSlice(context.Background(), "uid", "/a", 0, 0, data); err == nil || !strings.Contains(err.Error(), "HTTP 502") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("bad json and network", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "bad")
		})
		if _, err := c.UploadSlice(context.Background(), "uid", "/a", 0, 0, data); err == nil || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("err=%v", err)
		}

		c2 := &panClient{client: &http.Client{Timeout: 100 * time.Millisecond}, pcsBase: "http://127.0.0.1:1", bduss: "b"}
		if _, err := c2.UploadSlice(context.Background(), "uid", "/a", 0, 0, data); err == nil {
			t.Fatal("expected network error")
		}
	})

	t.Run("debug env", func(t *testing.T) {
		t.Setenv("BDFS_DEBUG", "1")
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"error_code":0,"md5":"`+wantMD5+`"}`)
		})
		if _, err := c.UploadSlice(context.Background(), "uid", "/a", 0, 0, data); err != nil {
			t.Fatal(err)
		}
	})
}

// 目的：根目录与嵌套路径的 target_path 构造，以及 errno/解码/网络错误。
func TestCreateSuperFile(t *testing.T) {
	t.Run("success root and nested", func(t *testing.T) {
		for _, path := range []string{"/a", "/dir/a"} {
			var form string
			c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				form = string(b)
				io.WriteString(w, `{"errno":0,"fs_id":1}`)
			})
			if err := c.CreateSuperFile(context.Background(), "uid", path, 10, map[int]string{0: "m0", 1: "m1"}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(form, "block_list") {
				t.Fatalf("form=%q", form)
			}
		}
	})

	t.Run("errno, bad json, network", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":-11}`)
		})
		if err := c.CreateSuperFile(context.Background(), "uid", "/a", 10, nil); err == nil {
			t.Fatal("expected errno error")
		}

		c2, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "bad")
		})
		if err := c2.CreateSuperFile(context.Background(), "uid", "/a", 10, nil); err == nil || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("err=%v", err)
		}

		c3 := &panClient{client: &http.Client{Timeout: 100 * time.Millisecond}, panBase: "http://127.0.0.1:1", bduss: "b"}
		if err := c3.CreateSuperFile(context.Background(), "uid", "/a", 10, nil); err == nil {
			t.Fatal("expected network error")
		}
	})
}

// 目的：验证秒传/分片请求的 block_list 序列化、policy 到 rtype 的映射，以及 multipart 分片内容。
func TestUploadParameterEncoding(t *testing.T) {
	if got := mergeBlockList(); got != "[]" {
		t.Fatalf("empty=%q", got)
	}
	if got := mergeBlockList("a", "b"); got != `["a","b"]` {
		t.Fatalf("blocks=%q", got)
	}

	cases := map[string]string{
		"overwrite": "2", "rsync": "2", "skip": "1", "rename": "3", "unknown": "2", "": "2",
	}
	for policy, want := range cases {
		if got := policyToRtype(policy); got != want {
			t.Errorf("policyToRtype(%q)=%q, want %q", policy, got, want)
		}
	}

	if got := sortBlockList(nil); got != nil {
		t.Fatalf("nil map => %v", got)
	}
	got := sortBlockList(map[int]string{0: "a", 2: "c", 1: "b"})
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("sortBlockList=%v", got)
	}

	rd, ct := buildMultipartBody([]byte("payload"))
	body, _ := io.ReadAll(rd)
	if !strings.HasPrefix(ct, "multipart/form-data") || !strings.Contains(string(body), "payload") {
		t.Fatalf("ct=%q body=%q", ct, body)
	}
}

// 目的：仅对 5xx/429 重试、4xx 快速失败、ctx 取消与非法 URL 处理。
func TestDoWithRetry(t *testing.T) {
	t.Run("500 then success", func(t *testing.T) {
		var n int32
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			if atomic.AddInt32(&n, 1) == 1 {
				w.WriteHeader(500)
				io.WriteString(w, "boom")
				return
			}
			io.WriteString(w, `{"errno":0}`)
		})
		resp, err := c.get(context.Background(), c.panBaseURL()+"/x")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if n != 2 {
			t.Fatalf("attempts=%d, want 2", n)
		}
	})

	t.Run("429 retry-after", func(t *testing.T) {
		var n int32
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			if atomic.AddInt32(&n, 1) == 1 {
				w.Header().Set("Retry-After", "999")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			io.WriteString(w, `{"errno":0}`)
		})
		resp, err := c.get(context.Background(), c.panBaseURL()+"/x")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if n != 2 {
			t.Fatalf("attempts=%d", n)
		}
	})

	t.Run("4xx immediate", func(t *testing.T) {
		var n int32
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&n, 1)
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"errno":-6,"errmsg":"denied"}`)
		})
		_, err := c.get(context.Background(), c.panBaseURL()+"/x")
		var be *mount.BaiduError
		if !errors.As(err, &be) || be.Errno != -6 {
			t.Fatalf("err=%v", err)
		}
		if n != 1 {
			t.Fatalf("attempts=%d, want 1", n)
		}
	})

	t.Run("non-json http error", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, strings.Repeat("x", 300))
		})
		if _, err := c.get(context.Background(), c.panBaseURL()+"/x"); err == nil || !strings.Contains(err.Error(), "HTTP 400") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("ctx canceled during backoff", func(t *testing.T) {
		c, _ := testPanClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if _, err := c.get(ctx, c.panBaseURL()+"/x"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("invalid url", func(t *testing.T) {
		c := &panClient{client: &http.Client{}, ua: "x"}
		if _, err := c.get(context.Background(), "http://%zz"); err == nil {
			t.Fatal("expected request build error")
		}
	})
}
