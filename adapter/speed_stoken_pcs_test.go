package adapter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"baidupcs-fuse/adapter/pcscore"
)

// 目的：滑动窗口样本淘汰与速度计算的边界（空/零字节/过旧样本）。
func TestSpeedTracker(t *testing.T) {
	if NewSpeedTracker(0).windowSec != 5 {
		t.Fatal("windowSec default should be 5")
	}
	tr := NewSpeedTracker(1)
	if tr.Speed() != 0 {
		t.Fatal("empty tracker speed should be 0")
	}
	tr.AddBytes(0)
	tr.AddBytes(100)
	if tr.Speed() <= 0 {
		t.Fatal("expected positive speed")
	}
	// 插入窗口外样本，AddBytes 应淘汰。
	tr.mu.Lock()
	tr.windows = append([]speedSample{{at: time.Now().Add(-time.Hour), bytes: 1000}}, tr.windows...)
	tr.mu.Unlock()
	tr.AddBytes(10)
	tr.mu.Lock()
	kept := len(tr.windows)
	tr.mu.Unlock()
	if kept > 3 {
		t.Fatalf("old samples not evicted: %d", kept)
	}
}

// 目的：人类可读单位在各量级边界上的输出契约。
func TestFormatSpeedAndSize(t *testing.T) {
	cases := map[float64]string{
		2 << 30: "2.0 GB/s",
		2 << 20: "2.0 MB/s",
		2 << 10: "2.0 KB/s",
		512:     "512 B/s",
	}
	for in, want := range cases {
		if got := FormatSpeed(in); got != want {
			t.Errorf("FormatSpeed(%v)=%q, want %q", in, got, want)
		}
	}
	sizes := map[int64]string{
		2 << 30: "2.0 GB",
		2 << 20: "2.0 MB",
		2 << 10: "2.0 KB",
		12:      "12 B",
	}
	for in, want := range sizes {
		if got := FormatSize(in); got != want {
			t.Errorf("FormatSize(%d)=%q, want %q", in, got, want)
		}
	}
}

// 目的：全局字节统计、单任务进度累加与活跃任务生命周期。
func TestTransferStats(t *testing.T) {
	s := NewTransferStats()
	s.TrackUpload("/unknown", 5)   // 无活跃任务，仅累加全局
	s.TrackDownload("/unknown", 7) // 同上
	s.StartUpload("/u", 100)
	s.StartDownload("/d", 50)
	s.TrackUpload("/u", 30)
	s.TrackDownload("/d", 20)

	if s.UploadBytes.Load() != 35 || s.DownloadBytes.Load() != 27 {
		t.Fatalf("bytes=%d/%d", s.UploadBytes.Load(), s.DownloadBytes.Load())
	}
	tasks := s.ActiveTasks()
	if len(tasks) != 2 {
		t.Fatalf("tasks=%d", len(tasks))
	}
	for _, task := range tasks {
		if task.Path == "/u" {
			if !task.IsUpload || task.Done != 30 || task.TotalSize != 100 {
				t.Fatalf("upload task=%+v", task)
			}
		} else if task.Path == "/d" {
			if task.IsUpload || task.Done != 20 || task.TotalSize != 50 {
				t.Fatalf("download task=%+v", task)
			}
		} else {
			t.Fatalf("unexpected task %+v", task)
		}
	}
	s.EndUpload("/u")
	s.EndDownload("/d")
	if len(s.ActiveTasks()) != 0 {
		t.Fatal("tasks not removed")
	}
}

// 目的：进度日志只在有活跃任务时输出，并正确区分上传/下载方向、百分比与总大小为 0 的情况。
func TestProgressLogger(t *testing.T) {
	if NewProgressLogger(NewTransferStats(), 0).interval != 3*time.Second {
		t.Fatal("default interval should be 3s")
	}

	old := log.Writer()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	stats := NewTransferStats()
	l := NewProgressLogger(stats, 10*time.Millisecond)
	l.print() // 无任务：不应输出
	if buf.Len() != 0 {
		t.Fatalf("unexpected output for idle stats: %q", buf.String())
	}

	stats.StartUpload("/u", 100)
	stats.TrackUpload("/u", 50)
	stats.StartDownload("/d", 0)
	stats.TrackDownload("/d", 10)
	l.print()
	out := buf.String()
	if !strings.Contains(out, "↑ /u") || !strings.Contains(out, "50.0%") {
		t.Fatalf("upload progress missing: %q", out)
	}
	if !strings.Contains(out, "↓ /d") || !strings.Contains(out, "0.0%") {
		t.Fatalf("download progress missing: %q", out)
	}

	// Start/Stop 只负责周期触发，不应 panic 或阻塞。
	buf.Reset()
	l.Start()
	l.Stop()
}

// ---- pcsclient ----

type fakePCSLib struct {
	meta    *pcscore.FileMeta
	metaErr error

	dir    []*pcscore.FileMeta
	dirErr error

	locate    string
	fsid      int64
	size      int64
	mtime     int64
	locateErr error

	quota, used int64
	quotaErr    error

	mkdirErr     error
	removeErr    error
	renameErr    error
	rapidErr     error
	precreateID  string
	precreateErr error
	sliceMD5     string
	sliceErr     error
	superErr     error

	stoken string
}

func (f *fakePCSLib) Stat(context.Context, string) (*pcscore.FileMeta, error) {
	return f.meta, f.metaErr
}
func (f *fakePCSLib) ReadDir(context.Context, string) ([]*pcscore.FileMeta, error) {
	return f.dir, f.dirErr
}
func (f *fakePCSLib) LocateDownload(context.Context, string) (string, int64, int64, int64, error) {
	return f.locate, f.fsid, f.size, f.mtime, f.locateErr
}
func (f *fakePCSLib) QuotaInfo(context.Context) (int64, int64, error) {
	return f.quota, f.used, f.quotaErr
}
func (f *fakePCSLib) Mkdir(context.Context, string) error          { return f.mkdirErr }
func (f *fakePCSLib) Remove(context.Context, ...string) error      { return f.removeErr }
func (f *fakePCSLib) Rename(context.Context, string, string) error { return f.renameErr }
func (f *fakePCSLib) RapidUpload(context.Context, string, int64, string, string, string, string, string, []string) error {
	return f.rapidErr
}
func (f *fakePCSLib) PrecreateUpload(context.Context, string, int64, []string) (string, error) {
	return f.precreateID, f.precreateErr
}
func (f *fakePCSLib) UploadSlice(context.Context, string, string, int, int64, []byte) (string, error) {
	return f.sliceMD5, f.sliceErr
}
func (f *fakePCSLib) CreateSuperFile(context.Context, string, string, int64, map[int]string) error {
	return f.superErr
}
func (f *fakePCSLib) UpdateStoken(token string) { f.stoken = token }

// 目的：pcscore→mount 类型转换、全部委托方法、错误透传与空目录语义。
func TestPCSClientAdapter(t *testing.T) {
	if NewPCSClient(1, "b", "s", time.Second) == nil {
		t.Fatal("NewPCSClient returned nil")
	}

	lib := &fakePCSLib{
		meta:   &pcscore.FileMeta{FsID: 1, Path: "/a", Filename: "a", Isdir: true, Size: 2, Mtime: 3, Mode: 0755},
		dir:    []*pcscore.FileMeta{{FsID: 2, Path: "/b", Filename: "b", Size: 4, Mtime: 5, Mode: 0644}},
		locate: "http://dlink", fsid: 1, size: 2, mtime: 3,
		quota: 100, used: 40,
		precreateID: "uid", sliceMD5: "md5",
	}
	a := &pcsClientAdapter{lib: lib}
	ctx := context.Background()

	e, err := a.Stat(ctx, "/a")
	if err != nil || e.FSID != 1 || !e.IsDir || e.Name != "a" || e.MtimeSec != 3 || e.Mode != 0755 {
		t.Fatalf("Stat=%+v err=%v", e, err)
	}
	entries, err := a.ReadDir(ctx, "/")
	if err != nil || len(entries) != 1 || entries[0].FSID != 2 {
		t.Fatalf("ReadDir=%+v err=%v", entries, err)
	}
	if d, fsid, size, mtime, err := a.LocateDownload(ctx, "/a"); err != nil || d != "http://dlink" || fsid != 1 || size != 2 || mtime != 3 {
		t.Fatalf("LocateDownload=%q %d %d %d %v", d, fsid, size, mtime, err)
	}
	if total, used, err := a.QuotaInfo(ctx); err != nil || total != 100 || used != 40 {
		t.Fatalf("QuotaInfo=%d/%d %v", total, used, err)
	}
	if err := a.Mkdir(ctx, "/d"); err != nil {
		t.Error(err)
	}
	if err := a.Remove(ctx, "/d"); err != nil {
		t.Error(err)
	}
	if err := a.Rename(ctx, "/a", "/b"); err != nil {
		t.Error(err)
	}
	if err := a.RapidUpload(ctx, "/a", 1, "c", "s", "", "overwrite", "", nil); err != nil {
		t.Error(err)
	}
	if id, err := a.PrecreateUpload(ctx, "/a", 1, nil); err != nil || id != "uid" {
		t.Fatalf("Precreate=%q %v", id, err)
	}
	if md5, err := a.UploadSlice(ctx, "uid", "/a", 0, 0, nil); err != nil || md5 != "md5" {
		t.Fatalf("UploadSlice=%q %v", md5, err)
	}
	if err := a.CreateSuperFile(ctx, "uid", "/a", 1, nil); err != nil {
		t.Error(err)
	}
	a.UpdateStoken("new")
	if lib.stoken != "new" {
		t.Fatalf("stoken=%q", lib.stoken)
	}

	boom := errors.New("boom")
	bad := &pcsClientAdapter{lib: &fakePCSLib{
		metaErr: boom, dirErr: boom, locateErr: boom, quotaErr: boom,
		mkdirErr: boom, removeErr: boom, renameErr: boom, rapidErr: boom,
		precreateErr: boom, sliceErr: boom, superErr: boom,
	}}
	if _, err := bad.Stat(ctx, "/a"); !errors.Is(err, boom) {
		t.Error("Stat error not propagated")
	}
	if _, err := bad.ReadDir(ctx, "/"); !errors.Is(err, boom) {
		t.Error("ReadDir error not propagated")
	}
	if _, _, _, _, err := bad.LocateDownload(ctx, "/a"); !errors.Is(err, boom) {
		t.Error("LocateDownload error not propagated")
	}
	if _, _, err := bad.QuotaInfo(ctx); !errors.Is(err, boom) {
		t.Error("QuotaInfo error not propagated")
	}
	if err := bad.Mkdir(ctx, "/d"); !errors.Is(err, boom) {
		t.Error("Mkdir error not propagated")
	}
	if err := bad.Remove(ctx, "/d"); !errors.Is(err, boom) {
		t.Error("Remove error not propagated")
	}
	if err := bad.Rename(ctx, "/a", "/b"); !errors.Is(err, boom) {
		t.Error("Rename error not propagated")
	}
	if err := bad.RapidUpload(ctx, "/a", 1, "c", "s", "", "overwrite", "", nil); !errors.Is(err, boom) {
		t.Error("RapidUpload error not propagated")
	}
	if _, err := bad.PrecreateUpload(ctx, "/a", 1, nil); !errors.Is(err, boom) {
		t.Error("PrecreateUpload error not propagated")
	}
	if _, err := bad.UploadSlice(ctx, "uid", "/a", 0, 0, nil); !errors.Is(err, boom) {
		t.Error("UploadSlice error not propagated")
	}
	if err := bad.CreateSuperFile(ctx, "uid", "/a", 1, nil); !errors.Is(err, boom) {
		t.Error("CreateSuperFile error not propagated")
	}

	// 空目录结果非 nil。
	empty := &pcsClientAdapter{lib: &fakePCSLib{}}
	if got, err := empty.ReadDir(ctx, "/"); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty ReadDir=%v %v", got, err)
	}
}

// ---- stoken ----

// 目的：初始 token、interval<=0 不启动、Stop 幂等。
func TestStokenRefresher_Basics(t *testing.T) {
	r := NewStokenRefresher("bduss", "old", time.Minute)
	if r.Token() != "old" {
		t.Fatalf("token=%q", r.Token())
	}
	// interval<=0 不启动 goroutine。
	disabled := NewStokenRefresher("bduss", "s", 0)
	disabled.Start()
	disabled.Stop()
	disabled.Stop() // 幂等
}

// 目的：启动立即刷新、Set-Cookie 更新 token、OnRefresh 回调与 BDUSS Cookie。
func TestStokenRefresher_RefreshLoop(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if !strings.Contains(r.Header.Get("Cookie"), "BDUSS=bduss") {
			t.Errorf("missing BDUSS cookie: %q", r.Header.Get("Cookie"))
		}
		http.SetCookie(w, &http.Cookie{Name: "STOKEN", Value: "new"})
		io.WriteString(w, `{"errno":0}`)
	}))
	defer srv.Close()

	r := NewStokenRefresher("bduss", "old", 5*time.Millisecond)
	r.baseURL = srv.URL
	r.client = srv.Client()
	updated := make(chan string, 1)
	r.OnRefresh = func(s string) { updated <- s }
	r.Start()
	defer r.Stop()

	select {
	case got := <-updated:
		if got != "new" {
			t.Fatalf("OnRefresh=%q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnRefresh not called")
	}
	if r.Token() != "new" {
		t.Fatalf("token=%q", r.Token())
	}
	if atomic.LoadInt32(&calls) == 0 {
		t.Fatal("refresh not called")
	}
}

// 目的：无新 cookie/同值/非 200/网络/非法 URL 均不得改变 token。
func TestStokenRefresher_RefreshBranches(t *testing.T) {
	t.Run("no new cookie", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"errno":0}`)
		}))
		defer srv.Close()
		r := NewStokenRefresher("b", "old", 0)
		r.baseURL, r.client = srv.URL, srv.Client()
		r.refresh()
		if r.Token() != "old" {
			t.Fatalf("token=%q", r.Token())
		}
	})

	t.Run("same token", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.SetCookie(w, &http.Cookie{Name: "STOKEN", Value: "old"})
		}))
		defer srv.Close()
		r := NewStokenRefresher("b", "old", 0)
		r.baseURL, r.client = srv.URL, srv.Client()
		var called int32
		r.OnRefresh = func(string) { atomic.AddInt32(&called, 1) }
		r.refresh()
		if called != 0 {
			t.Fatal("OnRefresh should not fire for unchanged token")
		}
	})

	t.Run("non-200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
		}))
		defer srv.Close()
		r := NewStokenRefresher("b", "old", 0)
		r.baseURL, r.client = srv.URL, srv.Client()
		r.refresh()
		if r.Token() != "old" {
			t.Fatal("token changed on HTTP error")
		}
	})

	t.Run("network error", func(t *testing.T) {
		r := NewStokenRefresher("b", "old", 0)
		r.baseURL = "http://127.0.0.1:1"
		r.client = &http.Client{Timeout: 100 * time.Millisecond}
		r.refresh()
		if r.Token() != "old" {
			t.Fatal("token changed on network error")
		}
	})

	t.Run("invalid request", func(t *testing.T) {
		r := NewStokenRefresher("b", "old", 0)
		r.baseURL = "http://%zz"
		r.refresh()
	})
}

// 目的：手动刷新：token 变化返回成功，未变化返回可诊断错误。
func TestStokenRefresher_RefreshNow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "STOKEN", Value: "new"})
	}))
	defer srv.Close()
	r := NewStokenRefresher("b", "old", time.Hour)
	r.baseURL, r.client = srv.URL, srv.Client()
	if err := r.RefreshNow(); err != nil {
		t.Fatalf("RefreshNow: %v", err)
	}
	if r.Token() != "new" {
		t.Fatalf("token=%q", r.Token())
	}

	unchanged := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer unchanged.Close()
	r2 := NewStokenRefresher("b", "old", time.Hour)
	r2.baseURL, r2.client = unchanged.URL, unchanged.Client()
	if err := r2.RefreshNow(); err == nil {
		t.Fatal("expected unchanged error")
	}
}

// 目的：Set-Cookie 与原始 header 的 STOKEN 解析边界，避免 XSTOKEN 误匹配。
func TestExtractSToken(t *testing.T) {
	mk := func(setCookies ...string) *http.Response {
		h := http.Header{}
		for _, c := range setCookies {
			h.Add("Set-Cookie", c)
		}
		return &http.Response{Header: h}
	}
	if got := extractSToken(mk("STOKEN=abc; Path=/")); got != "abc" {
		t.Fatalf("cookie => %q", got)
	}
	if got := extractSToken(mk("XSTOKEN=bad; stoken=good")); got != "good" {
		t.Fatalf("raw => %q", got)
	}
	if got := extractSToken(mk("XSTOKEN=bad")); got != "" {
		t.Fatalf("boundary => %q", got)
	}
	if got := extractSToken(mk("STOKEN=; path=/")); got != "" {
		t.Fatalf("empty => %q", got)
	}
	if got := extractSToken(mk()); got != "" {
		t.Fatalf("none => %q", got)
	}
	if got := extractSToken(mk("foo=1; stoken=val, other=2")); got != "val" {
		t.Fatalf("comma => %q", got)
	}
}
