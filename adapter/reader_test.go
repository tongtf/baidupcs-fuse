package adapter

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"baidupcs-fuse/download"
	"baidupcs-fuse/mount"
)

// seqPCSClient 让 Stat 按调用顺序返回不同版本，用于制造版本冲突。
type seqPCSClient struct {
	*MockPCSClient
	entries []*mount.RemoteEntry
	idx     int32
}

func (c *seqPCSClient) Stat(context.Context, string) (*mount.RemoteEntry, error) {
	i := int(atomic.AddInt32(&c.idx, 1)) - 1
	if i >= len(c.entries) {
		i = len(c.entries) - 1
	}
	return c.entries[i], nil
}

func newBaseFS(client PCSClient) *BaiduCloudFS {
	return NewBaiduCloudFS(client, nil, "acct")
}

// 目的：块缓存版本递增、非法 fsID 忽略与 nil map 初始化。
func TestBlockEpoch(t *testing.T) {
	fs := NewBaiduCloudFS(&MockPCSClient{}, nil, "a")
	if got := fs.BlockEpoch(1); got != 0 {
		t.Fatalf("initial epoch=%d", got)
	}
	fs.BumpBlockEpoch(0)
	fs.BumpBlockEpoch(-1)
	if got := fs.BlockEpoch(1); got != 0 {
		t.Fatalf("invalid fsID changed epoch: %d", got)
	}
	fs.BumpBlockEpoch(1)
	fs.BumpBlockEpoch(1)
	if got := fs.BlockEpoch(1); got != 2 {
		t.Fatalf("epoch=%d, want 2", got)
	}
	// nil map 也可用。
	fs2 := &BaiduCloudFS{}
	fs2.BumpBlockEpoch(3)
	if fs2.BlockEpoch(3) != 1 {
		t.Fatal("nil map not initialized")
	}
}

// 目的：未注入 fetcher 时明确报错，注入后参数原样透传。
func TestFetchRangeInjection(t *testing.T) {
	fs := newBaseFS(&MockPCSClient{})
	if _, err := fs.fetchRange(context.Background(), 1, "d", 0, 1); err == nil {
		t.Fatal("expected error when fetcher not injected")
	}
	fs.SetFetcher(func(_ context.Context, _ int64, _ string, off, length int64) ([]byte, error) {
		return []byte{byte(off), byte(length)}, nil
	})
	data, err := fs.fetchRange(context.Background(), 1, "d", 2, 3)
	if err != nil || len(data) != 2 || data[0] != 2 || data[1] != 3 {
		t.Fatalf("data=%v err=%v", data, err)
	}
}

// 目的：OpenRead 的 stat 缓存、初始/校验错误、nil 条目、版本冲突失效、取链错误与统计启动。
func TestOpenRead_Branches(t *testing.T) {
	t.Run("stat cache hit and background refresh", func(t *testing.T) {
		entry := &mount.RemoteEntry{FSID: 1, Size: 10, MtimeSec: 5}
		client := &MockPCSClient{
			statResult: entry,
			dlink:      "http://d", dlinkFsID: 1, dlinkSize: 10, dlinkMtime: 5,
		}
		fs := NewBaiduCloudFS(client, NewDlinkCache(30*time.Minute), "a")
		fs.SetStatCache(func(string) (*mount.RemoteEntry, bool) { return entry, true })
		fs.SetTransferStats(NewTransferStats())
		r, err := fs.OpenRead(context.Background(), "/a")
		if err != nil {
			t.Fatal(err)
		}
		if r.Size() != 10 || r.FSID() != 1 || r.Mtime() != 5 {
			t.Fatalf("reader=%d/%d/%d", r.Size(), r.FSID(), r.Mtime())
		}
		if ep, ok := r.(interface{ CacheEpoch() uint64 }); !ok || ep.CacheEpoch() != 0 {
			t.Fatalf("cache epoch missing or non-zero")
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("initial stat error", func(t *testing.T) {
		fs := newBaseFS(&MockPCSClient{statErr: os.ErrNotExist})
		if _, err := fs.OpenRead(context.Background(), "/a"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("initial nil entry", func(t *testing.T) {
		fs := newBaseFS(&MockPCSClient{})
		if _, err := fs.OpenRead(context.Background(), "/a"); err == nil {
			t.Fatal("expected nil entry error")
		}
	})

	t.Run("verify nil entry", func(t *testing.T) {
		entry := &mount.RemoteEntry{FSID: 1, Size: 1}
		fs := newBaseFS(&MockPCSClient{dlink: "http://d", dlinkFsID: 1, dlinkSize: 1})
		fs.SetStatCache(func(string) (*mount.RemoteEntry, bool) { return entry, true })
		// client.Stat 返回 nil；resolveDlink 的 GetDlinkDownload 失败后 Locate 成功。
		if _, err := fs.OpenRead(context.Background(), "/a"); err == nil {
			t.Fatal("expected verify nil error")
		}
	})

	t.Run("verify stat error", func(t *testing.T) {
		// 首次 Stat 成功、校验 Stat 失败：OpenRead 必须返回校验错误，不能返回半初始化 reader。
		client := &statErrAfterFirst{MockPCSClient: &MockPCSClient{}, first: &mount.RemoteEntry{FSID: 1, Size: 1}}
		fs := newBaseFS(client)
		if _, err := fs.OpenRead(context.Background(), "/a"); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("err=%v, want ErrPermission", err)
		}
	})

	t.Run("version conflict invalidates meta", func(t *testing.T) {
		client := &seqPCSClient{
			MockPCSClient: &MockPCSClient{dlink: "http://d", dlinkFsID: 1, dlinkSize: 10, dlinkMtime: 1},
			entries: []*mount.RemoteEntry{
				{FSID: 1, Size: 10, MtimeSec: 1},
				{FSID: 1, Size: 20, MtimeSec: 2},
			},
		}
		fs := newBaseFS(client)
		var invalidated int32
		fs.SetMetaInvalidate(func(path string) {
			if path != "/a" {
				t.Errorf("invalidate path=%q", path)
			}
			atomic.AddInt32(&invalidated, 1)
		})
		if _, err := fs.OpenRead(context.Background(), "/a"); !errors.Is(err, mount.ErrVersionConflict) {
			t.Fatalf("err=%v", err)
		}
		if invalidated != 1 {
			t.Fatalf("invalidate calls=%d", invalidated)
		}
	})

	t.Run("dlink error", func(t *testing.T) {
		client := &MockPCSClient{
			statResult: &mount.RemoteEntry{FSID: 1, Size: 10},
			dlinkErr:   os.ErrNotExist,
		}
		fs := newBaseFS(client)
		if _, err := fs.OpenRead(context.Background(), "/a"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v", err)
		}
	})
}

type statErrAfterFirst struct {
	*MockPCSClient
	calls int32
	first *mount.RemoteEntry
}

func (c *statErrAfterFirst) Stat(context.Context, string) (*mount.RemoteEntry, error) {
	if atomic.AddInt32(&c.calls, 1) == 1 {
		return c.first, nil
	}
	return nil, os.ErrPermission
}

// 目的：关闭、越界、短读、普通错误，以及 403/416 刷新 dlink 后仅重试一次。
func TestRemoteReader_ReadAt(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		r := &remoteReader{}
		r.closed.Store(true)
		if _, err := r.ReadAt(context.Background(), 0, 1); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("invalid ranges", func(t *testing.T) {
		r := &remoteReader{size: 10}
		cases := [][2]int64{{-1, 1}, {0, -1}, {11, 0}, {5, 6}}
		for _, c := range cases {
			if _, err := r.ReadAt(context.Background(), c[0], int(c[1])); err == nil {
				t.Errorf("range %v should fail", c)
			}
		}
	})

	t.Run("success and short read", func(t *testing.T) {
		r := &remoteReader{size: 10, fsID: 1, dlink: "d"}
		r.fetcher = func(_ context.Context, _ int64, _ string, _, length int64) ([]byte, error) {
			return make([]byte, length), nil
		}
		data, err := r.ReadAt(context.Background(), 2, 3)
		if err != nil || len(data) != 3 {
			t.Fatalf("data=%v err=%v", data, err)
		}
		r.fetcher = func(context.Context, int64, string, int64, int64) ([]byte, error) {
			return []byte{1}, nil
		}
		if _, err := r.ReadAt(context.Background(), 0, 3); !errors.Is(err, download.ErrShortRead) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("non-dlink fetch error", func(t *testing.T) {
		boom := errors.New("boom")
		r := &remoteReader{size: 10, fetcher: func(context.Context, int64, string, int64, int64) ([]byte, error) {
			return nil, boom
		}}
		if _, err := r.ReadAt(context.Background(), 0, 1); !errors.Is(err, boom) {
			t.Fatalf("err=%v", err)
		}
	})

	for _, sentinel := range []error{download.ErrCDNForbidden, download.ErrRangeNotSatisfiable} {
		t.Run("reload on "+sentinel.Error(), func(t *testing.T) {
			entry := &mount.RemoteEntry{FSID: 1, Size: 10, MtimeSec: 5}
			client := &MockPCSClient{
				statResult: entry,
				dlink:      "http://new", dlinkFsID: 1, dlinkSize: 10, dlinkMtime: 5,
			}
			fs := newBaseFS(client)
			var calls int32
			r := &remoteReader{
				fsID: 1, size: 10, mtime: 5, path: "/a", dlink: "http://old",
				bfs: fs, stopCh: make(chan struct{}),
			}
			r.fetcher = func(_ context.Context, _ int64, dlink string, _, length int64) ([]byte, error) {
				if atomic.AddInt32(&calls, 1) == 1 {
					if dlink != "http://old" {
						t.Errorf("first dlink=%q", dlink)
					}
					return nil, sentinel
				}
				if dlink != "http://new" {
					t.Errorf("retry dlink=%q", dlink)
				}
				return make([]byte, length), nil
			}
			data, err := r.ReadAt(context.Background(), 0, 4)
			if err != nil || len(data) != 4 {
				t.Fatalf("data=%v err=%v", data, err)
			}
			if calls != 2 || r.currentDlink() != "http://new" {
				t.Fatalf("calls=%d dlink=%q", calls, r.currentDlink())
			}
		})
	}

	t.Run("reload failure", func(t *testing.T) {
		client := &MockPCSClient{statErr: os.ErrNotExist}
		fs := newBaseFS(client)
		r := &remoteReader{
			fsID: 1, size: 10, path: "/a", dlink: "old", bfs: fs,
			fetcher: func(context.Context, int64, string, int64, int64) ([]byte, error) {
				return nil, download.ErrCDNForbidden
			},
		}
		if _, err := r.ReadAt(context.Background(), 0, 1); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v", err)
		}
	})
}

// 目的：bfs 缺失、已关闭、并发刷新合并、成功刷新并写回 dlink 缓存。
func TestRemoteReader_Reload(t *testing.T) {
	t.Run("bfs nil", func(t *testing.T) {
		r := &remoteReader{}
		if err := r.Reload(context.Background()); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("closed", func(t *testing.T) {
		r := &remoteReader{bfs: newBaseFS(&MockPCSClient{})}
		r.closed.Store(true)
		if err := r.Reload(context.Background()); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("already refreshed", func(t *testing.T) {
		r := &remoteReader{bfs: newBaseFS(&MockPCSClient{}), dlink: "new"}
		if err := r.reloadDlink(context.Background(), "old"); err != nil {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("success", func(t *testing.T) {
		client := &MockPCSClient{
			statResult: &mount.RemoteEntry{FSID: 1, Size: 10, MtimeSec: 5},
			dlink:      "http://new", dlinkFsID: 1, dlinkSize: 10, dlinkMtime: 5,
		}
		fs := NewBaiduCloudFS(client, NewDlinkCache(time.Minute), "a")
		r := &remoteReader{fsID: 1, size: 10, mtime: 5, path: "/a", bfs: fs, dlink: "old"}
		if err := r.Reload(context.Background()); err != nil {
			t.Fatal(err)
		}
		if r.currentDlink() != "http://new" {
			t.Fatalf("dlink=%q", r.currentDlink())
		}
		if got, ok := fs.dlinkCache.Get(1, 10, 5); !ok || got != "http://new" {
			t.Fatalf("cache=%q,%v", got, ok)
		}
	})
}

// 目的：取链前的版本校验、dlink 缓存失效、locatedownload 冲突与失败回退。
func TestFSReloadDlink(t *testing.T) {
	t.Run("cache nil and success", func(t *testing.T) {
		client := &MockPCSClient{
			statResult: &mount.RemoteEntry{FSID: 1, Size: 10, MtimeSec: 5},
			dlink:      "http://new", dlinkFsID: 1, dlinkSize: 10, dlinkMtime: 5,
		}
		fs := newBaseFS(client)
		dlink, err := fs.reloadDlink(context.Background(), "/a", 1, 10, 5)
		if err != nil || dlink != "http://new" {
			t.Fatalf("dlink=%q err=%v", dlink, err)
		}
	})

	t.Run("stat error and nil", func(t *testing.T) {
		fs := newBaseFS(&MockPCSClient{statErr: os.ErrNotExist})
		if _, err := fs.reloadDlink(context.Background(), "/a", 1, 1, 1); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v", err)
		}
		fs = newBaseFS(&MockPCSClient{})
		if _, err := fs.reloadDlink(context.Background(), "/a", 1, 1, 1); err == nil {
			t.Fatal("expected nil stat error")
		}
	})

	t.Run("stat version conflict", func(t *testing.T) {
		fs := newBaseFS(&MockPCSClient{statResult: &mount.RemoteEntry{FSID: 1, Size: 99, MtimeSec: 5}})
		fs.dlinkCache = NewDlinkCache(time.Minute)
		fs.dlinkCache.Set(1, "old", "/a", 10, 5)
		if _, err := fs.reloadDlink(context.Background(), "/a", 1, 10, 5); !errors.Is(err, mount.ErrVersionConflict) {
			t.Fatalf("err=%v", err)
		}
		if _, ok := fs.dlinkCache.Get(1, 10, 5); ok {
			t.Fatal("dlink cache should be invalidated")
		}
	})

	t.Run("locate version conflict invalidates meta", func(t *testing.T) {
		client := &MockPCSClient{
			statResult: &mount.RemoteEntry{FSID: 1, Size: 10, MtimeSec: 5},
			dlink:      "http://new", dlinkFsID: 1, dlinkSize: 20, dlinkMtime: 5,
		}
		fs := newBaseFS(client)
		var invalidated int32
		fs.SetMetaInvalidate(func(string) { atomic.AddInt32(&invalidated, 1) })
		if _, err := fs.reloadDlink(context.Background(), "/a", 1, 10, 5); !errors.Is(err, mount.ErrVersionConflict) {
			t.Fatalf("err=%v", err)
		}
		if invalidated != 1 {
			t.Fatalf("invalidated=%d", invalidated)
		}
	})

	t.Run("locate error then method download error", func(t *testing.T) {
		client := &MockPCSClient{
			statResult: &mount.RemoteEntry{FSID: 1, Size: 10, MtimeSec: 5},
			dlinkErr:   errors.New("locate failed"),
		}
		fs := newBaseFS(client)
		if _, err := fs.reloadDlink(context.Background(), "/a", 1, 10, 5); err == nil {
			t.Fatal("expected error")
		}
	})
}

// 目的：dlink 缓存命中、locatedownload fallback、版本冲突与错误传播。
func TestFSResolveDlink(t *testing.T) {
	t.Run("cache hit", func(t *testing.T) {
		fs := newBaseFS(&MockPCSClient{})
		fs.dlinkCache = NewDlinkCache(time.Minute)
		fs.dlinkCache.Set(1, "cached", "/a", 10, 5)
		dlink, err := fs.resolveDlink(context.Background(), "/a", 1, 10, 5)
		if err != nil || dlink != "cached" {
			t.Fatalf("dlink=%q err=%v", dlink, err)
		}
	})

	t.Run("fallback locate success and cache", func(t *testing.T) {
		client := &MockPCSClient{dlink: "http://loc", dlinkFsID: 1, dlinkSize: 10, dlinkMtime: 5}
		fs := newBaseFS(client)
		fs.dlinkCache = NewDlinkCache(time.Minute)
		dlink, err := fs.resolveDlink(context.Background(), "/a", 1, 10, 5)
		if err != nil || dlink != "http://loc" {
			t.Fatalf("dlink=%q err=%v", dlink, err)
		}
		if got, ok := fs.dlinkCache.Get(1, 10, 5); !ok || got != "http://loc" {
			t.Fatalf("cache=%q,%v", got, ok)
		}
	})

	t.Run("locate error", func(t *testing.T) {
		fs := newBaseFS(&MockPCSClient{dlinkErr: os.ErrNotExist})
		if _, err := fs.resolveDlink(context.Background(), "/a", 1, 10, 5); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("locate version conflict", func(t *testing.T) {
		client := &MockPCSClient{dlink: "http://loc", dlinkFsID: 2, dlinkSize: 10, dlinkMtime: 5}
		fs := newBaseFS(client)
		if _, err := fs.resolveDlink(context.Background(), "/a", 1, 10, 5); !errors.Is(err, mount.ErrVersionConflict) {
			t.Fatalf("err=%v", err)
		}
	})
}

// 目的：Close 必须幂等、标记关闭并结束下载统计；无 BaiduCloudFS 的 reader 也能安全关闭。
func TestRemoteReader_CloseLifecycle(t *testing.T) {
	stats := NewTransferStats()
	stats.StartDownload("/a", 10)
	fs := newBaseFS(&MockPCSClient{})
	fs.SetTransferStats(stats)

	r := &remoteReader{fsID: 1, size: 2, mtime: 3, path: "/a", bfs: fs, stopCh: make(chan struct{})}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !r.closed.Load() {
		t.Fatal("not marked closed")
	}
	if len(stats.ActiveTasks()) != 0 {
		t.Fatal("download stats not ended")
	}
	if err := r.Close(); err != nil { // 幂等
		t.Fatal(err)
	}
	// 关闭后读必须立即失败，不允许继续访问网络。
	if _, err := r.ReadAt(context.Background(), 0, 1); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("ReadAt after close err=%v", err)
	}

	// 无 bfs 的 reader 也能 Close。
	r2 := &remoteReader{stopCh: make(chan struct{})}
	if err := r2.Close(); err != nil {
		t.Fatal(err)
	}
}

// 目的：后台刷新成功、版本冲突不更新、单飞去重与 stopCh 取消。
func TestBackgroundRefresh(t *testing.T) {
	entry := &mount.RemoteEntry{FSID: 1, Size: 10, MtimeSec: 5}
	mk := func() (*MockPCSClient, *BaiduCloudFS) {
		client := &MockPCSClient{
			statResult: entry,
			dlink:      "http://new", dlinkFsID: 1, dlinkSize: 10, dlinkMtime: 5,
		}
		return client, newBaseFS(client)
	}

	t.Run("success", func(t *testing.T) {
		_, fs := mk()
		r := &remoteReader{dlink: "old", stopCh: make(chan struct{})}
		r.backgroundRefresh(fs, "/a", 1, 10, 5, time.Millisecond)
		if r.currentDlink() != "http://new" {
			t.Fatalf("dlink=%q", r.currentDlink())
		}
	})

	t.Run("version conflict", func(t *testing.T) {
		client := &MockPCSClient{
			statResult: &mount.RemoteEntry{FSID: 1, Size: 11, MtimeSec: 5},
		}
		fs := newBaseFS(client)
		r := &remoteReader{dlink: "old", stopCh: make(chan struct{})}
		r.backgroundRefresh(fs, "/a", 1, 10, 5, time.Millisecond)
		if r.currentDlink() != "old" {
			t.Fatalf("dlink=%q", r.currentDlink())
		}
	})

	t.Run("in flight dedup", func(t *testing.T) {
		_, fs := mk()
		key := refreshKey{account: fs.accountKey, fsID: 1, path: "/a"}
		refreshInFlight.Store(key, struct{}{})
		defer refreshInFlight.Delete(key)
		r := &remoteReader{dlink: "old", stopCh: make(chan struct{})}
		r.backgroundRefresh(fs, "/a", 1, 10, 5, time.Millisecond)
		if r.currentDlink() != "old" {
			t.Fatalf("dlink=%q, want unchanged", r.currentDlink())
		}
	})

	t.Run("stop channel", func(t *testing.T) {
		_, fs := mk()
		r := &remoteReader{dlink: "old", stopCh: make(chan struct{})}
		done := make(chan struct{})
		go func() {
			r.backgroundRefresh(fs, "/a", 1, 10, 5, time.Hour)
			close(done)
		}()
		close(r.stopCh)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("backgroundRefresh did not stop")
		}
		if r.currentDlink() != "old" {
			t.Fatalf("dlink=%q", r.currentDlink())
		}
	})
}
