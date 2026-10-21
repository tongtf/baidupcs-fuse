package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"baidupcs-fuse/cache"
	"baidupcs-fuse/mount"
)

// 目的：后台目录刷新必须在周期到达后刷新过期目录，并在刷新失败时继续处理其它目录；
// interval<=0 时必须完全禁用，不能留下 goroutine。
func TestBackgroundRefresh_RefreshesStaleDirs(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	cloud := newHandlerFS()
	cloud.addDir("/d")
	cloud.addFile("/d/fresh", []byte("new"))
	cloud.dirHook = make(chan string, 8)
	root, fsctx := newHandlerFSContext(t, cloud)
	_ = root
	fsctx.meta.SetDir("/d", []*mount.RemoteEntry{{Name: "old", Size: 1}})

	opts := fsctx.opts
	opts.DirRefreshInterval = 2 * time.Millisecond
	stop := fsctx.startBackgroundRefresh(opts)
	defer stop()

	select {
	case path := <-cloud.dirHook:
		if path != "/d" {
			t.Fatalf("refreshed %q, want /d", path)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not run")
	}

	// 禁用模式直接返回 no-op。
	off := fsctx.opts
	off.DirRefreshInterval = 0
	fsctx.startBackgroundRefresh(off)()
}

// 目的：refreshStaleDirs 必须处理根目录子路径、nil 条目和单个目录失败，
// 一次失败不能中断其它目录刷新。
func TestRefreshStaleDirs_ErrorAndRootPath(t *testing.T) {
	cloud := newHandlerFS()
	cloud.addFile("/rootfile", []byte("x"))
	cloud.addDir("/bad")
	cloud.dirErrs["/bad"] = os.ErrPermission
	_, fsctx := newHandlerFSContext(t, cloud)
	fsctx.meta.SetDir("/", []*mount.RemoteEntry{{Name: "rootfile"}})
	fsctx.meta.SetDir("/bad", nil)

	opts := fsctx.opts
	opts.DirRefreshInterval = time.Nanosecond
	opts.HttpTimeout = time.Minute // 超过 30s 时应被截断
	fsctx.refreshStaleDirs(opts)

	if _, ok := fsctx.meta.Get("/rootfile"); !ok {
		t.Fatal("root child path not updated")
	}
	// 失败的目录不能标记为已刷新。
	if !fsctx.meta.NeedsRefresh("/bad", time.Nanosecond) {
		t.Fatal("failed refresh should leave directory stale")
	}
}

// 目的：定期刷盘循环必须把 Dirty manifest 落盘，并在 stop 后停止 ticker。
func TestDiskFlushLoop_FlushesManifest(t *testing.T) {
	cloud := newHandlerFS()
	_, fsctx := newHandlerFSContext(t, cloud)
	key := cache.BlockKeyFromPath(fsctx.opts.AccountKey, 9, 4, 0, 0)
	fsctx.disk.Put(key, []byte("data"))

	stop := fsctx.startDiskFlushInterval(2 * time.Millisecond)
	manifest := filepath.Join(fsctx.opts.CacheDir, "blocks", "manifest.json")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(manifest); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disk manifest was not flushed")
		}
		time.Sleep(time.Millisecond)
	}
	stop()
}

// 目的：Unix socket 必须支持 health/单路径/全量刷新与坏请求错误响应，
// 停止后 socket 文件被清理。
func TestRefreshSocket_HealthAndRefresh(t *testing.T) {
	cloud := newHandlerFS()
	cloud.addDir("/d")
	cloud.addDir("/e")
	cloud.addDir("/bad")
	cloud.addFile("/d/a", []byte("1234"))
	cloud.addFile("/e/b", []byte("5678"))
	cloud.dirErrs["/bad"] = os.ErrPermission
	_, fsctx := newHandlerFSContext(t, cloud)
	fsctx.meta.SetDir("/d", nil)
	fsctx.meta.SetDir("/e", nil)
	fsctx.meta.SetDir("/bad", nil)

	opts := fsctx.opts
	// macOS 的 Unix socket 路径上限约 104 字节，测试缓存目录必须足够短。
	shortDir, err := os.MkdirTemp("/tmp", "bdfs-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(shortDir) })
	opts.CacheDir = shortDir
	sock := refreshSocketPath(opts)
	stop := fsctx.startRefreshSocket(opts)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()

	resp := socketRequest(t, sock, `{"type":"health"}`)
	if !resp.OK || resp.ActiveFetches != 0 {
		t.Fatalf("health response=%+v", resp)
	}
	resp = socketRequest(t, sock, `{"type":"refresh","path":"/d"}`)
	if !resp.OK || resp.Refreshed != 1 || !strings.Contains(resp.Message, "/d") {
		t.Fatalf("refresh /d response=%+v", resp)
	}
	dirEntries, ok := fsctx.meta.GetDir("/d")
	if !ok || len(dirEntries) != 1 || dirEntries[0].Name != "a" {
		t.Fatalf("single-path refresh did not update dir cache: %+v ok=%v", dirEntries, ok)
	}
	resp = socketRequest(t, sock, `{bad json`)
	if resp.OK || !strings.Contains(resp.Message, "解析请求失败") {
		t.Fatalf("bad request response=%+v", resp)
	}

	// 全量刷新：失败目录跳过，成功目录计数；nil 条目不得 panic。
	full := fsctx.doRefresh("", opts)
	if !full.OK || full.Refreshed < 2 {
		t.Fatalf("full refresh=%+v", full)
	}
	if !fsctx.meta.NeedsRefresh("/bad", time.Nanosecond) {
		t.Fatal("failed directory should remain stale")
	}
	// 单路径错误。
	bad := fsctx.doRefresh("/bad", opts)
	if bad.OK || !strings.Contains(bad.Message, "刷新失败") {
		t.Fatalf("bad path refresh=%+v", bad)
	}

	stop()
	stopped = true
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Fatalf("socket not removed: %v", err)
	}

	// 监听失败（CacheDir 是文件）必须安全返回 no-op。
	file := filepath.Join(t.TempDir(), "not-dir")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	badOpts := opts
	badOpts.CacheDir = file
	noop := fsctx.startRefreshSocket(badOpts)
	noop()
}

func socketRequest(t *testing.T, path, payload string) refreshResponse {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("dial socket: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(conn, payload); err != nil {
		t.Fatalf("write request: %v", err)
	}
	var resp refreshResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

// 目的：实例锁必须保证同一 CacheDir 只有一个挂载/recover 持有者，
// 释放后可重新获取，nil 释放安全。
func TestInstanceLock_ExclusiveAndRelease(t *testing.T) {
	dir := t.TempDir()
	f, err := AcquireInstanceLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireInstanceLock(dir); err == nil {
		t.Fatal("second lock should fail while held")
	}
	ReleaseInstanceLock(f)
	ReleaseInstanceLock(nil)

	f2, err := AcquireInstanceLock(dir)
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	ReleaseInstanceLock(f2)

	// CacheDir 是普通文件：mkdir 失败必须返回错误。
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireInstanceLock(file); err == nil {
		t.Fatal("lock on regular file should fail")
	}
}

// 目的：预读必须跳过越界/已缓存块、避免重复在途，并在回源失败时静默记录。
func TestPrefetch_BoundsAndDedup(t *testing.T) {
	cloud := newHandlerFS()
	cloud.addFile("/f", make([]byte, 8))
	root, fsctx := newHandlerFSContext(t, cloud)
	node, _ := attachFile(t, fsctx, root.EmbeddedInode(), "f", "/f", cloud.entries["/f"])
	reader := &handlerReader{data: make([]byte, 8), fsid: 5}

	node.prefetch(reader, -1)
	node.prefetch(reader, 8)
	if reader.readAt != 0 {
		t.Fatalf("out-of-range prefetch read %d times", reader.readAt)
	}
	node.prefetch(reader, 0)
	if reader.readAt != 1 {
		t.Fatalf("prefetch read=%d", reader.readAt)
	}
	key := cache.BlockKeyFromPath(fsctx.opts.AccountKey, 5, 8, 0, 0)
	if !fsctx.blocks.Has(key) {
		t.Fatal("prefetched block not cached")
	}
	node.prefetch(reader, 0)
	if reader.readAt != 1 {
		t.Fatal("cached block fetched again")
	}

	node.prefetching.Store(true)
	node.prefetch(reader, 4)
	node.prefetching.Store(false)
	if reader.readAt != 1 {
		t.Fatal("prefetch ran while already in flight")
	}

	reader.readErr = errors.New("boom")
	node.prefetch(reader, 4)
	if reader.readAt != 2 {
		t.Fatalf("failed prefetch not attempted: %d", reader.readAt)
	}
}

// 目的：Release 必须关闭读句柄的 reader；写句柄提交失败要把错误映射给内核，
// 未知/空句柄安全返回成功。
func TestRelease_HandlesCloseSemantics(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addFile("/f", []byte("x"))
	root, fsctx := newHandlerFSContext(t, cloud)
	node, _ := attachFile(t, fsctx, root.EmbeddedInode(), "f", "/f", cloud.entries["/f"])

	r := &handlerReader{data: []byte("x"), fsid: 1}
	if errno := node.Release(ctx, &readFileHandle{node: node, reader: r}); errno != 0 {
		t.Fatalf("release read handle errno=%v", errno)
	}
	if !r.closed {
		t.Fatal("reader not closed")
	}

	w := &memWriter{}
	wh := &writeFileHandle{node: node, writer: w}
	node.addWriter(wh)
	if errno := node.Release(ctx, wh); errno != 0 {
		t.Fatalf("release write handle errno=%v", errno)
	}
	if !w.closed {
		t.Fatal("writer not closed")
	}
	if len(node.writers) != 0 {
		t.Fatal("writer not removed from node")
	}

	w2 := &memWriter{closeErr: os.ErrPermission}
	if errno := node.Release(ctx, &writeFileHandle{node: node, writer: w2}); errno != syscall.EACCES {
		t.Fatalf("failed commit errno=%v", errno)
	}
	if errno := node.Release(ctx, nil); errno != 0 {
		t.Fatalf("nil handle errno=%v", errno)
	}
}

// 目的：无活跃写句柄的 truncate 必须覆盖 Stat/类型/同尺寸短路/创建/回源/提交错误，
// 且失败路径关闭一次性 writer，不泄漏 staging。
func TestTruncatePath_ErrorBranches(t *testing.T) {
	ctx := context.Background()
	makeFS := func(t *testing.T, content []byte) (*handlerFS, *fileNode) {
		cloud := newHandlerFS()
		cloud.addFile("/f", content)
		root, fsctx := newHandlerFSContext(t, cloud)
		node, _ := attachFile(t, fsctx, root.EmbeddedInode(), "f", "/f", cloud.entries["/f"])
		return cloud, node
	}

	cloud, node := makeFS(t, []byte("hello"))
	cloud.statErrs["/err"] = os.ErrPermission
	if errno := node.fs.truncatePath(ctx, "/err", 1); errno != syscall.EACCES {
		t.Fatalf("stat err errno=%v", errno)
	}
	cloud.nilPaths["/nil"] = true
	if errno := node.fs.truncatePath(ctx, "/nil", 1); errno != syscall.EIO {
		t.Fatalf("nil stat errno=%v", errno)
	}
	if errno := node.fs.truncatePath(ctx, "/f", 5); errno != 0 {
		t.Fatalf("same size errno=%v", errno)
	}
	cloud.addDir("/dir")
	if errno := node.fs.truncatePath(ctx, "/dir", 1); errno != syscall.EISDIR {
		t.Fatalf("dir truncate errno=%v", errno)
	}

	created := 0
	cloud.writerMake = func(string) (mount.RemoteWriter, error) { created++; return &memWriter{}, nil }
	if errno := node.fs.truncatePath(ctx, "/f", 5); errno != 0 || created != 0 {
		t.Fatalf("same size should not create writer: errno=%v created=%d", errno, created)
	}

	cloud.writerMake = func(string) (mount.RemoteWriter, error) { return nil, os.ErrPermission }
	if errno := node.fs.truncatePath(ctx, "/f", 2); errno != syscall.EACCES {
		t.Fatalf("create writer errno=%v", errno)
	}

	// size=0：不打开旧 reader，直接截断并提交。
	w0 := &memWriter{data: []byte("hello"), target: 5}
	cloud.writerMake = func(string) (mount.RemoteWriter, error) { return w0, nil }
	if errno := node.fs.truncatePath(ctx, "/f", 0); errno != 0 {
		t.Fatalf("truncate zero errno=%v", errno)
	}
	if !w0.closed || w0.target != 0 || w0.reader != nil {
		t.Fatalf("zero truncate writer=%+v", w0)
	}

	// OpenRead 失败：writer 必须被关闭。
	w1 := &memWriter{}
	cloud.writerMake = func(string) (mount.RemoteWriter, error) { return w1, nil }
	cloud.openErr = os.ErrPermission
	if errno := node.fs.truncatePath(ctx, "/f", 2); errno != syscall.EACCES {
		t.Fatalf("open read errno=%v", errno)
	}
	if !w1.closed {
		t.Fatal("writer not closed after OpenRead failure")
	}
	cloud.openErr = nil

	// Truncate 失败：writer 关闭并返回错误。
	w2 := &memWriter{truncErr: os.ErrNotExist}
	cloud.writerMake = func(string) (mount.RemoteWriter, error) { return w2, nil }
	if errno := node.fs.truncatePath(ctx, "/f", 2); errno != syscall.ENOENT {
		t.Fatalf("truncate errno=%v", errno)
	}
	if !w2.closed {
		t.Fatal("writer not closed after truncate failure")
	}

	// Close 失败：提交错误向上返回。
	w3 := &memWriter{closeErr: os.ErrPermission}
	cloud.writerMake = func(string) (mount.RemoteWriter, error) { return w3, nil }
	if errno := node.fs.truncatePath(ctx, "/f", 3); errno != syscall.EACCES {
		t.Fatalf("close errno=%v", errno)
	}
}

// 目的：Mount 的前置失败必须明确报错并释放实例锁：缺少 curl、非法挂载点。
func TestMount_EarlyFailures(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	opts.MountPoint = t.TempDir()

	t.Setenv("PATH", "")
	if _, _, err := Mount(opts, newMockFS()); err == nil || !strings.Contains(err.Error(), "curl") {
		t.Fatalf("missing curl err=%v", err)
	}

	t.Setenv("PATH", os.Getenv("PATH"))
	mnt := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(mnt, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	opts.MountPoint = mnt
	_, _, err := Mount(opts, newMockFS())
	if err == nil {
		t.Skip("mount succeeded unexpectedly; environment has a working FUSE")
	}
	// 失败后必须已 releaseInstanceLock，否则 recover/再次挂载全部被拒。
	f, lerr := AcquireInstanceLock(opts.CacheDir)
	if lerr != nil {
		t.Fatalf("instance lock leaked after mount failure: %v", lerr)
	}
	ReleaseInstanceLock(f)
}
