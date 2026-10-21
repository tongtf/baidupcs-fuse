package adapter

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"baidupcs-fuse/mount"
)

// closeTrackReader 是可观测 Close/FSID 的远端旧版本，用于验证写洞与尾部回填。
type closeTrackReader struct {
	data   []byte
	fsid   int64
	closed atomic.Int32
}

func (r *closeTrackReader) ReadAt(_ context.Context, off int64, size int) ([]byte, error) {
	if off >= int64(len(r.data)) {
		return nil, nil
	}
	end := off + int64(size)
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	return r.data[off:end], nil
}
func (r *closeTrackReader) Size() int64  { return int64(len(r.data)) }
func (r *closeTrackReader) FSID() int64  { return r.fsid }
func (r *closeTrackReader) Mtime() int64 { return 0 }
func (r *closeTrackReader) Reload(context.Context) error {
	return nil
}
func (r *closeTrackReader) Close() error {
	r.closed.Add(1)
	return nil
}

type metaRecorder struct {
	mu    sync.Mutex
	paths []string
}

func (m *metaRecorder) Invalidate(path string) {
	m.mu.Lock()
	m.paths = append(m.paths, path)
	m.mu.Unlock()
}

func (m *metaRecorder) has(path string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.paths {
		if p == path {
			return true
		}
	}
	return false
}

// 目的：writer 的 open→写→truncate→sync→reset→close 状态迁移必须符合同一逻辑大小，
// 增长截断保留旧远端尾部，关闭后所有写操作返回 os.ErrClosed。
func TestWriter_WriteTruncateResetState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	sess, err := mgr.Open("/dir/f")
	if err != nil {
		t.Fatal(err)
	}
	client := newUploadRecorder()
	u := NewUploadSession(client, mgr)
	reader := &closeTrackReader{data: []byte("ABCDEFGHIJ"), fsid: 42}
	w := &baiduRemoteWriter{
		session: sess, uploadSess: u, path: "/dir/f", stagingMgr: mgr, reader: reader,
	}

	if w.Dirty() || w.Size() != 0 || w.TargetSize() != 0 {
		t.Fatalf("initial state dirty=%v size=%d target=%d", w.Dirty(), w.Size(), w.TargetSize())
	}
	// POSIX: 0 字节 pwrite 不扩展文件，也不制造写洞。
	if err := w.Write(ctx, 100, nil); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 0 || w.TargetSize() != 0 {
		t.Fatalf("zero-length write changed state: size=%d target=%d", w.Size(), w.TargetSize())
	}

	if err := w.Write(ctx, 0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if !w.Dirty() || w.Size() != 3 || w.TargetSize() != 3 || !mgr.IsDirty("/dir/f") {
		t.Fatalf("after write dirty=%v size=%d target=%d", w.Dirty(), w.Size(), w.TargetSize())
	}
	if got, err := w.ReadFrom(ctx, 1, 2); err != nil || string(got) != "bc" {
		t.Fatalf("ReadFrom=%q err=%v", got, err)
	}
	w.MarkDirty() // 幂等

	if err := w.Truncate(-1); err == nil {
		t.Fatal("negative truncate must fail")
	}
	if w.Size() != 3 || w.TargetSize() != 3 {
		t.Fatalf("failed truncate changed state: size=%d target=%d", w.Size(), w.TargetSize())
	}
	if err := w.Truncate(2); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 2 || w.TargetSize() != 2 {
		t.Fatalf("shrink state: size=%d target=%d", w.Size(), w.TargetSize())
	}
	// 有 reader 时增大目标不得立刻补零，旧尾部留到 commit 回填。
	if err := w.Truncate(6); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 2 || w.TargetSize() != 6 {
		t.Fatalf("grow state: size=%d target=%d", w.Size(), w.TargetSize())
	}

	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Sync 后保留 writer 与 staging，但 dirty 清除；尾部已从旧版本回填为 "abCDEF"。
	if w.Size() != 6 || w.Dirty() || mgr.IsDirty("/dir/f") || reader.closed.Load() != 0 {
		t.Fatalf("after sync size=%d dirty=%v readerClosed=%d", w.Size(), w.Dirty(), reader.closed.Load())
	}
	if got, _ := w.ReadFrom(ctx, 0, 6); string(got) != "abCDEF" {
		t.Fatalf("tail not preserved: %q", got)
	}

	if err := w.Write(ctx, 6, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 0 || w.TargetSize() != 0 || !w.Dirty() {
		t.Fatalf("reset state: size=%d target=%d dirty=%v", w.Size(), w.TargetSize(), w.Dirty())
	}

	stagingPath := sess.Path()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Fatalf("staging not removed: %v", err)
	}
	if mgr.HasSession("/dir/f") || reader.closed.Load() != 1 {
		t.Fatalf("session=%v readerClosed=%d", mgr.HasSession("/dir/f"), reader.closed.Load())
	}
	for name, op := range map[string]func() error{
		"write":    func() error { return w.Write(ctx, 0, []byte("x")) },
		"truncate": func() error { return w.Truncate(0) },
		"reset":    func() error { return w.Reset() },
	} {
		if err := op(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("%s after close err=%v", name, err)
		}
	}
	// 已关闭 writer 的 Sync/Close 必须幂等。
	if err := w.Sync(ctx); err != nil {
		t.Fatalf("Sync after close: %v", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// 目的：成功提交必须失效 dlink/metadata/块缓存并清理 staging；失败提交必须保留
// staging、写入失败清单，供恢复流程重传。
func TestWriter_Commit_InvalidatesAndRecordsFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	client := newUploadRecorder()
	u := NewUploadSession(client, mgr)
	dlink := NewDlinkCache(time.Hour)
	meta := &metaRecorder{}
	var invalidated []int64
	fs := NewBaiduCloudFS(client, dlink, "acct")
	fs.SetWriteSupport(mgr, u)

	sess, err := mgr.Open("/dir/f")
	if err != nil {
		t.Fatal(err)
	}
	reader := &closeTrackReader{data: []byte("hello"), fsid: 42}
	w := &baiduRemoteWriter{
		session: sess, uploadSess: u, path: "/dir/f", stagingMgr: mgr,
		cloudFS: fs, reader: reader,
		deps: writerDeps{
			dlinkCache: dlink, metaCache: meta,
			blockInvalidate: func(fsid int64) { invalidated = append(invalidated, fsid) },
		},
	}
	dlink.Set(42, "https://old", "/dir/f", 5, 0)

	if err := w.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := dlink.Get(42, 5, 0); ok {
		t.Fatal("dlink cache not invalidated after commit")
	}
	if !meta.has("/dir/f") || !meta.has("/dir") {
		t.Fatalf("metadata not invalidated: %v", meta.paths)
	}
	if len(invalidated) != 1 || invalidated[0] != 42 {
		t.Fatalf("block invalidation=%v", invalidated)
	}
	if w.Dirty() {
		// Sync 成功后必须清除 dirty，允许后续继续写。
		t.Fatal("dirty not cleared")
	}
	// 失败重试：秒传/预创建都失败，writer 必须保持 dirty 并记录可恢复清单。
	meta.mu.Lock()
	meta.paths = nil
	meta.mu.Unlock()
	client.rapidErr = errors.New("rapid unavailable")
	client.precreateErr = os.ErrPermission
	if err := w.Write(ctx, 0, []byte("again")); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("sync err=%v", err)
	}
	if !w.Dirty() {
		t.Fatal("failed commit must keep dirty data")
	}
	entries, err := LoadFailedUploads(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("failed manifest=%+v err=%v", entries, err)
	}
	if entries[0].RemotePath != "/dir/f" || entries[0].Size != 5 || entries[0].LocalPath != sess.Path() {
		t.Fatalf("manifest entry=%+v", entries[0])
	}
	if meta.has("/dir/f") {
		t.Fatal("failed commit must not invalidate caches")
	}

	stagingPath := sess.Path()
	if err := w.Close(ctx); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("close err=%v", err)
	}
	if _, err := os.Stat(stagingPath); err != nil {
		t.Fatalf("failed close must keep staging for recover: %v", err)
	}
	if mgr.HasSession("/dir/f") {
		t.Fatal("session must be released after failed close")
	}
	if reader.closed.Load() != 1 {
		t.Fatalf("reader closed=%d", reader.closed.Load())
	}
	// 失败关闭后不得再写入。
	if err := w.Write(ctx, 0, []byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("write after failed close err=%v", err)
	}
}

// 目的：卸载流程必须先拒绝新写入、等待在途提交 drain，并让 mount ctx 取消传播到
// commit ctx；commitTimeout 必须产生 deadline。
func TestBaiduCloudFS_ShutdownAndCommitDrain(t *testing.T) {
	ctx := context.Background()
	fs := NewBaiduCloudFS(&MockPCSClient{}, NewDlinkCache(time.Minute), "acct")

	if _, err := fs.CreateWriter(ctx, "/x"); !errors.Is(err, mount.ErrWriteNotEnabled) {
		t.Fatalf("write gate err=%v", err)
	}
	mgr := NewStagingManager(t.TempDir(), true)
	fs.SetWriteSupport(mgr, NewUploadSession(&MockPCSClient{}, mgr))
	w, err := fs.CreateWriter(ctx, "/x")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}

	if err := fs.WaitCommits(ctx); err != nil {
		t.Fatalf("idle WaitCommits: %v", err)
	}
	fs.beginCommit()
	waitCtx, cancelWait := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelWait()
	if err := fs.WaitCommits(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitCommits with in-flight commit err=%v", err)
	}
	fs.endCommit()
	if err := fs.WaitCommits(ctx); err != nil {
		t.Fatalf("WaitCommits after drain: %v", err)
	}

	fs.BeginShutdown()
	if _, err := fs.CreateWriter(ctx, "/y"); !errors.Is(err, mount.ErrShuttingDown) {
		t.Fatalf("shutdown gate err=%v", err)
	}

	// commitTimeout 必须给上传 ctx 加 deadline。
	fs.SetCommitTimeout(10 * time.Millisecond)
	w2 := &baiduRemoteWriter{cloudFS: fs, commitTimeout: fs.commitTimeout}
	cctx, cancel := w2.commitContext(context.Background())
	if _, ok := cctx.Deadline(); !ok {
		t.Fatal("commit ctx has no deadline")
	}
	cancel()

	// mount ctx 取消必须终止正在进行的 commit ctx。
	mountCtx, cancelMount := context.WithCancel(context.Background())
	fs.SetMountContext(mountCtx)
	w3 := &baiduRemoteWriter{cloudFS: fs}
	cctx2, cancel2 := w3.commitContext(context.Background())
	cancelMount()
	select {
	case <-cctx2.Done():
	case <-time.After(time.Second):
		t.Fatal("mount ctx cancellation not propagated")
	}
	cancel2()
}

// recordingPCS 记录目录操作参数并可注入错误，用于验证 CloudFS 的委托与错误分类。
type recordingPCS struct {
	*MockPCSClient
	mu        sync.Mutex
	mkdirs    []string
	removes   [][]string
	renames   [][2]string
	mkdirErr  error
	removeErr error
	renameErr error
}

func (r *recordingPCS) Mkdir(_ context.Context, path string) error {
	r.mu.Lock()
	r.mkdirs = append(r.mkdirs, path)
	r.mu.Unlock()
	return r.mkdirErr
}
func (r *recordingPCS) Remove(_ context.Context, paths ...string) error {
	r.mu.Lock()
	r.removes = append(r.removes, append([]string(nil), paths...))
	r.mu.Unlock()
	return r.removeErr
}
func (r *recordingPCS) Rename(_ context.Context, from, to string) error {
	r.mu.Lock()
	r.renames = append(r.renames, [2]string{from, to})
	r.mu.Unlock()
	return r.renameErr
}

// 目的：CloudFS 必须原样委托目录操作与配额/元数据查询，并把底层错误分类透传给 FUSE。
func TestBaiduCloudFS_DelegationAndErrors(t *testing.T) {
	ctx := context.Background()
	client := &recordingPCS{MockPCSClient: &MockPCSClient{
		quotaTotal: 100, quotaUsed: 40,
		statResult:    &mount.RemoteEntry{FSID: 1, Path: "/f", Size: 7},
		readDirResult: []*mount.RemoteEntry{{FSID: 1, Name: "f"}},
	}}
	fs := NewBaiduCloudFS(client, NewDlinkCache(time.Minute), "acct")

	if err := fs.Mkdir(ctx, "/d"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove(ctx, "/a", "/b"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Rename(ctx, "/a", "/c"); err != nil {
		t.Fatal(err)
	}
	if len(client.mkdirs) != 1 || client.mkdirs[0] != "/d" {
		t.Fatalf("mkdirs=%v", client.mkdirs)
	}
	if len(client.removes) != 1 || len(client.removes[0]) != 2 || client.removes[0][1] != "/b" {
		t.Fatalf("removes=%v", client.removes)
	}
	if len(client.renames) != 1 || client.renames[0] != [2]string{"/a", "/c"} {
		t.Fatalf("renames=%v", client.renames)
	}

	stats, err := fs.StatFS(ctx)
	if err != nil || stats.Total != 100 || stats.Used != 40 || stats.Free != 60 {
		t.Fatalf("statfs=%+v err=%v", stats, err)
	}
	entry, err := fs.Stat(ctx, "/f")
	if err != nil || entry.FSID != 1 || entry.Size != 7 {
		t.Fatalf("stat=%+v err=%v", entry, err)
	}
	entries, err := fs.ReadDir(ctx, "/")
	if err != nil || len(entries) != 1 || entries[0].Name != "f" {
		t.Fatalf("readdir=%v err=%v", entries, err)
	}

	client.mkdirErr = os.ErrExist
	if err := fs.Mkdir(ctx, "/d"); !errors.Is(err, os.ErrExist) {
		t.Fatalf("mkdir err=%v", err)
	}
	client.removeErr = os.ErrPermission
	if err := fs.Remove(ctx, "/a"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("remove err=%v", err)
	}
	client.renameErr = os.ErrNotExist
	if err := fs.Rename(ctx, "/a", "/b"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rename err=%v", err)
	}
	client.quotaErr = context.Canceled
	if _, err := fs.StatFS(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("quota err=%v", err)
	}
	client.statErr = os.ErrPermission
	if _, err := fs.Stat(ctx, "/f"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("stat err=%v", err)
	}
	client.readDirErr = os.ErrNotExist
	if _, err := fs.ReadDir(ctx, "/"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("readdir err=%v", err)
	}

	// 非 panClient 不能生成 method=download 链接，必须明确失败而不是返回空链接。
	if _, err := fs.GetDlinkDownload(ctx, "/f"); err == nil {
		t.Fatal("expected error for non-pan client")
	}

	// 统计与块失效回调的注入必须真实生效。
	ts := NewTransferStats()
	fs.SetTransferStats(ts)
	if fs.Stats() != ts {
		t.Fatal("stats injection not honored")
	}
	var gotFSID int64
	fs.SetBlockInvalidate(func(fsid int64) { gotFSID = fsid })
	fs.blockInvalidate(9)
	if gotFSID != 9 {
		t.Fatalf("block invalidate callback=%d", gotFSID)
	}
}
