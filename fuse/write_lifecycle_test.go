package fuse

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"

	"baidupcs-fuse/mount"
	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type lifecycleWriter struct {
	size   int64
	dirty  bool
	closed bool
}

func (w *lifecycleWriter) Write(ctx context.Context, off int64, data []byte) error {
	w.dirty = true
	if end := off + int64(len(data)); end > w.size {
		w.size = end
	}
	return nil
}
func (w *lifecycleWriter) Close(ctx context.Context) error { w.closed = true; return nil }
func (w *lifecycleWriter) Sync(ctx context.Context) error  { return nil }
func (w *lifecycleWriter) Truncate(size int64) error       { w.size = size; w.dirty = true; return nil }
func (w *lifecycleWriter) TargetSize() int64               { return w.size }
func (w *lifecycleWriter) SetTargetSize(size int64)        { w.size = size }
func (w *lifecycleWriter) MarkDirty()                      { w.dirty = true }
func (w *lifecycleWriter) ReadFrom(ctx context.Context, off int64, size int) ([]byte, error) {
	return make([]byte, size), nil
}
func (w *lifecycleWriter) Size() int64                    { return w.size }
func (w *lifecycleWriter) Reset() error                   { w.size = 0; w.dirty = true; return nil }
func (w *lifecycleWriter) Dirty() bool                    { return w.dirty }
func (w *lifecycleWriter) SetReader(r mount.RemoteReader) {}

type flakyWriterFS struct {
	*mockCloudFS
	fail   atomic.Bool
	writer *lifecycleWriter
}

func (f *flakyWriterFS) CreateWriter(ctx context.Context, path string) (mount.RemoteWriter, error) {
	if f.fail.Load() {
		return nil, errors.New("create writer failed")
	}
	return f.writer, nil
}

// TestOpen_CreateWriterFailureRollsBack: 失败不得留下 opening 占位，第二次 open 必须立即可用。
func TestOpen_CreateWriterFailureRollsBack(t *testing.T) {
	opts := defaultOpts()
	opts.EnableWrite = true
	opts.CacheDir = t.TempDir()
	cloud := &flakyWriterFS{mockCloudFS: newMockFS(), writer: &lifecycleWriter{}}
	cloud.files["/f"] = &mount.RemoteEntry{Path: "/f", Size: 0}
	ctx := newContext(opts, cloud)
	node := &fileNode{fs: ctx, path: "/f", entry: &mount.RemoteEntry{Path: "/f", Size: 0}}

	cloud.fail.Store(true)
	if _, _, errno := node.Open(context.Background(), syscall.O_WRONLY); errno != syscall.EIO {
		t.Fatalf("first open errno=%v, want EIO", errno)
	}
	cloud.fail.Store(false)
	h, _, errno := node.Open(context.Background(), syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("second open errno=%v, want 0", errno)
	}
	// 同文件第二个写句柄应 EBUSY，避免并发 commit 冲突。
	if _, _, errno := node.Open(context.Background(), syscall.O_WRONLY); errno != syscall.EBUSY {
		t.Fatalf("concurrent write open errno=%v, want EBUSY", errno)
	}
	_ = node.Release(context.Background(), h)
}

// TestCreate_WriterFailureReturnsEIO: CreateWriter 失败时不得返回“成功 + nil handle”。
func TestCreate_WriterFailureReturnsEIO(t *testing.T) {
	opts := defaultOpts()
	opts.EnableWrite = true
	opts.CacheDir = t.TempDir()
	cloud := &flakyWriterFS{mockCloudFS: newMockFS(), writer: &lifecycleWriter{}}
	ctx := newContext(opts, cloud)
	cloud.fail.Store(true)
	n := &dirNode{fs: ctx, path: "/"}
	out := &fuse.EntryOut{}
	if _, _, _, errno := n.Create(context.Background(), "new.txt", syscall.O_WRONLY|syscall.O_CREAT, 0644, out); errno != syscall.EIO {
		t.Fatalf("Create errno=%v, want EIO", errno)
	}
}

// TestSetattr_NoHandleTruncateCommits: 无句柄 truncate 必须走一次性 writer 并同步提交。
func TestSetattr_NoHandleTruncateCommits(t *testing.T) {
	opts := defaultOpts()
	opts.EnableWrite = true
	opts.CacheDir = t.TempDir()
	w := &lifecycleWriter{size: 100, dirty: true}
	cloud := &flakyWriterFS{mockCloudFS: newMockFS(), writer: w}
	cloud.files["/f"] = &mount.RemoteEntry{Path: "/f", Size: 100}
	ctx := newContext(opts, cloud)
	node := &fileNode{fs: ctx, path: "/f", entry: &mount.RemoteEntry{Path: "/f", Size: 100}}
	ctx.meta.Set("/f", &mount.RemoteEntry{Path: "/f", Size: 100})

	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = 0
	out := &fuse.AttrOut{}
	if errno := node.Setattr(context.Background(), nil, in, out); errno != 0 {
		t.Fatalf("Setattr errno=%v", errno)
	}
	if !w.closed {
		t.Fatal("writer must be closed (committed) after truncate")
	}
	if w.size != 0 {
		t.Fatalf("writer size=%d, want 0", w.size)
	}
	if node.entry.Size != 0 {
		t.Fatalf("node size=%d, want 0", node.entry.Size)
	}
}

type countingStatFS struct {
	*mockCloudFS
	statCount atomic.Int32
}

func (f *countingStatFS) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	f.statCount.Add(1)
	return f.mockCloudFS.Stat(ctx, path)
}

// TestLookup_NegativeCacheSuppressesRepeatedStat: TTL 内缺失文件不再打 API。
func TestLookup_NegativeCacheSuppressesRepeatedStat(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	cloud := &countingStatFS{mockCloudFS: newMockFS()}
	ctx := newContext(opts, cloud)
	parent := &gofuse.Inode{}
	out := &fuse.EntryOut{}
	for i := 0; i < 3; i++ {
		if _, errno := ctx.lookupChild(context.Background(), parent, "/", "missing", out); errno != syscall.ENOENT {
			t.Fatalf("lookup %d errno=%v, want ENOENT", i, errno)
		}
	}
	if got := cloud.statCount.Load(); got != 1 {
		t.Fatalf("Stat called %d times, want 1", got)
	}
}

type renameFS struct {
	*mockCloudFS
	calls       int
	removeCalls int
	firstErr    error
	errAt       map[int]error
}

func (f *renameFS) Rename(ctx context.Context, from, to string) error {
	f.calls++
	if err, ok := f.errAt[f.calls]; ok {
		return err
	}
	if f.calls == 1 {
		return f.firstErr
	}
	return nil
}
func (f *renameFS) Remove(ctx context.Context, paths ...string) error {
	f.removeCalls++
	return nil
}

// TestRename_NonEEXISTDoesNotDeleteTarget: 非 EEXIST 失败时绝不能先删目标。
func TestRename_NonEEXISTDoesNotDeleteTarget(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	cloud := &renameFS{mockCloudFS: newMockFS(), firstErr: errors.New("server 500")}
	ctx := newContext(opts, cloud)
	if errno := ctx.renamePath(context.Background(), "/a", "/b", "/", "/"); errno == 0 {
		t.Fatal("expected rename failure")
	}
	if cloud.removeCalls != 0 {
		t.Fatalf("Remove called %d times on non-EEXIST error", cloud.removeCalls)
	}
}

// TestRename_SamePathNoop: mv /a /a 必须是 no-op，不能走 EEXIST 删除目标分支。
func TestRename_SamePathNoop(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	cloud := &renameFS{mockCloudFS: newMockFS(), firstErr: errors.New("should not be called")}
	ctx := newContext(opts, cloud)
	if errno := ctx.renamePath(context.Background(), "/a", "/a", "/", "/"); errno != 0 {
		t.Fatalf("same-path rename errno=%v, want 0", errno)
	}
	if cloud.calls != 0 || cloud.removeCalls != 0 {
		t.Fatalf("calls=%d remove=%d, want 0/0", cloud.calls, cloud.removeCalls)
	}
}

// TestRename_DirectoryIntoItselfEINVAL: 禁止把目录移动到自身内部。
func TestRename_DirectoryIntoItselfEINVAL(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	cloud := &renameFS{mockCloudFS: newMockFS(), firstErr: errors.New("should not be called")}
	ctx := newContext(opts, cloud)
	if errno := ctx.renamePath(context.Background(), "/a", "/a/b", "/", "/a"); errno != syscall.EINVAL {
		t.Fatalf("rename into itself errno=%v, want EINVAL", errno)
	}
	if cloud.calls != 0 {
		t.Fatalf("Rename called %d times, want 0", cloud.calls)
	}
}

// TestRename_EEXISTMovesTargetToBackupThenRetries: EEXIST 时先把目标移到备份，
// 源改名成功后才清理备份，避免源 rename 失败导致目标永久丢失。
func TestRename_EEXISTMovesTargetToBackupThenRetries(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	cloud := &renameFS{
		mockCloudFS: newMockFS(),
		firstErr:    &mount.BaiduError{Errno: -8, Method: "rename"},
	}
	ctx := newContext(opts, cloud)
	// Stat(old)/Stat(target) 必须成功；目标真实存在时才允许删除后重试。
	ctx.cloudFS.(*renameFS).mockCloudFS.files["/a"] = &mount.RemoteEntry{Path: "/a", Size: 1}
	ctx.cloudFS.(*renameFS).mockCloudFS.files["/b"] = &mount.RemoteEntry{Path: "/b", Size: 1}
	if errno := ctx.renamePath(context.Background(), "/a", "/b", "/", "/"); errno != 0 {
		t.Fatalf("rename errno=%v, want 0", errno)
	}
	if cloud.removeCalls != 1 {
		t.Fatalf("Remove called %d times, want 1 (backup cleanup)", cloud.removeCalls)
	}
	if cloud.calls != 3 {
		t.Fatalf("Rename called %d times, want 3 (target→backup, source→target, backup cleanup)", cloud.calls)
	}
}

// TestRename_EEXISTSourceFailureRollsBackTarget: 源 rename 失败时把备份改回目标。
func TestRename_EEXISTSourceFailureRollsBackTarget(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	cloud := &renameFS{
		mockCloudFS: newMockFS(),
		firstErr:    &mount.BaiduError{Errno: -8, Method: "rename"},
		errAt:       map[int]error{3: errors.New("source rename failed")},
	}
	cloud.files["/a"] = &mount.RemoteEntry{Path: "/a", Size: 1}
	cloud.files["/b"] = &mount.RemoteEntry{Path: "/b", Size: 2}
	ctx := newContext(opts, cloud)
	if errno := ctx.renamePath(context.Background(), "/a", "/b", "/", "/"); errno == 0 {
		t.Fatal("expected rename failure")
	}
	if cloud.calls != 4 {
		t.Fatalf("Rename called %d times, want 4 (incl. rollback)", cloud.calls)
	}
	if cloud.removeCalls != 0 {
		t.Fatalf("Remove called %d times, want 0 before rollback", cloud.removeCalls)
	}
}

// TestRmdir_NonEmptyReturnsENOTEMPTY: 不能在远端执行可能递归的删除。
func TestRmdir_NonEmptyReturnsENOTEMPTY(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	mfs := newMockFS()
	mfs.addDir("/d", []string{"child.txt"})
	mfs.addFile("/d/child.txt", 1)
	ctx := newContext(opts, mfs)
	n := &dirNode{fs: ctx, path: "/"}
	if errno := n.Rmdir(context.Background(), "d"); errno != syscall.ENOTEMPTY {
		t.Fatalf("Rmdir errno=%v, want ENOTEMPTY", errno)
	}
}

// TestRename_FileOverNonEmptyDirReturnsENOTEMPTY: 不能递归删除目标目录内容。
func TestRename_FileOverNonEmptyDirReturnsENOTEMPTY(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	cloud := &renameFS{
		mockCloudFS: newMockFS(),
		firstErr:    &mount.BaiduError{Errno: -8, Method: "rename"},
	}
	cloud.dirs["/a"] = nil
	cloud.dirs["/d"] = []string{"keep.txt"}
	cloud.files["/d/keep.txt"] = &mount.RemoteEntry{Path: "/d/keep.txt", Size: 1}
	ctx := newContext(opts, cloud)
	if errno := ctx.renamePath(context.Background(), "/a", "/d", "/", "/"); errno != syscall.ENOTEMPTY {
		t.Fatalf("rename errno=%v, want ENOTEMPTY", errno)
	}
	if cloud.removeCalls != 0 {
		t.Fatalf("Remove called %d times, want 0", cloud.removeCalls)
	}
}

// TestRename_FileOverDirReturnsEISDIR: POSIX 类型不匹配错误。
func TestRename_FileOverDirReturnsEISDIR(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	cloud := &renameFS{
		mockCloudFS: newMockFS(),
		firstErr:    &mount.BaiduError{Errno: -8, Method: "rename"},
	}
	cloud.files["/a"] = &mount.RemoteEntry{Path: "/a", Size: 1}
	cloud.dirs["/d"] = nil
	ctx := newContext(opts, cloud)
	if errno := ctx.renamePath(context.Background(), "/a", "/d", "/", "/"); errno != syscall.EISDIR {
		t.Fatalf("rename errno=%v, want EISDIR", errno)
	}
	if cloud.removeCalls != 0 {
		t.Fatalf("Remove called %d times, want 0", cloud.removeCalls)
	}
}

type growWriter struct {
	staging []byte
	target  int64
}

func (w *growWriter) Write(ctx context.Context, off int64, data []byte) error {
	end := off + int64(len(data))
	if end > int64(len(w.staging)) {
		next := make([]byte, end)
		copy(next, w.staging)
		w.staging = next
	}
	copy(w.staging[off:], data)
	if end > w.target {
		w.target = end
	}
	return nil
}
func (w *growWriter) Close(ctx context.Context) error { return nil }
func (w *growWriter) Sync(ctx context.Context) error  { return nil }
func (w *growWriter) Truncate(size int64) error       { w.target = size; return nil }
func (w *growWriter) TargetSize() int64               { return w.target }
func (w *growWriter) SetTargetSize(size int64)        { w.target = size }
func (w *growWriter) MarkDirty()                      {}
func (w *growWriter) Size() int64                     { return int64(len(w.staging)) }
func (w *growWriter) Reset() error                    { w.staging = nil; w.target = 0; return nil }
func (w *growWriter) Dirty() bool                     { return true }
func (w *growWriter) SetReader(r mount.RemoteReader)  {}
func (w *growWriter) ReadFrom(ctx context.Context, off int64, size int) ([]byte, error) {
	if off >= int64(len(w.staging)) {
		return nil, nil
	}
	end := off + int64(size)
	if end > int64(len(w.staging)) {
		end = int64(len(w.staging))
	}
	return append([]byte(nil), w.staging[off:end]...), nil
}

type sliceReader struct {
	data   []byte
	fsID   int64
	mtime  int64
	closed bool
}

func (r *sliceReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	if off < 0 || off+int64(size) > int64(len(r.data)) {
		return nil, errors.New("short read")
	}
	return append([]byte(nil), r.data[off:off+int64(size)]...), nil
}
func (r *sliceReader) Size() int64                      { return int64(len(r.data)) }
func (r *sliceReader) FSID() int64                      { return r.fsID }
func (r *sliceReader) Mtime() int64                     { return r.mtime }
func (r *sliceReader) Reload(ctx context.Context) error { return nil }
func (r *sliceReader) Close() error                     { r.closed = true; return nil }

// TestReadFromWriter_TruncateGrowBeyondRemoteReadsZeros: truncate -s +N 后读扩展区
// 必须返回 0，而不是把超出 pinned 版本的 Range 发给远端导致 EIO。
func TestReadFromWriter_TruncateGrowBeyondRemoteReadsZeros(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	ctx := newContext(opts, &mockCloudFS{files: map[string]*mount.RemoteEntry{}, dirs: map[string][]string{}})
	reader := &sliceReader{data: []byte("0123456789"), fsID: 7, mtime: 1}
	w := &growWriter{target: 20}
	node := &fileNode{fs: ctx, path: "/f", entry: &mount.RemoteEntry{Path: "/f", Size: 20}}
	h := &writeFileHandle{node: node, writer: w, reader: reader}

	res, errno := h.Read(context.Background(), make([]byte, 10), 5)
	if errno != 0 {
		t.Fatalf("read errno=%v", errno)
	}
	got, status := res.Bytes(nil)
	if status != fuse.OK {
		t.Fatalf("ReadResult status=%v", status)
	}
	want := []byte("56789\x00\x00\x00\x00\x00")
	if string(got) != string(want) {
		t.Fatalf("read=%q, want %q", got, want)
	}
}

// TestUnlink_DirectoryReturnsEISDIR: unlink 不能落到目录上（远端可能递归删除）。
func TestUnlink_DirectoryReturnsEISDIR(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	mfs := newMockFS()
	mfs.addDir("/d", []string{"child.txt"})
	mfs.addFile("/d/child.txt", 1)
	cloud := &renameFS{mockCloudFS: mfs}
	ctx := newContext(opts, cloud)
	if errno := ctx.removePath(context.Background(), "/d", false); errno != syscall.EISDIR {
		t.Fatalf("unlink dir errno=%v, want EISDIR", errno)
	}
	if cloud.removeCalls != 0 {
		t.Fatalf("Remove called %d times for directory unlink", cloud.removeCalls)
	}
}

// TestRmdir_FileReturnsENOTDIR: rmdir 不能删文件。
func TestRmdir_FileReturnsENOTDIR(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	mfs := newMockFS()
	mfs.addFile("/f.txt", 1)
	cloud := &renameFS{mockCloudFS: mfs}
	ctx := newContext(opts, cloud)
	if errno := ctx.removePath(context.Background(), "/f.txt", true); errno != syscall.ENOTDIR {
		t.Fatalf("rmdir file errno=%v, want ENOTDIR", errno)
	}
	if cloud.removeCalls != 0 {
		t.Fatalf("Remove called %d times for file rmdir", cloud.removeCalls)
	}
}

// TestOpenAt_UninitializedInode: createFile 在 NewInode 之前打开已有文件，
// 此时不能调用 remotePath（go-fuse Inode.Path 会因 bridge=nil panic）。
func TestOpenAt_UninitializedInode(t *testing.T) {
	opts := defaultOpts()
	opts.EnableWrite = true
	opts.CacheDir = t.TempDir()
	cloud := &flakyWriterFS{mockCloudFS: newMockFS(), writer: &lifecycleWriter{}}
	cloud.files["/f"] = &mount.RemoteEntry{Path: "/f", Size: 0}
	ctx := newContext(opts, cloud)
	node := &fileNode{fs: ctx, path: "/f", entry: &mount.RemoteEntry{Path: "/f", Size: 0}}
	h, _, errno := node.openAt(context.Background(), "/f", syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("openAt errno=%v, want 0", errno)
	}
	_ = node.Release(context.Background(), h)
}

// TestSetattr_MtimeWriteback: utimensat 设置 mtime 后必须写回本地元数据缓存。
func TestSetattr_MtimeWriteback(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	ctx := newContext(opts, newMockFS())
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MTIME
	in.Mtime = 123456789
	node := &fileNode{fs: ctx, path: "/f", entry: &mount.RemoteEntry{Path: "/f", Size: 1}}
	var out fuse.AttrOut
	if errno := node.Setattr(context.Background(), nil, in, &out); errno != 0 {
		t.Fatalf("Setattr errno=%v", errno)
	}
	if out.Attr.Mtime != 123456789 {
		t.Fatalf("out mtime=%d", out.Attr.Mtime)
	}
	got, ok := ctx.meta.Get("/f")
	if !ok || got.MtimeSec != 123456789 {
		t.Fatalf("meta entry=%+v ok=%v, want mtime 123456789", got, ok)
	}
}
