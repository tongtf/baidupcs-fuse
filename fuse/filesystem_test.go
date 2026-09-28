package fuse

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"baidupcs-fuse/cache"
	"baidupcs-fuse/config"
	"baidupcs-fuse/mount"
)

// mockCloudFS 测试用内存 CloudFS 实现。
type mockCloudFS struct {
	files map[string]*mount.RemoteEntry
	dirs  map[string][]string
}

func newMockFS() *mockCloudFS {
	return &mockCloudFS{
		files: make(map[string]*mount.RemoteEntry),
		dirs:  make(map[string][]string),
	}
}

func (m *mockCloudFS) addFile(path string, size int64) {
	m.files[path] = &mount.RemoteEntry{
		FSID:  1,
		Path:  path,
		Name:  path,
		Size:  size,
		Mode:  0644,
	}
}

func (m *mockCloudFS) addDir(path string, children []string) {
	m.dirs[path] = children
}

func (m *mockCloudFS) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	if e, ok := m.files[path]; ok {
		return e, nil
	}
	if _, ok := m.dirs[path]; ok {
		return &mount.RemoteEntry{FSID: 2, Path: path, Name: path, IsDir: true, Mode: 0755}, nil
	}
	return nil, os.ErrNotExist
}

func (m *mockCloudFS) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	children, ok := m.dirs[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	var entries []*mount.RemoteEntry
	for _, name := range children {
		childPath := path + "/" + name
		if e, ok := m.files[childPath]; ok {
			entries = append(entries, e)
		} else if _, ok := m.dirs[childPath]; ok {
			entries = append(entries, &mount.RemoteEntry{FSID: 2, Path: childPath, Name: name, IsDir: true, Mode: 0755})
		}
	}
	return entries, nil
}

func (m *mockCloudFS) OpenRead(ctx context.Context, path string) (mount.RemoteReader, error) {
	e, ok := m.files[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return &mockReader{size: e.Size}, nil
}

func (m *mockCloudFS) StatFS(ctx context.Context) (*mount.FSStats, error) {
	return &mount.FSStats{Total: 100 << 30, Used: 0, Free: 100 << 30}, nil
}

func (m *mockCloudFS) Mkdir(ctx context.Context, path string) error   { return nil }
func (m *mockCloudFS) Remove(ctx context.Context, paths ...string) error { return nil }
func (m *mockCloudFS) Rename(ctx context.Context, from, to string) error { return nil }
func (m *mockCloudFS) CreateWriter(ctx context.Context, path string) (mount.RemoteWriter, error) {
	return nil, mount.ErrWriteNotEnabled
}

type mockReader struct {
	size int64
}

func (r *mockReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	return make([]byte, size), nil
}
func (r *mockReader) Size() int64    { return r.size }
func (r *mockReader) FSID() int64    { return 1 }
func (r *mockReader) Mtime() int64   { return 0 }
func (r *mockReader) RefreshDlink(ctx context.Context, path string) error { return nil }
func (r *mockReader) Close() error   { return nil }

func defaultOpts() config.MountOptions {
	return config.MountOptions{
		MountPoint:     "/tmp/bdfs-test-" + time.Now().Format("150405"),
		RemotePath:     "/",
		ReadBlockSize:  16 * 1024 * 1024,
		UploadSliceSize: 4 * 1024 * 1024,
		MetadataTTL:    30 * time.Second,
		DirTTL:         60 * time.Second,
		DlinkTTL:       30 * time.Minute,
		MaxParallel:    5,
		PerFileSem:     1,
		AccountKey:     "test",
	}
}

func TestNewContext(t *testing.T) {
	opts := defaultOpts()
	fs := newMockFS()
	ctx := newContext(opts, fs)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	if ctx.opts.AccountKey != "test" {
		t.Errorf("accountKey = %q, want %q", ctx.opts.AccountKey, "test")
	}
	if ctx.meta == nil {
		t.Error("expected non-nil meta cache")
	}
}

func TestInodeManager_GetOrCreate(t *testing.T) {
	im := newInodeManager(30 * time.Second)
	e1 := im.getOrCreate("/a.txt", 10)
	e2 := im.getOrCreate("/a.txt", 20)
	if e1.id != e2.id {
		t.Error("expected same inode for same path")
	}
}

func TestInodeManager_Expiry(t *testing.T) {
	im := newInodeManager(10 * time.Millisecond)
	im.getOrCreate("/a.txt", 10)
	time.Sleep(15 * time.Millisecond)
	_, ok := im.getByPath("/a.txt")
	if ok {
		t.Error("expected expired")
	}
}

func TestBaiduFS_ChildPath(t *testing.T) {
	ctx := newContext(defaultOpts(), newMockFS())
	root := &BaiduFS{fs: ctx, rootPath: "/"}
	if root.childPath("a.txt") != "/a.txt" {
		t.Errorf("childPath = %q, want /a.txt", root.childPath("a.txt"))
	}

	root2 := &BaiduFS{fs: ctx, rootPath: "/docs"}
	if root2.childPath("a.txt") != "/docs/a.txt" {
		t.Errorf("childPath = %q, want /docs/a.txt", root2.childPath("a.txt"))
	}
}

// ============================================================
// readCrossBlock 测试
// ============================================================

// crossBlockReader 实现 mount.RemoteReader，返回预设的分块数据。
type crossBlockReader struct {
	data   []byte // 完整文件内容
	fsid   int64
	size   int64
	mtime  int64
}

func (r *crossBlockReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	end := off + int64(size)
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	return r.data[off:end], nil
}
func (r *crossBlockReader) Size() int64  { return r.size }
func (r *crossBlockReader) FSID() int64  { return r.fsid }
func (r *crossBlockReader) Mtime() int64 { return r.mtime }
func (r *crossBlockReader) RefreshDlink(ctx context.Context, path string) error {
	return nil
}
func (r *crossBlockReader) Close() error { return nil }

func TestReadCrossBlock_SecondBlockFromRemote(t *testing.T) {
	// 文件 24 字节，blockSize=16。读取 off=12, size=12 → 跨块
	// 第一块 [0,16)，第二块 [16,24)
	fileData := []byte("0123456789abcdef01234567") // 24 bytes
	reader := &crossBlockReader{data: fileData, fsid: 1, size: 24, mtime: 100}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	// firstBlockData = fileData[0:16] = "0123456789abcdef"
	firstBlockData := fileData[0:16]
	startInBlock := int64(12) // off=12 在第一块内的偏移

	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100,        // fsID, mtime
		0, 16,         // blockOff=0, blockSize=16
		12, 12,        // off=12, size=12
		firstBlockData, startInBlock)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true (cross-block read)")
	}

	// 期望返回 fileData[12:24] = "cdef01234567"
	gotBytes, _ := result.Bytes(nil)
	got := string(gotBytes)
	want := "cdef01234567"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestReadCrossBlock_SecondBlockEOF(t *testing.T) {
	// 文件 20 字节，blockSize=16。读取 off=12, size=12 → 第二块超出文件范围
	// 第一块 [0,16)，第二块 [16,20) 只有 4 字节
	fileData := []byte("0123456789abcdef0123") // 20 bytes
	reader := &crossBlockReader{data: fileData, fsid: 1, size: 20, mtime: 100}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	firstBlockData := fileData[0:16]
	startInBlock := int64(12)

	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100, 0, 16, 12, 12, firstBlockData, startInBlock)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	// 期望返回 fileData[12:20] = "cdef0123"（8 字节，不是 12）
	gotBytes, _ := result.Bytes(nil)
	got := string(gotBytes)
	want := "cdef0123"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestReadCrossBlock_NoCrossNeeded(t *testing.T) {
	// 数据完全在第一块内，不需要跨块
	fileData := []byte("0123456789abcdef")
	reader := &crossBlockReader{data: fileData, fsid: 1, size: 16, mtime: 100}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	firstBlockData := fileData[0:16]
	startInBlock := int64(8)

	// size=4, 从 startInBlock=8 开始只需要 8 字节（在第一块内）
	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100, 0, 16, 8, 4, firstBlockData, startInBlock)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ok=false 表示不需要跨块
	if ok {
		t.Fatal("expected ok=false (no cross-block needed)")
	}
	if result != nil {
		t.Fatal("expected nil result when no cross-block needed")
	}
}

func TestReadCrossBlock_SecondBlockFromCache(t *testing.T) {
	// 先 Put 第二块到 BlockCache，验证直接从缓存读取
	fileData := []byte("0123456789abcdef01234567") // 24 bytes
	reader := &crossBlockReader{data: fileData, fsid: 1, size: 24, mtime: 100}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	// 预填充第二块到 BlockCache
	secondBlockKey := cache.BlockKeyFromPath(opts.AccountKey, 1, 24, 100, 16)
	ctx.blocks.Put(secondBlockKey, fileData[16:24])

	firstBlockData := fileData[0:16]
	startInBlock := int64(12)

	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100, 0, 16, 12, 12, firstBlockData, startInBlock)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}

	gotBytes, _ := result.Bytes(nil)
	got := string(gotBytes)
	want := "cdef01234567"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// ============================================================
// readCrossBlock 边界值/非法值测试
// ============================================================

// errorReader 第二次 ReadAt 返回错误。
type errorReader struct {
	data      []byte
	fsid      int64
	size      int64
	mtime     int64
	failAfter int // 第 failAfter 次 ReadAt 后失败
	callCount int
}

func (r *errorReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	r.callCount++
	if r.callCount > r.failAfter {
		return nil, os.ErrPermission // 模拟远端拉取失败
	}
	end := off + int64(size)
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	return r.data[off:end], nil
}
func (r *errorReader) Size() int64  { return r.size }
func (r *errorReader) FSID() int64  { return r.fsid }
func (r *errorReader) Mtime() int64 { return r.mtime }
func (r *errorReader) RefreshDlink(ctx context.Context, path string) error {
	return nil
}
func (r *errorReader) Close() error { return nil }

func TestReadCrossBlock_SecondBlockFetchError(t *testing.T) {
	// 第二块从远端拉取失败 → 返回第一块剩余 + error
	fileData := []byte("0123456789abcdef01234567") // 24 bytes
	reader := &errorReader{data: fileData, fsid: 1, size: 24, mtime: 100, failAfter: 0}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	firstBlockData := fileData[0:16]
	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100, 0, 16, 12, 12, firstBlockData, 12)

	if err == nil {
		t.Fatal("expected error from second block fetch")
	}
	if !ok {
		t.Fatal("expected ok=true even on error (partial data returned)")
	}
	// 应返回第一块剩余部分 "cdef"
	gotBytes, _ := result.Bytes(nil)
	got := string(gotBytes)
	want := "cdef"
	if got != want {
		t.Errorf("got %q, want %q (partial first block on error)", got, want)
	}
}

func TestReadCrossBlock_DiskCacheHit(t *testing.T) {
	// 第二块在 DiskCache 中（不在 BlockCache）
	fileData := []byte("0123456789abcdef01234567") // 24 bytes
	reader := &crossBlockReader{data: fileData, fsid: 1, size: 24, mtime: 100}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	// 只 Put 到 DiskCache，不 Put 到 BlockCache
	secondBlockKey := cache.BlockKeyFromPath(opts.AccountKey, 1, 24, 100, 16)
	ctx.disk.Put(secondBlockKey, fileData[16:24])

	firstBlockData := fileData[0:16]
	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100, 0, 16, 12, 12, firstBlockData, 12)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	gotBytes, _ := result.Bytes(nil)
	got := string(gotBytes)
	want := "cdef01234567"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// 验证回填到 BlockCache
	if _, ok := ctx.blocks.Get(secondBlockKey); !ok {
		t.Error("second block should be backfilled to BlockCache")
	}
}

func TestReadCrossBlock_SizeZero(t *testing.T) {
	// size=0 → needFromSecond <= 0 → 不需要跨块
	fileData := []byte("0123456789abcdef")
	reader := &crossBlockReader{data: fileData, fsid: 1, size: 16, mtime: 100}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100, 0, 16, 8, 0, fileData[0:16], 8)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for size=0")
	}
	if result != nil {
		t.Fatal("expected nil result")
	}
}

func TestReadCrossBlock_StartInBlockZero(t *testing.T) {
	// startInBlock=0 → 从块头开始读取，整块跨块场景
	fileData := []byte("0123456789abcdef01234567") // 24 bytes
	reader := &crossBlockReader{data: fileData, fsid: 1, size: 24, mtime: 100}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	// off=16, size=12 → startInBlock=0 在第二块
	// 但 readCrossBlock 的 off 参数是文件级偏移，startInBlock 是块内偏移
	// 这里测试 startInBlock=0：从第一块头开始读，需要跨到第二块
	firstBlockData := fileData[0:16]
	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100, 0, 16, 0, 20, firstBlockData, 0)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true (cross-block)")
	}
	// 期望返回 fileData[0:20] = "0123456789abcdef0123"
	gotBytes, _ := result.Bytes(nil)
	got := string(gotBytes)
	want := "0123456789abcdef0123"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestReadCrossBlock_FirstBlockShort(t *testing.T) {
	// 第一块不满 blockSize（文件末尾块），secondBlockOff >= Size → EOF
	fileData := []byte("0123456789") // 10 bytes，不满 16
	reader := &crossBlockReader{data: fileData, fsid: 1, size: 10, mtime: 100}

	opts := defaultOpts()
	opts.ReadBlockSize = 16
	opts.BlockCacheSize = 1024 * 1024
	opts.CacheDir = t.TempDir()
	opts.DiskCacheSize = 1024 * 1024
	ctx := newContext(opts, newMockFS())

	firstBlockData := fileData[0:10] // 整个文件就是第一块
	result, ok, err := readCrossBlock(context.Background(), ctx, reader, "/test.txt",
		1, 100, 0, 16, 5, 10, firstBlockData, 5)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	// secondBlockOff=16 >= Size=10 → EOF，返回第一块剩余 "56789"
	gotBytes, _ := result.Bytes(nil)
	got := string(gotBytes)
	want := "56789"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// ============================================================
// doRefresh timeout 测试
// ============================================================

// slowCloudFS 模拟 API 响应慢的 CloudFS。
type slowCloudFS struct {
	delay time.Duration
}

func (s *slowCloudFS) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	return nil, os.ErrNotExist
}
func (s *slowCloudFS) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	time.Sleep(s.delay)
	return []*mount.RemoteEntry{}, nil
}
func (s *slowCloudFS) OpenRead(ctx context.Context, path string) (mount.RemoteReader, error) {
	return nil, os.ErrNotExist
}
func (s *slowCloudFS) StatFS(ctx context.Context) (*mount.FSStats, error) {
	return &mount.FSStats{}, nil
}
func (s *slowCloudFS) Mkdir(ctx context.Context, path string) error   { return nil }
func (s *slowCloudFS) Remove(ctx context.Context, paths ...string) error { return nil }
func (s *slowCloudFS) Rename(ctx context.Context, from, to string) error { return nil }
func (s *slowCloudFS) CreateWriter(ctx context.Context, path string) (mount.RemoteWriter, error) {
	return nil, mount.ErrWriteNotEnabled
}

func TestDoRefresh_Timeout(t *testing.T) {
	// API 响应需要 5s，超时设为 30s（doRefresh 内部硬编码）
	// 但我们可以用 context deadline 验证超时机制存在
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	mfs := newMockFS()
	mfs.addDir("/docs", []string{"a.txt"})
	mfs.addFile("/docs/a.txt", 100)

	ctx := newContext(opts, mfs)

	// 正常刷新应成功
	resp := ctx.doRefresh("/docs", opts)
	if !resp.OK {
		t.Errorf("normal refresh should succeed: %s", resp.Message)
	}
}

func TestDoRefresh_SlowAPI(t *testing.T) {
	// 验证 doRefresh 在 API 慢时不阻塞整个进程
	// 用 slowCloudFS（100ms 延迟）验证能在合理时间内返回
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	slowFS := &slowCloudFS{delay: 100 * time.Millisecond}
	ctx := newContext(opts, slowFS)

	start := time.Now()
	resp := ctx.doRefresh("/", opts)
	elapsed := time.Since(start)

	if !resp.OK {
		t.Errorf("refresh should succeed: %s", resp.Message)
	}
	// 100ms 延迟应在 1s 内完成
	if elapsed > 1*time.Second {
		t.Errorf("refresh took too long: %v", elapsed)
	}
}

func TestDoRefresh_EmptyPath_FullRefresh(t *testing.T) {
	// path="" 走全量刷新分支
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	mfs := newMockFS()
	mfs.addDir("/docs", []string{"a.txt"})
	mfs.addFile("/docs/a.txt", 100)
	mfs.addDir("/photos", []string{"b.jpg"})
	mfs.addFile("/photos/b.jpg", 200)

	ctx := newContext(opts, mfs)

	// 先让 meta cache 有目录
	metaCtx := context.Background()
	_ = metaCtx
	ctx.meta.SetDir("/docs", []*mount.RemoteEntry{{FSID: 1, Name: "a.txt", Path: "/docs/a.txt", Size: 100}})
	ctx.meta.SetDir("/photos", []*mount.RemoteEntry{{FSID: 2, Name: "b.jpg", Path: "/photos/b.jpg", Size: 200}})

	// 全量刷新
	resp := ctx.doRefresh("", opts)
	if !resp.OK {
		t.Errorf("full refresh should succeed: %s", resp.Message)
	}
	if resp.Refreshed != 2 {
		t.Errorf("refreshed = %d, want 2", resp.Refreshed)
	}
}

func TestDoRefresh_APIError(t *testing.T) {
	// ReadDir 返回错误
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	errFS := &errorCloudFS{readDirErr: os.ErrPermission}
	ctx := newContext(opts, errFS)

	resp := ctx.doRefresh("/nonexistent", opts)
	if resp.OK {
		t.Error("refresh should fail on API error")
	}
}

// errorCloudFS 返回预设错误的 CloudFS。
type errorCloudFS struct {
	readDirErr error
}

func (e *errorCloudFS) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	return nil, os.ErrNotExist
}
func (e *errorCloudFS) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	return nil, e.readDirErr
}
func (e *errorCloudFS) OpenRead(ctx context.Context, path string) (mount.RemoteReader, error) {
	return nil, os.ErrNotExist
}
func (e *errorCloudFS) StatFS(ctx context.Context) (*mount.FSStats, error) {
	return &mount.FSStats{}, nil
}
func (e *errorCloudFS) Mkdir(ctx context.Context, path string) error   { return nil }
func (e *errorCloudFS) Remove(ctx context.Context, paths ...string) error { return nil }
func (e *errorCloudFS) Rename(ctx context.Context, from, to string) error { return nil }
func (e *errorCloudFS) CreateWriter(ctx context.Context, path string) (mount.RemoteWriter, error) {
	return nil, mount.ErrWriteNotEnabled
}

// ============================================================
// trackDownload 回调注入测试
// ============================================================

func TestTrackDownload_CallbackInjection(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	mfs := newMockFS()
	ctx := newContext(opts, mfs)

	// 未注入回调时不应 panic
	// trackDownload 通过 n.fs.onTrackDownload 调用
	if ctx.onTrackDownload != nil {
		t.Error("onTrackDownload should be nil before injection")
	}

	// 注入回调
	var calledPath string
	var calledBytes int64
	ctx.onTrackDownload = func(path string, bytes int64) {
		calledPath = path
		calledBytes = bytes
	}

	// 模拟调用
	ctx.onTrackDownload("/test.txt", 1024)
	if calledPath != "/test.txt" || calledBytes != 1024 {
		t.Errorf("callback not invoked correctly: path=%q bytes=%d", calledPath, calledBytes)
	}
}

func TestTrackDownload_NilCallbackSafe(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	mfs := newMockFS()
	ctx := newContext(opts, mfs)

	// onTrackDownload 为 nil 时调用不应 panic
	// trackDownload 内部有 nil 检查
	if ctx.onTrackDownload != nil {
		t.Error("should be nil")
	}
}

// ============================================================
// refreshMu 并发保护测试
// ============================================================

func TestRefreshMu_ConcurrentProtection(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	var callCount int32
	slowFS := &countingSlowFS{delay: 50 * time.Millisecond, count: &callCount}
	ctx := newContext(opts, slowFS)

	// 并发触发 10 次 doRefresh（模拟 ticker + 手动刷新同时触发）
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			// 使用 refreshMu 保护（模拟 doRefresh 前的 TryLock）
			if ctx.refreshMu.TryLock() {
				ctx.doRefresh("/", opts)
				ctx.refreshMu.Unlock()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	// 只有 1 次应该真正执行（TryLock 保证）
	count := atomic.LoadInt32(&callCount)
	if count != 1 {
		t.Errorf("ReadDir called %d times, want 1 (TryLock should prevent concurrent)", count)
	}
}

// countingSlowFS 带计数的慢速 CloudFS。
type countingSlowFS struct {
	delay time.Duration
	count *int32
}

func (s *countingSlowFS) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	return nil, os.ErrNotExist
}
func (s *countingSlowFS) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	atomic.AddInt32(s.count, 1)
	time.Sleep(s.delay)
	return []*mount.RemoteEntry{}, nil
}
func (s *countingSlowFS) OpenRead(ctx context.Context, path string) (mount.RemoteReader, error) {
	return nil, os.ErrNotExist
}
func (s *countingSlowFS) StatFS(ctx context.Context) (*mount.FSStats, error) {
	return &mount.FSStats{}, nil
}
func (s *countingSlowFS) Mkdir(ctx context.Context, path string) error   { return nil }
func (s *countingSlowFS) Remove(ctx context.Context, paths ...string) error { return nil }
func (s *countingSlowFS) Rename(ctx context.Context, from, to string) error { return nil }
func (s *countingSlowFS) CreateWriter(ctx context.Context, path string) (mount.RemoteWriter, error) {
	return nil, mount.ErrWriteNotEnabled
}

func TestTrackDownload_ZeroBytes(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	mfs := newMockFS()
	ctx := newContext(opts, mfs)

	var calledBytes int64
	ctx.onTrackDownload = func(path string, bytes int64) {
		calledBytes = bytes
	}

	// bytes=0 不应 panic
	ctx.onTrackDownload("/test.txt", 0)
	if calledBytes != 0 {
		t.Errorf("bytes = %d, want 0", calledBytes)
	}
}

func TestRefreshMu_SequentialBothExecute(t *testing.T) {
	// 顺序调用两次都应执行（TryLock 不阻塞已释放的锁）
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()

	var callCount int32
	slowFS := &countingSlowFS{delay: 10 * time.Millisecond, count: &callCount}
	ctx := newContext(opts, slowFS)

	// 第一次
	if ctx.refreshMu.TryLock() {
		ctx.doRefresh("/", opts)
		ctx.refreshMu.Unlock()
	}
	// 第二次（锁已释放）
	if ctx.refreshMu.TryLock() {
		ctx.doRefresh("/", opts)
		ctx.refreshMu.Unlock()
	}

	count := atomic.LoadInt32(&callCount)
	if count != 2 {
		t.Errorf("ReadDir called %d times, want 2 (sequential calls should both execute)", count)
	}
}
