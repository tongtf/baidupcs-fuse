package adapter

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"baidupcs-fuse/mount"
)

// MockPCSClient 实现 PCSClient 接口，用于单元测试。
type MockPCSClient struct {
	statResult   *mount.RemoteEntry
	statErr      error
	readDirResult []*mount.RemoteEntry
	readDirErr   error
	dlink        string
	dlinkFsID    int64
	dlinkSize    int64
	dlinkMtime   int64
	dlinkErr     error
	quotaTotal   int64
	quotaUsed    int64
	quotaErr     error
	rapidErr     error // 注入 RapidUpload 错误
	precreateErr error // 注入 PrecreateUpload 错误
}

func (m *MockPCSClient) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	return m.statResult, m.statErr
}

func (m *MockPCSClient) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	return m.readDirResult, m.readDirErr
}

func (m *MockPCSClient) LocateDownload(ctx context.Context, path string) (string, int64, int64, int64, error) {
	return m.dlink, m.dlinkFsID, m.dlinkSize, m.dlinkMtime, m.dlinkErr
}

func (m *MockPCSClient) QuotaInfo(ctx context.Context) (int64, int64, error) {
	return m.quotaTotal, m.quotaUsed, m.quotaErr
}

func (m *MockPCSClient) Mkdir(ctx context.Context, path string) error   { return nil }
func (m *MockPCSClient) Remove(ctx context.Context, paths ...string) error { return nil }
func (m *MockPCSClient) Rename(ctx context.Context, from, to string) error { return nil }

func (m *MockPCSClient) RapidUpload(ctx context.Context, remotePath string, size int64, contentMD5, sliceMD5, dataContent, policy, uploadID string, blockListMD5 []string) error {
	return m.rapidErr
}

func (m *MockPCSClient) PrecreateUpload(ctx context.Context, remotePath string, size int64, blockListMD5 []string) (string, error) {
	return "mock-upload-id", m.precreateErr
}

func (m *MockPCSClient) UploadSlice(ctx context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (string, error) {
	return "mock-md5", nil
}

func (m *MockPCSClient) CreateSuperFile(ctx context.Context, uploadID, remotePath string, size int64, checksumMap map[int]string) error {
	return nil
}

func (m *MockPCSClient) UpdateStoken(newToken string) {}

func TestStat_Success(t *testing.T) {
	client := &MockPCSClient{
		statResult: &mount.RemoteEntry{FSID: 1, Path: "/a.txt", Name: "a.txt", Size: 100, Mode: 0644},
	}
	fs := NewBaiduCloudFS(client, NewDlinkCache(30*time.Minute), "test")
	entry, err := fs.Stat(context.Background(), "/a.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry.FSID != 1 || entry.Size != 100 {
		t.Errorf("entry = %+v, want FSID=1 Size=100", entry)
	}
}

func TestStat_NotFound(t *testing.T) {
	client := &MockPCSClient{statErr: os.ErrNotExist}
	fs := NewBaiduCloudFS(client, NewDlinkCache(30*time.Minute), "test")
	_, err := fs.Stat(context.Background(), "/nope")
	if err != os.ErrNotExist {
		t.Errorf("err = %v, want os.ErrNotExist", err)
	}
}

func TestOpenRead_3Step_VersionPin(t *testing.T) {
	client := &MockPCSClient{
		statResult: &mount.RemoteEntry{FSID: 1, Size: 1000, MtimeSec: 100},
		dlink:      "https://example.com/dl",
		dlinkFsID:  1,
	}
	fs := NewBaiduCloudFS(client, NewDlinkCache(30*time.Minute), "test")
	reader, err := fs.OpenRead(context.Background(), "/test.bin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reader.Size() != 1000 {
		t.Errorf("size = %d, want 1000", reader.Size())
	}
}

func TestOpenRead_VersionConflict(t *testing.T) {
	callCount := 0
	// 自定义 client 让两次 Stat 返回不同结果
	client := &conflictMockClient{callCount: &callCount}
	fs := NewBaiduCloudFS(client, NewDlinkCache(30*time.Minute), "test")
	_, err := fs.OpenRead(context.Background(), "/changed.txt")
	if err != mount.ErrVersionConflict {
		t.Errorf("err = %v, want ErrVersionConflict", err)
	}
}

func TestStatFS_Success(t *testing.T) {
	client := &MockPCSClient{quotaTotal: 1000, quotaUsed: 300}
	fs := NewBaiduCloudFS(client, NewDlinkCache(30*time.Minute), "test")
	stats, err := fs.StatFS(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Total != 1000 || stats.Used != 300 || stats.Free != 700 {
		t.Errorf("stats = %+v, want Total=1000 Used=300 Free=700", stats)
	}
}

func TestReadDir_Success(t *testing.T) {
	client := &MockPCSClient{
		readDirResult: []*mount.RemoteEntry{
			{FSID: 1, Name: "a.txt", IsDir: false, Size: 10},
			{FSID: 2, Name: "sub", IsDir: true, Size: 0},
		},
	}
	fs := NewBaiduCloudFS(client, NewDlinkCache(30*time.Minute), "test")
	entries, err := fs.ReadDir(context.Background(), "/docs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
}

func TestDlinkCache_GetSet(t *testing.T) {
	c := NewDlinkCache(30 * time.Minute)
	c.Set(1, "https://example.com/dl1", "/test.bin")
	dlink, ok := c.Get(1)
	if !ok || dlink != "https://example.com/dl1" {
		t.Errorf("Get(1) = %q, %v, want https://example.com/dl1, true", dlink, ok)
	}
}

func TestDlinkCache_Expires(t *testing.T) {
	c := NewDlinkCache(10 * time.Millisecond)
	c.Set(1, "https://example.com/dl1", "/test.bin")
	time.Sleep(15 * time.Millisecond)
	_, ok := c.Get(1)
	if ok {
		t.Error("expected expired")
	}
}

func TestDlinkCache_Invalidate(t *testing.T) {
	c := NewDlinkCache(30 * time.Minute)
	c.Set(1, "https://example.com/dl1", "/test.bin")
	c.Invalidate(1)
	_, ok := c.Get(1)
	if ok {
		t.Error("expected invalidated")
	}
}

func TestDlinkCache_InvalidateByPath(t *testing.T) {
	c := NewDlinkCache(30 * time.Minute)
	// 同一路径两个 fsID（模拟重命名后新旧 fsID）
	c.Set(10, "https://example.com/dl10", "/docs/a.txt")
	c.Set(11, "https://example.com/dl11", "/docs/a.txt")
	// 不同路径
	c.Set(20, "https://example.com/dl20", "/docs/b.txt")

	// 按路径精确失效
	c.InvalidateByPath("/docs/a.txt")

	// /docs/a.txt 的两个 fsID 应全部失效
	if _, ok := c.Get(10); ok {
		t.Error("fsID 10 should be invalidated")
	}
	if _, ok := c.Get(11); ok {
		t.Error("fsID 11 should be invalidated")
	}
	// /docs/b.txt 应保留
	if _, ok := c.Get(20); !ok {
		t.Error("fsID 20 should still exist")
	}
}

func TestDlinkCache_InvalidateByPath_NoMatch(t *testing.T) {
	c := NewDlinkCache(30 * time.Minute)
	c.Set(1, "https://example.com/dl1", "/test.bin")

	// 不存在的路径不应影响现有缓存
	c.InvalidateByPath("/nonexistent.txt")
	if _, ok := c.Get(1); !ok {
		t.Error("existing entry should not be affected")
	}
}

func TestDlinkCache_InvalidateByPath_EmptyCache(t *testing.T) {
	c := NewDlinkCache(30 * time.Minute)
	// 空缓存不应 panic
	c.InvalidateByPath("/test.bin")
}

func TestDlinkCache_InvalidateByPath_EmptyString(t *testing.T) {
	c := NewDlinkCache(30 * time.Minute)
	c.Set(1, "https://example.com/dl1", "")
	// 空字符串路径匹配
	c.InvalidateByPath("")
	if _, ok := c.Get(1); ok {
		t.Error("empty string path should match")
	}
}

func TestDlinkCache_InvalidateByPath_Concurrent(t *testing.T) {
	c := NewDlinkCache(30 * time.Minute)
	for i := int64(0); i < 100; i++ {
		c.Set(i, "https://example.com/dl", "/file.txt")
	}
	// 并发 InvalidateByPath 不应 panic
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.InvalidateByPath("/file.txt")
		}()
	}
	wg.Wait()
	// 全部应被清除
	for i := int64(0); i < 100; i++ {
		if _, ok := c.Get(i); ok {
			t.Errorf("fsID %d should be invalidated", i)
		}
	}
}

func TestDlinkCache_InvalidateByPath_SingleFsID(t *testing.T) {
	c := NewDlinkCache(30 * time.Minute)
	// 一个 fsID 只对应一个路径
	c.Set(1, "https://example.com/dl1", "/a.txt")
	// 失效 /b.txt 不应影响 /a.txt
	c.InvalidateByPath("/b.txt")
	if _, ok := c.Get(1); !ok {
		t.Error("fsID 1 should not be invalidated")
	}
}

// conflictMockClient 模拟版本冲突：Stat 第 1 次返回 fsid=1，第 2 次返回 fsid=2，
// LocateDownload 返回 fsid=2 → 触发 OpenRead 版本冲突。
type conflictMockClient struct {
	callCount *int
}

func (m *conflictMockClient) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	*m.callCount++
	if *m.callCount == 1 {
		return &mount.RemoteEntry{FSID: 1, Size: 100, MtimeSec: 100}, nil
	}
	// 第 2 次及以后返回 fsid=2（与 LocateDownload 一致但与 entry1 不同）
	return &mount.RemoteEntry{FSID: 2, Size: 100, MtimeSec: 100}, nil
}

func (m *conflictMockClient) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	return nil, nil
}

func (m *conflictMockClient) LocateDownload(ctx context.Context, path string) (string, int64, int64, int64, error) {
	return "https://example.com/dl", 2, 100, 100, nil
}

func (m *conflictMockClient) QuotaInfo(ctx context.Context) (int64, int64, error) {
	return 0, 0, nil
}

func (m *conflictMockClient) Mkdir(ctx context.Context, path string) error    { return nil }
func (m *conflictMockClient) Remove(ctx context.Context, paths ...string) error { return nil }
func (m *conflictMockClient) Rename(ctx context.Context, from, to string) error { return nil }

func (m *conflictMockClient) RapidUpload(ctx context.Context, remotePath string, size int64, contentMD5, sliceMD5, dataContent, policy, uploadID string, blockListMD5 []string) error {
	return nil
}

func (m *conflictMockClient) PrecreateUpload(ctx context.Context, remotePath string, size int64, blockListMD5 []string) (string, error) {
	return "", nil
}

func (m *conflictMockClient) UploadSlice(ctx context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (string, error) {
	return "", nil
}

func (m *conflictMockClient) CreateSuperFile(ctx context.Context, uploadID, remotePath string, size int64, checksumMap map[int]string) error {
	return nil
}

func (m *conflictMockClient) UpdateStoken(newToken string) {}

// ============================================================
// backfillGap tests
// ============================================================

// mockReader 实现 mount.RemoteReader，返回预设数据。
type mockReader struct {
	data []byte
}

func (r *mockReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	end := off + int64(size)
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	return r.data[off:end], nil
}
func (r *mockReader) Size() int64    { return int64(len(r.data)) }
func (r *mockReader) FSID() int64    { return 1 }
func (r *mockReader) Mtime() int64   { return 0 }
func (r *mockReader) RefreshDlink(ctx context.Context, path string) error { return nil }
func (r *mockReader) Close() error   { return nil }

func TestBackfillGap_NilReader(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	s, _ := mgr.Open("/test.txt")
	defer mgr.CloseAndRemove("/test.txt")

	w := &baiduRemoteWriter{session: s, path: "/test.txt", stagingMgr: mgr}
	// nil reader → 无回填，直接写
	if err := w.Write(context.Background(), 10, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	data, _ := os.ReadFile(s.tmpPath)
	if string(data[10:15]) != "hello" {
		t.Errorf("got %q, want %q", string(data[10:15]), "hello")
	}
}

func TestBackfillGap_WithGap(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	s, _ := mgr.Open("/test.txt")
	defer mgr.CloseAndRemove("/test.txt")

	// 远端旧文件内容："ABCDEFGHIJ"（10 字节）
	reader := &mockReader{data: []byte("ABCDEFGHIJ")}
	w := &baiduRemoteWriter{session: s, path: "/test.txt", stagingMgr: mgr}
	w.SetReader(reader)

	// 写到 offset=5，触发回填 [0,5) → "ABCDE"
	if err := w.Write(context.Background(), 5, []byte("XY")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	data, _ := os.ReadFile(s.tmpPath)
	// 期望："ABCDEXY"（回填 0-5 + 写入 XY）
	got := string(data)
	want := "ABCDEXY"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBackfillGap_NoGap(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	s, _ := mgr.Open("/test.txt")
	defer mgr.CloseAndRemove("/test.txt")

	reader := &mockReader{data: []byte("ABCDEFGHIJ")}
	w := &baiduRemoteWriter{session: s, path: "/test.txt", stagingMgr: mgr}
	w.SetReader(reader)

	// 连续写：先 [0,5)，再 [5,10)，无 gap
	w.Write(context.Background(), 0, []byte("ABCDE"))
	w.Write(context.Background(), 5, []byte("FGHIJ"))
	s.Close()

	data, _ := os.ReadFile(s.tmpPath)
	got := string(data)
	want := "ABCDEFGHIJ"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBackfillGap_UploadFailure_RetainsStaging(t *testing.T) {
	dir := t.TempDir()
	client := &MockPCSClient{rapidErr: os.ErrNotExist, precreateErr: os.ErrNotExist}
	dlinkCache := NewDlinkCache(30 * time.Minute)
	fs := NewBaiduCloudFS(client, dlinkCache, "test")
	stagingMgr := NewStagingManager(dir, true)
	uploadSess := NewUploadSession(client, stagingMgr)
	fs.SetWriteSupport(stagingMgr, uploadSess)

	w, err := fs.CreateWriter(context.Background(), "/upload-fail.txt")
	if err != nil {
		t.Fatal(err)
	}
	w.Write(context.Background(), 0, []byte("data"))

	// Close 触发后台上传（异步，立即返回 nil）；staging 文件应保留供 recovery。
	err = w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close should return nil (async upload); got %v", err)
	}

	// 等待后台 goroutine 完成并将失败记录到 manifest（poll，避免与异步提交竞态）
	manifestPath := filepath.Join(dir, "failed-uploads", "manifest.json")
	foundManifest := false
	for i := 0; i < 200; i++ {
		if data, rerr := os.ReadFile(manifestPath); rerr == nil && bytes.Contains(data, []byte("/upload-fail.txt")) {
			foundManifest = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !foundManifest {
		t.Fatal("expected failed-upload manifest entry after async upload failure")
	}

	// session 已清除（失败只清 session map，不清磁盘文件）
	if stagingMgr.HasSession("/upload-fail.txt") {
		t.Error("session should be cleared after upload failure")
	}

	// staging 目录应保留（uploads 目录下仍有文件）供 mount recover 使用
	entries, _ := os.ReadDir(filepath.Join(dir, "uploads"))
	if len(entries) == 0 {
		t.Error("staging directory should be retained after upload failure")
	}
}
