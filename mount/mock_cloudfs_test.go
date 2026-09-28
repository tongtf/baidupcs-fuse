package mount

import (
	"context"
	"os"
	"sync"
	"testing"
)

// MockCloudFS 内存 map 实现 CloudFS，用于单元测试。
type MockCloudFS struct {
	mu      sync.RWMutex
	files   map[string][]byte   // path → content
	dirs    map[string][]string // dir → child names
	statErr map[string]error    // path → 注入的 Stat 错误
	openErr map[string]error    // path → 注入的 OpenRead 错误
}

func NewMockCloudFS() *MockCloudFS {
	return &MockCloudFS{
		files:   make(map[string][]byte),
		dirs:    make(map[string][]string),
		statErr: make(map[string]error),
		openErr: make(map[string]error),
	}
}

func (m *MockCloudFS) AddFile(path string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
}

func (m *MockCloudFS) AddDir(path string, children []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirs[path] = children
}

func (m *MockCloudFS) Stat(ctx context.Context, path string) (*RemoteEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err, ok := m.statErr[path]; ok {
		return nil, err
	}
	if data, ok := m.files[path]; ok {
		return &RemoteEntry{FSID: 1, Path: path, Name: path, IsDir: false, Size: int64(len(data)), Mode: 0644}, nil
	}
	if _, ok := m.dirs[path]; ok {
		return &RemoteEntry{FSID: 2, Path: path, Name: path, IsDir: true, Size: 0, Mode: 0755}, nil
	}
	return nil, os.ErrNotExist
}

func (m *MockCloudFS) ReadDir(ctx context.Context, path string) ([]*RemoteEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	children, ok := m.dirs[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	var entries []*RemoteEntry
	for _, name := range children {
		childPath := path + "/" + name
		if data, ok := m.files[childPath]; ok {
			entries = append(entries, &RemoteEntry{FSID: 1, Path: childPath, Name: name, IsDir: false, Size: int64(len(data)), Mode: 0644})
		} else if _, ok := m.dirs[childPath]; ok {
			entries = append(entries, &RemoteEntry{FSID: 2, Path: childPath, Name: name, IsDir: true, Size: 0, Mode: 0755})
		}
	}
	return entries, nil
}

func (m *MockCloudFS) OpenRead(ctx context.Context, path string) (RemoteReader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err, ok := m.openErr[path]; ok {
		return nil, err
	}
	data, ok := m.files[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return &mockRemoteReader{data: data, size: int64(len(data))}, nil
}

func (m *MockCloudFS) StatFS(ctx context.Context) (*FSStats, error) {
	return &FSStats{Total: 100 << 30, Used: 0, Free: 100 << 30}, nil
}

func (m *MockCloudFS) Mkdir(ctx context.Context, path string) error      { return nil }
func (m *MockCloudFS) Remove(ctx context.Context, paths ...string) error { return nil }
func (m *MockCloudFS) Rename(ctx context.Context, from, to string) error { return nil }
func (m *MockCloudFS) CreateWriter(ctx context.Context, path string) (RemoteWriter, error) {
	return nil, ErrWriteNotEnabled
}

// mockRemoteReader 内存 RemoteReader 实现。
type mockRemoteReader struct {
	data   []byte
	size   int64
	closed bool
}

func (r *mockRemoteReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	if r.closed {
		return nil, os.ErrClosed
	}
	if off < 0 || size < 0 || off+int64(size) > r.size {
		return nil, os.ErrInvalid
	}
	buf := make([]byte, size)
	copy(buf, r.data[off:off+int64(size)])
	return buf, nil
}

func (r *mockRemoteReader) Size() int64                                         { return r.size }
func (r *mockRemoteReader) FSID() int64                                         { return 1 }
func (r *mockRemoteReader) Mtime() int64                                        { return 0 }
func (r *mockRemoteReader) RefreshDlink(ctx context.Context, path string) error { return nil }
func (r *mockRemoteReader) Close() error                                        { r.closed = true; return nil }

// --- 单测 ---

func TestMock_Stat_Exists(t *testing.T) {
	m := NewMockCloudFS()
	m.AddFile("/test.txt", []byte("hello"))
	entry, err := m.Stat(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry.IsDir {
		t.Error("expected file, got dir")
	}
	if entry.Size != 5 {
		t.Errorf("size = %d, want 5", entry.Size)
	}
}

func TestMock_Stat_NotExists(t *testing.T) {
	m := NewMockCloudFS()
	_, err := m.Stat(context.Background(), "/nope")
	if err != os.ErrNotExist {
		t.Errorf("err = %v, want os.ErrNotExist", err)
	}
}

func TestMock_ReadDir(t *testing.T) {
	m := NewMockCloudFS()
	m.AddDir("/docs", []string{"a.txt", "b.txt"})
	m.AddFile("/docs/a.txt", []byte("aaa"))
	m.AddFile("/docs/b.txt", []byte("bbb"))
	entries, err := m.ReadDir(context.Background(), "/docs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
}

func TestMock_OpenRead(t *testing.T) {
	m := NewMockCloudFS()
	m.AddFile("/data.bin", []byte("0123456789"))
	r, err := m.OpenRead(context.Background(), "/data.bin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Size() != 10 {
		t.Errorf("size = %d, want 10", r.Size())
	}
}

func TestMock_ReadAt_Correct(t *testing.T) {
	m := NewMockCloudFS()
	m.AddFile("/f.txt", []byte("0123456789"))
	r, _ := m.OpenRead(context.Background(), "/f.txt")
	data, err := r.ReadAt(context.Background(), 0, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "01234" {
		t.Errorf("data = %q, want %q", data, "01234")
	}
}

func TestMock_ReadAt_ExceedEOF(t *testing.T) {
	m := NewMockCloudFS()
	m.AddFile("/f.txt", []byte("abc"))
	r, _ := m.OpenRead(context.Background(), "/f.txt")
	_, err := r.ReadAt(context.Background(), 0, 100)
	if err == nil {
		t.Error("expected error for exceed EOF")
	}
}

func TestMock_ReadAt_Close(t *testing.T) {
	m := NewMockCloudFS()
	m.AddFile("/f.txt", []byte("abc"))
	r, _ := m.OpenRead(context.Background(), "/f.txt")
	r.Close()
	_, err := r.ReadAt(context.Background(), 0, 1)
	if err != os.ErrClosed {
		t.Errorf("err = %v, want os.ErrClosed", err)
	}
}
