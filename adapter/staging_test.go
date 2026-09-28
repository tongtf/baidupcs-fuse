package adapter

import (
	"os"
	"sync"
	"testing"

	"baidupcs-fuse/mount"
)

func TestStagingManager_OpenWriteClose(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)

	s, err := mgr.Open("/test.txt")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	data := []byte("hello world")
	if err := s.Write(0, data); err != nil {
		t.Fatalf("write: %v", err)
	}

	tmpPath, err := s.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	content, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("content = %q, want %q", content, "hello world")
	}
}

func TestStagingManager_DuplicateSession(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)

	s1, _ := mgr.Open("/test.txt")
	defer s1.Close()

	_, err := mgr.Open("/test.txt")
	if err != mount.ErrStagingBusy {
		t.Errorf("err = %v, want ErrStagingBusy", err)
	}
}

func TestStagingManager_WriteAtOffset(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)

	s, _ := mgr.Open("/test.txt")
	s.Write(10, []byte("data"))
	s.Close()

	tmpPath, _ := mgr.TmpPath("/test.txt")
	content, _ := os.ReadFile(tmpPath)
	if len(content) != 14 { // 10 zeros + "data"
		t.Errorf("len = %d, want 14", len(content))
	}
	if string(content[10:]) != "data" {
		t.Errorf("content[10:] = %q, want %q", content[10:], "data")
	}
}

func TestStagingManager_NotEnabled(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, false)

	_, err := mgr.Open("/test.txt")
	if err != mount.ErrWriteNotEnabled {
		t.Errorf("err = %v, want ErrWriteNotEnabled", err)
	}
}

func TestStagingManager_Remove(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)

	s, _ := mgr.Open("/test.txt")
	s.Write(0, []byte("data"))
	s.Close()

	s.Remove()

	_, err := os.Stat(s.tmpPath)
	if !os.IsNotExist(err) {
		t.Error("expected tmp file to be removed")
	}
}

func TestStagingManager_CloseAndRemove(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)

	s, _ := mgr.Open("/test.txt")
	s.Write(0, []byte("data"))
	s.Close()

	mgr.CloseAndRemove("/test.txt")

	if mgr.HasSession("/test.txt") {
		t.Error("expected session to be removed")
	}
}

func TestStagingManager_HasSession(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)

	if mgr.HasSession("/test.txt") {
		t.Error("expected no session before open")
	}

	s, _ := mgr.Open("/test.txt")
	if !mgr.HasSession("/test.txt") {
		t.Error("expected session after open")
	}
	s.Close()

	mgr.CloseAndRemove("/test.txt")
	if mgr.HasSession("/test.txt") {
		t.Error("expected no session after close and remove")
	}
}

// ============================================================
// ClearDirty 生命周期测试
// ============================================================

func TestWriteSession_DirtyLifecycle(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	s, err := mgr.Open("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CloseAndRemove("/test.txt")

	// 新 session 不是 dirty（只有 Write 后才 dirty）
	if s.Dirty() {
		t.Error("new session should not be dirty")
	}

	// 写入数据后应 dirty
	s.Write(0, []byte("hello"))
	if !s.Dirty() {
		t.Error("session should be dirty after write")
	}

	// ClearDirty 后应不再 dirty
	s.ClearDirty()
	if s.Dirty() {
		t.Error("session should not be dirty after ClearDirty")
	}

	// 再次写入后应重新 dirty
	s.Write(5, []byte(" world"))
	if !s.Dirty() {
		t.Error("session should be dirty after second write")
	}
}

func TestWriteSession_DirtyAfterClose(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	s, _ := mgr.Open("/test.txt")
	defer mgr.CloseAndRemove("/test.txt")

	s.Write(0, []byte("data"))
	s.Close()

	// Close 后不应 panic
	// Dirty 在 Close 后的行为取决于实现
}

func TestWriteSession_ClearDirtyOnClean(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	s, _ := mgr.Open("/test.txt")
	defer mgr.CloseAndRemove("/test.txt")

	// 未写入时 ClearDirty 不应 panic
	s.ClearDirty()
	if s.Dirty() {
		t.Error("ClearDirty on clean session should keep it clean")
	}
}

func TestWriteSession_DirtyConcurrent(t *testing.T) {
	dir := t.TempDir()
	mgr := NewStagingManager(dir, true)
	s, _ := mgr.Open("/test.txt")
	defer mgr.CloseAndRemove("/test.txt")

	// 并发 Write + Dirty + ClearDirty 不应 panic 或 race
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				s.Write(0, []byte("data"))
			case 1:
				s.Dirty()
			case 2:
				s.ClearDirty()
			}
		}(i)
	}
	wg.Wait()
}
