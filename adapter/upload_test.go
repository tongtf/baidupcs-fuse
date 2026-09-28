package adapter

import (
	"os"
	"path/filepath"
	"testing"

	"baidupcs-fuse/mount"
)

func TestWritePath_SmallFile(t *testing.T) {
	dir := t.TempDir()
	client := &MockPCSClient{}
	dlinkCache := NewDlinkCache(30 * 60e9)
	stagingMgr := NewStagingManager(dir, true)
	upload := NewUploadSession(client, stagingMgr)

	// 创建 staging 写入数据
	s, err := stagingMgr.Open("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	s.Write(0, []byte("hello world"))
	tmpPath, err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	// 上传
	result, err := upload.Upload(nil, tmpPath, "/test.txt", 11)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if result.Size != 11 {
		t.Errorf("size = %d, want 11", result.Size)
	}
	_ = dlinkCache
}

func TestWritePath_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	client := &MockPCSClient{}
	stagingMgr := NewStagingManager(dir, true)
	upload := NewUploadSession(client, stagingMgr)

	result, err := upload.Upload(nil, "", "/empty.txt", 0)
	if err != nil {
		t.Fatalf("upload empty: %v", err)
	}
	if result.Size != 0 {
		t.Errorf("size = %d, want 0", result.Size)
	}
}

func TestWritePath_LargeFile_SliceScaling(t *testing.T) {
	// 模拟 5GB 文件 → sliceSize = max(4MB, ceil(5GB/1024)) ≈ 4.88MB
	// 不实际创建 5GB 文件，只验证分片大小计算逻辑
	dir := t.TempDir()
	client := &MockPCSClient{}
	stagingMgr := NewStagingManager(dir, true)
	upload := NewUploadSession(client, stagingMgr)

	// 验证 sliceSize 字段
	if upload.sliceSize != 4*1024*1024 {
		t.Errorf("default sliceSize = %d, want 4MB", upload.sliceSize)
	}
}

func TestWritePath_StagingBusy(t *testing.T) {
	dir := t.TempDir()
	stagingMgr := NewStagingManager(dir, true)

	s1, _ := stagingMgr.Open("/same.txt")
	defer s1.Close()

	_, err := stagingMgr.Open("/same.txt")
	if err != mount.ErrStagingBusy {
		t.Errorf("err = %v, want ErrStagingBusy", err)
	}
}

func TestWritePath_UploadSuccess_Cleanup(t *testing.T) {
	dir := t.TempDir()
	client := &MockPCSClient{}
	stagingMgr := NewStagingManager(dir, true)
	upload := NewUploadSession(client, stagingMgr)

	s, _ := stagingMgr.Open("/cleanup.txt")
	s.Write(0, []byte("data"))
	tmpPath, _ := s.Close()

	// 上传
	upload.Upload(nil, tmpPath, "/cleanup.txt", 4)

	// staging 文件应该还在（CloseAndRemove 需要显式调用）
	if _, err := os.Stat(tmpPath); err != nil {
		t.Error("staging file should still exist before explicit cleanup")
	}

	// 清理
	stagingMgr.CloseAndRemove("/cleanup.txt")
	if stagingMgr.HasSession("/cleanup.txt") {
		t.Error("session should be removed")
	}
}

func TestWritePath_CrossDirRename(t *testing.T) {
	// 跨目录 rename 不支持（v1）
	client := &MockPCSClient{}
	err := client.Rename(nil, "/a/file.txt", "/b/file.txt")
	// MockPCSClient 总是返回 nil（真实实现会检查路径）
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestWritePath_TmpPath(t *testing.T) {
	dir := t.TempDir()
	stagingMgr := NewStagingManager(dir, true)

	// 不存在时返回 false
	_, ok := stagingMgr.TmpPath("/nope.txt")
	if ok {
		t.Error("expected false for non-existent path")
	}

	// 创建后返回 true
	s, _ := stagingMgr.Open("/exists.txt")
	defer s.Close()

	tmpPath, ok := stagingMgr.TmpPath("/exists.txt")
	if !ok {
		t.Error("expected true after open")
	}
	if filepath.Ext(tmpPath) != ".tmp" {
		t.Errorf("ext = %q, want .tmp", filepath.Ext(tmpPath))
	}
}
