package adapter

import (
	"context"
	"errors"
	"os"
	"testing"
)

// countingClient 注入 UploadSlice 错误并统计调用次数。
type countingClient struct {
	*MockPCSClient
	calls int
	err   error
}

func (c *countingClient) UploadSlice(ctx context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (string, error) {
	c.calls++
	return "mock-md5", c.err
}

// newUploadSessionWithFailingRapid 构造 RapidUpload 失败（逼进分片上传）的会话。
func newUploadSessionWithFailingRapid(t *testing.T, client PCSClient) *UploadSession {
	t.Helper()
	mgr := NewStagingManager(t.TempDir(), true)
	sess := NewUploadSession(client, mgr)
	sess.SetSliceSize(4 << 20) // 4MB，使小文件单片（确定性单次 UploadSlice）
	return sess
}

// makeStagingFile 创建一个临时 staging 文件并返回路径。
func makeStagingFile(t *testing.T, size int64) string {
	t.Helper()
	dir := t.TempDir()
	p := dir + "/zz_staging.bin"
	if err := os.WriteFile(p, make([]byte, size), 0644); err != nil {
		t.Fatalf("write staging: %v", err)
	}
	return p
}

// TestUpload_Failure_RemovesProgress：上传失败后清除进度标记——失败上传不再挂在
// status 里显示“活跃/卡死”（这是此前卡死假象的直接来源）。设 maxRetries=0 避免重试退避拖慢测试。
func TestUpload_Failure_RemovesProgress(t *testing.T) {
	base := &MockPCSClient{rapidErr: errors.New("rapid fail")}
	cc := &countingClient{MockPCSClient: base, err: errors.New("boom")}
	sess := newUploadSessionWithFailingRapid(t, cc)
	sess.maxRetries = 0

	stagingPath := makeStagingFile(t, 1024)
	if _, err := sess.Upload(context.Background(), stagingPath, "/z/fail.bin", 1024); err == nil {
		t.Fatal("expected upload to fail")
	}
	if cc.calls == 0 {
		t.Fatal("expected UploadSlice to be invoked")
	}
	if _, statErr := os.Stat(progressPath(sess.stagingMgr.cacheDir, "/z/fail.bin")); !os.IsNotExist(statErr) {
		t.Errorf("progress.json should be removed after failure, stat err = %v", statErr)
	}
}

// TestUpload_Success_RemovesProgress：成功上传后进度标记被清除。
func TestUpload_Success_RemovesProgress(t *testing.T) {
	base := &MockPCSClient{rapidErr: errors.New("rapid fail")}
	cc := &countingClient{MockPCSClient: base, err: nil}
	sess := newUploadSessionWithFailingRapid(t, cc)

	stagingPath := makeStagingFile(t, 1024)
	if _, err := sess.Upload(context.Background(), stagingPath, "/z/ok.bin", 1024); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.calls == 0 {
		t.Fatal("expected UploadSlice to be invoked")
	}
	if _, statErr := os.Stat(progressPath(sess.stagingMgr.cacheDir, "/z/ok.bin")); !os.IsNotExist(statErr) {
		t.Errorf("progress.json should be removed after success, stat err = %v", statErr)
	}
}
