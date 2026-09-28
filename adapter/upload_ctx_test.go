package adapter

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// blockingUploadClient 让 UploadSlice 阻塞直到 ctx 取消后返回 ctx.Err(),
// 用于验证上传路径对生命周期 ctx 取消会“立即中止”而非拖到超时。
type blockingUploadClient struct {
	*MockPCSClient
	called chan struct{} // 容量1：非信号一次“已进入 UploadSlice”
}

func (c *blockingUploadClient) UploadSlice(ctx context.Context, _, _ string, _ int, _ int64, _ []byte) (string, error) {
	select {
	case c.called <- struct{}{}: // 标记已进入（非阻塞）
	default:
	}
	<-ctx.Done()
	return "", ctx.Err()
}

// TestUpload_AbortsPromptlyWhenContextCancelled：父 ctx 被取消时，上传应在 UploadSlice 立即返回
//（对应 pcscore NewRequestWithContext + baidu.go runCommit/Upload 透传 ctx），不得拖到 curl 超时。
// 同时确认失败后进度标记已被清除（与 TestUpload_Failure_RemovesProgress 同契约）。
func TestUpload_AbortsPromptlyWhenContextCancelled(t *testing.T) {
	base := &MockPCSClient{rapidErr: errors.New("rapid fail")}
	cc := &blockingUploadClient{MockPCSClient: base, called: make(chan struct{}, 1)}
	sess := newUploadSessionWithFailingRapid(t, cc)

	stagingPath := makeStagingFile(t, 1024) // 单片，确定性单次 UploadSlice

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := sess.Upload(ctx, stagingPath, "/z/cancel.bin", 1024)
		done <- err
	}()

	// 等 UploadSlice 真正进入阻塞，再取消 ctx
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("UploadSlice 未在预期时间内被调用")
	case <-cc.called:
	}
	cancel()

	// 取消后应在有界时间内返回（而非拖到 curl timeout）
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want wrapping context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Upload 在取消后未在有界时间内返回")
	}

	// 中止也应清除进度标记
	if _, statErr := os.Stat(progressPath(sess.stagingMgr.cacheDir, "/z/cancel.bin")); !os.IsNotExist(statErr) {
		t.Errorf("progress.json should be removed after aborted upload, stat err = %v", statErr)
	}
}

// ctxCancelReader 实现 mount.RemoteReader：ctx 取消时 ReadAt 立即返回 ctx.Err()。
type ctxCancelReader struct{ size int64 }

func (r *ctxCancelReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return make([]byte, size), nil
}
func (r *ctxCancelReader) Size() int64                  { return r.size }
func (r *ctxCancelReader) FSID() int64                  { return 1 }
func (r *ctxCancelReader) Mtime() int64                 { return 1 }
func (r *ctxCancelReader) RefreshDlink(context.Context, string) error { return nil }
func (r *ctxCancelReader) Close() error                 { return nil }

// TestBackfillGap_PropagatesContextError：覆盖写已有文件产生写洞时，backfillGap 使用调用方 ctx；
// 当该 ctx 在卸载/退出过程中被取消（Write → backfillGap → ReadAt），回填应立即以 ctx.Err() 失败，
// 而非阻塞或吞掉错误。直接走公开 Write() 入口验证整条调用链。
func TestBackfillGap_PropagatesContextError(t *testing.T) {
	mgr := NewStagingManager(t.TempDir(), true)
	s, err := mgr.Open("/gap.bin")
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 预取消：模拟卸载过程中回填被取消

	w := &baiduRemoteWriter{
		session:    s,
		stagingMgr: mgr,
		reader:     &ctxCancelReader{size: 100},
	}

	if err := w.Write(ctx, 50, nil); err == nil {
		t.Fatal("预期 Write(backfillGap) 在取消的 ctx 下失败")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want wrapping context.Canceled", err)
	}
}
