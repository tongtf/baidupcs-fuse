package fuse

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
)

// 目的：Flush/Fsync 对非写句柄、空 writer、干净 writer 必须 no-op；
// 脏数据同步失败必须返回映射后的 errno，成功则清零 dirty。
func TestFlushFsync_Branches(t *testing.T) {
	ctx := context.Background()
	node := &fileNode{fs: &baiduFSContext{}, path: "/f"}

	if errno := node.Flush(ctx, nil); errno != 0 {
		t.Fatalf("Flush nil errno=%v", errno)
	}
	if errno := node.Flush(ctx, &readFileHandle{node: node}); errno != 0 {
		t.Fatalf("Flush read handle errno=%v", errno)
	}
	if errno := node.Flush(ctx, &writeFileHandle{node: node}); errno != 0 {
		t.Fatalf("Flush nil writer errno=%v", errno)
	}
	if errno := node.Flush(ctx, &writeFileHandle{node: node, writer: &memWriter{}}); errno != 0 {
		t.Fatalf("Flush clean writer errno=%v", errno)
	}

	ok := &memWriter{dirty: true}
	if errno := node.Flush(ctx, &writeFileHandle{node: node, writer: ok}); errno != 0 {
		t.Fatalf("Flush dirty errno=%v", errno)
	}
	if ok.dirty {
		t.Fatal("successful flush did not clear dirty")
	}

	bad := &memWriter{dirty: true, syncErr: os.ErrPermission}
	if errno := node.Flush(ctx, &writeFileHandle{node: node, writer: bad}); errno != syscall.EACCES {
		t.Fatalf("Flush error errno=%v", errno)
	}
	if !bad.dirty {
		t.Fatal("failed flush must keep dirty")
	}

	if errno := node.Fsync(ctx, nil, 0); errno != 0 {
		t.Fatalf("Fsync nil errno=%v", errno)
	}
	if errno := node.Fsync(ctx, &readFileHandle{node: node}, 0); errno != 0 {
		t.Fatalf("Fsync read handle errno=%v", errno)
	}
	if errno := node.Fsync(ctx, &writeFileHandle{node: node}, 0); errno != 0 {
		t.Fatalf("Fsync nil writer errno=%v", errno)
	}
	if errno := node.Fsync(ctx, &writeFileHandle{node: node, writer: &memWriter{syncErr: errors.New("boom")}}, 0); errno != syscall.EIO {
		t.Fatalf("Fsync error errno=%v", errno)
	}
	if errno := node.Fsync(ctx, &writeFileHandle{node: node, writer: &memWriter{}}, 0); errno != 0 {
		t.Fatalf("Fsync success errno=%v", errno)
	}
}

// 目的：isSubpath 只把 parent 内部路径视为子路径，根目录与同路径不能误判，
// 避免 rename 把目录移动到自身/把 /ab 当作 /a 的子路径。
func TestIsSubpath_Boundaries(t *testing.T) {
	for _, tc := range []struct {
		child, parent string
		want          bool
	}{
		{"/a", "/", true},
		{"/", "/", false},
		{"/a/b", "/a", true},
		{"/a", "/a", false},
		{"/ab", "/a", false},
		{"/b", "/a", false},
	} {
		if got := isSubpath(tc.child, tc.parent); got != tc.want {
			t.Fatalf("isSubpath(%q,%q)=%v want %v", tc.child, tc.parent, got, tc.want)
		}
	}
}

// 目的：盘缓存刷盘 wrapper 必须立即返回可停止的清理函数，默认 5min 周期不应在测试中触发。
func TestStartDiskFlush_WrapperStops(t *testing.T) {
	cloud := newHandlerFS()
	_, ctx := newHandlerFSContext(t, cloud)
	stop := ctx.startDiskFlush()
	stop()
	// 再调用一次不应 panic/阻塞（Mount 退出路径可能重复清理）。
	stop()
}

// 目的：Mount 在实例锁被占用时必须在创建任何后台组件前失败，不能共享缓存目录。
func TestMount_InstanceLockHeld(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	opts.MountPoint = t.TempDir()
	lock, err := AcquireInstanceLock(opts.CacheDir)
	if err != nil {
		t.Fatal(err)
	}
	defer ReleaseInstanceLock(lock)

	if _, _, err := Mount(opts, newMockFS()); err == nil {
		t.Fatal("mount with held lock should fail")
	}
}
