//go:build darwin || linux || freebsd || netbsd || openbsd

package fuse

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// acquireInstanceLock 对 cacheDir 加进程级 flock，防止同一缓存目录被两个挂载实例共享
// （会导致 staging、manifest、socket 互相覆盖）。
func acquireInstanceLock(cacheDir string) (*os.File, error) {
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, err
	}
	_ = os.Chmod(cacheDir, 0700)
	path := filepath.Join(cacheDir, "instance.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("cache directory %s is already used by another bdfs instance: %w", cacheDir, err)
	}
	_ = f.Truncate(0)
	_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
	_ = f.Sync()
	return f, nil
}

func releaseInstanceLock(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}

// AcquireInstanceLock 供 CLI 子命令（如 recover）独占缓存目录。
// 挂载进程持有同一把锁；recover 在挂载运行期间必须拒绝执行。
func AcquireInstanceLock(cacheDir string) (*os.File, error) {
	return acquireInstanceLock(cacheDir)
}

// ReleaseInstanceLock 释放 AcquireInstanceLock 获取的锁。
func ReleaseInstanceLock(f *os.File) { releaseInstanceLock(f) }
