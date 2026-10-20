//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package fuse

import "os"

func acquireInstanceLock(cacheDir string) (*os.File, error) { return nil, nil }
func releaseInstanceLock(f *os.File)                        {}

// AcquireInstanceLock 非 flock 平台的兜底实现。
func AcquireInstanceLock(cacheDir string) (*os.File, error) { return acquireInstanceLock(cacheDir) }

// ReleaseInstanceLock 非 flock 平台的兜底实现。
func ReleaseInstanceLock(f *os.File) { releaseInstanceLock(f) }
