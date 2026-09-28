package adapter

import (
	"context"
	"errors"
	"os"
	"syscall"

	"baidupcs-fuse/download"
)

// classifyErr 将百度 API 错误/系统错误分类为结构化错误（T6.6）。
// 映射到 download 包的哨兵错误，供 FUSE 层进一步转 errno。
func classifyErr(err error) error {
	if err == nil {
		return nil
	}

	// 上下文错误
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err // FUSE 层映射为 EIO
	}

	// OS 错误透传
	if errors.Is(err, os.ErrNotExist) {
		return err
	}
	if errors.Is(err, os.ErrExist) {
		return err
	}
	if errors.Is(err, os.ErrPermission) {
		return err
	}

	// download 包错误透传
	if errors.Is(err, download.ErrCDNForbidden) {
		return err
	}
	if errors.Is(err, download.ErrRangeNotSatisfiable) {
		return err
	}
	if errors.Is(err, download.ErrShortRead) {
		return err
	}

	// syscall 错误透传
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return err
	}

	// 默认：包装为 EIO 友好格式
	return err
}
