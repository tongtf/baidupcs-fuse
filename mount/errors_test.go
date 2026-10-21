package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
)

// 目的：Error() 输出同时包含 API 方法与 errno，便于日志定位。
func TestBaiduError_ErrorMessage(t *testing.T) {
	tests := []struct {
		name string
		err  *BaiduError
		want string
	}{
		{
			name: "with method",
			err:  &BaiduError{Errno: -9, Method: "list", Msg: "not found"},
			want: "baidu api list: errno=-9 not found",
		},
		{
			name: "without method",
			err:  &BaiduError{Errno: -6, Msg: "bad token"},
			want: "baidu api: errno=-6 bad token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

// 目的：哨兵错误到 POSIX errno 的稳定映射（取消/超时/版本冲突/只读/关闭）。
func TestBaiduErrorToErrno_SentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{"nil", nil, 0},
		{"canceled", fmt.Errorf("wrap: %w", context.Canceled), syscall.EINTR},
		{"deadline", context.DeadlineExceeded, syscall.ETIMEDOUT},
		{"version conflict", ErrVersionConflict, syscall.ESTALE},
		{"staging busy", ErrStagingBusy, syscall.EBUSY},
		{"write not enabled", ErrWriteNotEnabled, syscall.EROFS},
		{"shutting down", ErrShuttingDown, syscall.EIO},
		{"short read", ErrShortRead, syscall.EIO},
		{"not exist", os.ErrNotExist, syscall.ENOENT},
		{"exist", os.ErrExist, syscall.EEXIST},
		{"permission", os.ErrPermission, syscall.EACCES},
		{"unknown", errors.New("boom"), syscall.EIO},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BaiduErrorToErrno(tt.err); got != tt.want {
				t.Errorf("BaiduErrorToErrno(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// 目的：syscall.Errno 无论直接还是包裹后都必须原样透传，避免被误映射成 EIO。
func TestBaiduErrorToErrno_SyscallPassthrough(t *testing.T) {
	for _, want := range []syscall.Errno{syscall.ENOSPC, syscall.EBUSY, syscall.ENOTDIR} {
		// 直接传 syscall.Errno 与包裹后传都应透传。
		if got := BaiduErrorToErrno(want); got != want {
			t.Errorf("direct %v: got %v", want, got)
		}
		if got := BaiduErrorToErrno(fmt.Errorf("op: %w", want)); got != want {
			t.Errorf("wrapped %v: got %v", want, got)
		}
	}
}

// 目的：缺少具体 errno 时按错误消息识别 ENOTEMPTY/ENOSPC。
func TestBaiduErrorToErrno_MessageHints(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want syscall.Errno
	}{
		{"english not empty", "directory is not empty", syscall.ENOTEMPTY},
		{"chinese not empty", "目录不为空", syscall.ENOTEMPTY},
		{"chinese not empty 2", "目录非空", syscall.ENOTEMPTY},
		{"english space", "not enough space", syscall.ENOSPC},
		{"english quota", "quota exceeded", syscall.ENOSPC},
		{"chinese space", "空间不足", syscall.ENOSPC},
		{"chinese quota", "配额已满", syscall.ENOSPC},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &BaiduError{Errno: -1, Msg: tt.msg}
			if got := BaiduErrorToErrno(err); got != tt.want {
				t.Errorf("msg %q => %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}

// 目的：百度 errno 到 POSIX errno 的完整映射，未知值安全回退 EIO。
func TestBaiduErrorToErrno_ErrnoMapping(t *testing.T) {
	tests := []struct {
		errno int
		want  syscall.Errno
	}{
		{-6, syscall.EACCES},
		{-7, syscall.EACCES},
		{-12, syscall.EACCES},
		{-14, syscall.EACCES},
		{-20, syscall.EACCES},
		{-21, syscall.EACCES},
		{-23, syscall.EACCES},
		{-62, syscall.EACCES},
		{-70, syscall.EACCES},
		{-9, syscall.ENOENT},
		{-10, syscall.ENOENT},
		{-8, syscall.EEXIST},
		{-11, syscall.EEXIST},
		{-112, syscall.EIO},
		{-113, syscall.EIO},
		{-999, syscall.EIO},
		{0, syscall.EIO},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("errno%d", tt.errno), func(t *testing.T) {
			err := &BaiduError{Errno: tt.errno, Msg: "plain"}
			got := BaiduErrorToErrno(err)
			if got != tt.want {
				t.Errorf("errno %d => %v, want %v", tt.errno, got, tt.want)
			}
		})
	}
}
