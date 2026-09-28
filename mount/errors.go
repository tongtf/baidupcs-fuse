package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
)

var (
	// ErrVersionConflict 远端文件在 open 期间被替换（AUDIT-002）。
	ErrVersionConflict = errors.New("remote file changed during open")

	// ErrStagingBusy 同路径已有活跃写 session（v1 不支持并发写同一文件）。
	ErrStagingBusy = errors.New("concurrent write to same file")

	// ErrCrossDirRename 跨目录 rename 不支持（v1 不做非原子批量移动）。
	ErrCrossDirRename = errors.New("cross-directory rename not supported")

	// ErrShortRead 块拉取返回字节数 ≠ 请求 size（AUDIT-001）。
	ErrShortRead = errors.New("block fetch returned fewer bytes than requested")

	// ErrWriteNotEnabled 写功能未启用，需要 --enable-write。
	ErrWriteNotEnabled = errors.New("write not enabled, use --enable-write")

	// ErrNeedUpload 秒传未命中（return_type=1），需要真实上传。
	ErrNeedUpload = errors.New("rapid upload not matched, need real upload")
)

// BaiduError 百度 API 错误（errno != 0）。
type BaiduError struct {
	Errno  int    // 百度 API 返回的 errno
	Method string // API 方法名
	Msg    string // error_msg
}

func (e *BaiduError) Error() string {
	if e.Method != "" {
		return fmt.Sprintf("baidu api %s: errno=%d %s", e.Method, e.Errno, e.Msg)
	}
	return fmt.Sprintf("baidu api: errno=%d %s", e.Errno, e.Msg)
}

// BaiduErrorToErrno 将百度 API 错误或系统错误映射为 FUSE errno。
// 百度 errno → FUSE errno 对照：
//
//	-6  → EACCES  (BDUSS/STOKEN 无效或过期)
//	-7  → EACCES  (权限不足)
//	-9  → ENOENT  (文件不存在)
//	-10 → ENOENT  (文件已删除)
//	-11 → EEXIST  (目标已存在)
//	-12 → EACCES  (操作不允许)
//	-14 → EACCES  (账号被限制)
//	-20 → EACCES  (验证码过期)
//	-21 → EACCES  (文件/目录被锁定)
//	-23 → EACCES  (文件名违规)
//	-62 → EACCES  (下载次数超限)
//	-70 → EACCES  (分享违规)
//	-112 → EIO    (签名过期，仅 pan-api 路径)
//	-113 → EIO    (签名过期，仅 pan-api 路径)
func BaiduErrorToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}

	// 系统错误直接映射
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return syscall.EIO
	}
	if errors.Is(err, os.ErrNotExist) {
		return syscall.ENOENT
	}
	if errors.Is(err, os.ErrExist) {
		return syscall.EEXIST
	}
	if errors.Is(err, os.ErrPermission) {
		return syscall.EACCES
	}

	// syscall 错误透传
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}

	// 百度 API 错误码精确映射
	var bErr *BaiduError
	if errors.As(err, &bErr) {
		return baiduErrnoToSyscall(bErr.Errno)
	}

	// 默认 → EIO
	return syscall.EIO
}

// baiduErrnoToSyscall 百度 errno → syscall errno。
func baiduErrnoToSyscall(e int) syscall.Errno {
	switch {
	case e == -6 || e == -7 || e == -12 || e == -14 || e == -20 || e == -21 || e == -23 || e == -62 || e == -70:
		return syscall.EACCES
	case e == -9 || e == -10:
		return syscall.ENOENT
	case e == -11:
		return syscall.EEXIST
	case e == -112 || e == -113:
		return syscall.EIO // 签名过期
	default:
		return syscall.EIO
	}
}
