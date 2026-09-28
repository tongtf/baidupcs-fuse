package mount

import "context"

// RemoteReader 一次 open = 一个被 pin 的版本快照 + dlink（带 TTL）。
// AUDIT-001: ReadAt 返回 []byte，长度必须 == size，否则内部已返回 EIO。
type RemoteReader interface {
	// ReadAt 返回 [off, off+size) 的完整数据。
	// 返回的 []byte 长度必须 == size，否则内部已返回 EIO。
	// go-fuse/v2 的 Read handler 直接将返回的 []byte 复制到 fuse.ReadResult。
	ReadAt(ctx context.Context, off int64, size int) ([]byte, error)

	// Size 返回 pinned size（open 时刻锁定），不是实时值。
	Size() int64

	// FSID 返回 pinned fs_id（BlockCache key 用）。
	FSID() int64

	// Mtime 返回 pinned mtime（BlockCache key 用）。
	Mtime() int64

	// RefreshDlink 使用 method=download 获取新 dlink（locatedownload 签名失败的 fallback）。
	RefreshDlink(ctx context.Context, path string) error

	// Close 释放 pinned version 和 dlink 引用。v1 简单实现可为空。
	Close() error
}

// RemoteWriter Phase 5: staging + precreate/superfile2/create。
type RemoteWriter interface {
	Write(ctx context.Context, off int64, data []byte) error
	Close(ctx context.Context) error
	// Sync 将当前脏数据上传到远端但不清除 writer（对齐 POSIX fsync）。
	// 成功后内部自动清除 dirty 标记。
	Sync(ctx context.Context) error
	// Truncate 调整目标大小（对齐 POSIX truncate）。
	Truncate(size int64) error
	// ReadFrom 从 staging 文件读取 [off, off+size)（对齐 read_session）。
	ReadFrom(ctx context.Context, off int64, size int) ([]byte, error)
	// Size 返回当前 staging 文件大小。
	Size() int64
	// Reset 截断 staging 文件到 0（O_TRUNC 场景）。
	Reset() error
	// Dirty reports whether there are uncommitted writes.
	Dirty() bool

	// SetReader 注入远端读取器，用于写洞回填（backfill）。
	SetReader(r RemoteReader)
}

// RemoteEntry 远端文件/目录元信息。
type RemoteEntry struct {
	FSID     int64  // fs_id（稳定远程标识）
	Path     string // 规范化网盘路径
	Name     string // server_filename
	IsDir    bool   // 是否目录
	Size     int64  // 文件大小（目录=0）
	MtimeSec int64  // 修改时间（秒级 Unix 时间戳）
	Mode     uint32 // Unix 权限位（文件 0644 / 目录 0755）
}

// FSStats statfs 返回的文件系统统计信息。
type FSStats struct {
	Total int64 // 总容量（字节）
	Used  int64 // 已用容量（字节）
	Free  int64 // 可用容量（字节）
}
