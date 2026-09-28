package mount

import "context"

// CloudFS 云端文件系统接口 — FUSE/Cache/Scheduler 只依赖此接口（ADR-0001）。
// 实现见 adapter/baidu.go（唯一允许 import baidupcs.* 的文件）。
type CloudFS interface {
	Stat(ctx context.Context, path string) (*RemoteEntry, error)
	ReadDir(ctx context.Context, path string) ([]*RemoteEntry, error)

	// OpenRead 内部 3 步消除竞态：①Stat pin → ②LocateDownload → ③再 Stat 比对（AUDIT-002）。
	OpenRead(ctx context.Context, path string) (RemoteReader, error)

	StatFS(ctx context.Context) (*FSStats, error) // → pcs.QuotaInfo

	Mkdir(ctx context.Context, path string) error
	Remove(ctx context.Context, paths ...string) error
	Rename(ctx context.Context, from, to string) error

	CreateWriter(ctx context.Context, path string) (RemoteWriter, error) // Phase 5
}
