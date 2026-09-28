package adapter

import (
	"context"
	"time"

	"baidupcs-fuse/adapter/pcscore"
	"baidupcs-fuse/mount"
)

// pcsClientAdapter 将 pcscore.Library 适配为 PCSClient 接口。
// 仅依赖本地类型，绝不 import baidupcs.*（ADR-0001）。所有 BaiduPCS-Go 具体类型均在 adapter/pcscore 边界内消费。
type pcsClientAdapter struct {
	lib pcscore.Library
}

// NewPCSClient 创建 Core-PCS 适配器。
// appID: BaiduPCS-Go 应用 ID（266719，Core PCS API 鉴权默认值）；bduss + stoken：PCS API 路径需两者，stoken 缺省会返回 31030。
func NewPCSClient(appID int, bduss, stoken string, httpTimeout time.Duration) PCSClient {
	return &pcsClientAdapter{lib: pcscore.New(appID, bduss, stoken, httpTimeout)}
}

func (a *pcsClientAdapter) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	fd, err := a.lib.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	return &mount.RemoteEntry{
		FSID:     fd.FsID,
		Path:     fd.Path,
		Name:     fd.Filename,
		IsDir:    fd.Isdir,
		Size:     fd.Size,
		MtimeSec: fd.Mtime,
		Mode:     fd.Mode,
	}, nil
}

func (a *pcsClientAdapter) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	fdl, err := a.lib.ReadDir(ctx, path)
	if err != nil {
		return nil, err
	}

	entries := make([]*mount.RemoteEntry, 0, len(fdl))
	for _, fd := range fdl {
		entries = append(entries, &mount.RemoteEntry{
			FSID:     fd.FsID,
			Path:     fd.Path,
			Name:     fd.Filename,
			IsDir:    fd.Isdir,
			Size:     fd.Size,
			MtimeSec: fd.Mtime,
			Mode:     fd.Mode,
		})
	}
	return entries, nil
}

func (a *pcsClientAdapter) LocateDownload(ctx context.Context, path string) (dlink string, fsID int64, size int64, mtimeSec int64, err error) {
	return a.lib.LocateDownload(ctx, path)
}

func (a *pcsClientAdapter) QuotaInfo(ctx context.Context) (total, used int64, err error) {
	return a.lib.QuotaInfo(ctx)
}

func (a *pcsClientAdapter) Mkdir(ctx context.Context, path string) error {
	return a.lib.Mkdir(ctx, path)
}

func (a *pcsClientAdapter) Remove(ctx context.Context, paths ...string) error {
	return a.lib.Remove(ctx, paths...)
}

func (a *pcsClientAdapter) Rename(ctx context.Context, from, to string) error {
	return a.lib.Rename(ctx, from, to)
}

func (a *pcsClientAdapter) RapidUpload(ctx context.Context, remotePath string, size int64, contentMD5, sliceMD5, dataContent, policy, uploadID string, blockListMD5 []string) error {
	return a.lib.RapidUpload(ctx, remotePath, size, contentMD5, sliceMD5, dataContent, policy, uploadID, blockListMD5)
}

func (a *pcsClientAdapter) PrecreateUpload(ctx context.Context, remotePath string, size int64, blockListMD5 []string) (string, error) {
	return a.lib.PrecreateUpload(ctx, remotePath, size, blockListMD5)
}

func (a *pcsClientAdapter) UploadSlice(ctx context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (string, error) {
	return a.lib.UploadSlice(ctx, uploadID, remotePath, seq, offset, data)
}

func (a *pcsClientAdapter) CreateSuperFile(ctx context.Context, uploadID, remotePath string, size int64, checksumMap map[int]string) error {
	return a.lib.CreateSuperFile(ctx, uploadID, remotePath, size, checksumMap)
}

// UpdateStoken pcsClientAdapter 不使用 STOKEN，no-op。
func (a *pcsClientAdapter) UpdateStoken(newToken string) {}
