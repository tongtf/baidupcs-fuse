package pcscore

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/qjfoidnh/BaiduPCS-Go/baidupcs"
	"github.com/qjfoidnh/BaiduPCS-Go/baidupcs/pcserror"
)

// Library 是 BaiduPCS-Go Core PCS 适配层的薄封装。
// 这是整个 baidupcs-fuse 唯一 import github.com/qjfoidnh/BaiduPCS-Go/* 的代码位置（ADR-0001）。
// 越过本包边界的所有类型均为本地/stdlib 类型，绝不外泄 baidupcs.* 具体类型。
type Library interface {
	Stat(ctx context.Context, path string) (*FileMeta, error)
	ReadDir(ctx context.Context, path string) ([]*FileMeta, error)
	LocateDownload(ctx context.Context, path string) (dlink string, fsID int64, size int64, mtimeSec int64, err error)
	QuotaInfo(ctx context.Context) (total, used int64, err error)
	Mkdir(ctx context.Context, path string) error
	Remove(ctx context.Context, paths ...string) error
	Rename(ctx context.Context, from, to string) error
	RapidUpload(ctx context.Context, remotePath string, size int64, contentMD5, sliceMD5, dataContent, policy, uploadID string, blockListMD5 []string) error
	PrecreateUpload(ctx context.Context, remotePath string, size int64, blockListMD5 []string) (uploadID string, err error)
	UploadSlice(ctx context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (md5 string, err error)
	CreateSuperFile(ctx context.Context, uploadID, remotePath string, size int64, checksumMap map[int]string) error
	UpdateStoken(newToken string)
}

// FileMeta 是 baidupcs.FileDirectory 的本地等价类型（Mode 由本包按 Unix 惯例推断）。
type FileMeta struct {
	FsID     int64
	Path     string
	Filename string
	Isdir    bool
	Size     int64
	Mtime    int64
	Mode     uint32
}

// New 创建 Core PCS Library 实现。appID: BaiduPCS-Go 应用 ID；bduss: BDUSS cookie。
// stoken 为 PCS API 路径（Stat/上传等）必需——缺省时多数操作返回 31030 pcs token not exist，
// 故调用 BaiduPCS.SetStoken 补上 STOKEN cookie。
// defaultHTTPTimeout 与 adapter 包一致：Core 路径裸客户端的无超时兜底。
const defaultHTTPTimeout = 60 * time.Second

func New(appID int, bduss, stoken string, httpTimeout time.Duration) Library {
	if httpTimeout <= 0 {
		httpTimeout = defaultHTTPTimeout
	}
	pcs := baidupcs.NewPCS(appID, bduss)
	if stoken != "" {
		pcs.SetStoken(stoken)
	}
	return &coreLibrary{pcs: pcs, timeout: httpTimeout}
}

type coreLibrary struct {
	pcs     *baidupcs.BaiduPCS
	timeout time.Duration
}

func (c *coreLibrary) Stat(ctx context.Context, path string) (*FileMeta, error) {
	fd, pcsErr := c.pcs.FilesDirectoriesMeta(path)
	if pcsErr != nil {
		return nil, toError(pcsErr)
	}
	return &FileMeta{
		FsID:     fd.FsID,
		Path:     fd.Path,
		Filename: fd.Filename,
		Isdir:    fd.Isdir,
		Size:     fd.Size,
		Mtime:    fd.Mtime,
		Mode:     fileMode(fd),
	}, nil
}

func (c *coreLibrary) ReadDir(ctx context.Context, path string) ([]*FileMeta, error) {
	fdl, pcsErr := c.pcs.FilesDirectoriesList(path, &baidupcs.OrderOptions{})
	if pcsErr != nil {
		return nil, toError(pcsErr)
	}

	entries := make([]*FileMeta, 0, len(fdl))
	for _, fd := range fdl {
		entries = append(entries, &FileMeta{
			FsID:     fd.FsID,
			Path:     fd.Path,
			Filename: fd.Filename,
			Isdir:    fd.Isdir,
			Size:     fd.Size,
			Mtime:    fd.Mtime,
			Mode:     fileMode(fd),
		})
	}
	return entries, nil
}

func (c *coreLibrary) LocateDownload(ctx context.Context, path string) (dlink string, fsID int64, size int64, mtimeSec int64, err error) {
	info, pcsErr := c.pcs.LocateDownload(path)
	if pcsErr != nil {
		return "", 0, 0, 0, toError(pcsErr)
	}

	if len(info.URLs) == 0 {
		return "", 0, 0, 0, fmt.Errorf("no download URLs returned")
	}

	dlink = info.URLs[0].URL
	for _, u := range info.URLs {
		if strings.HasPrefix(u.URL, "https://") {
			dlink = u.URL
			break
		}
	}

	fd, pcsErr2 := c.pcs.FilesDirectoriesMeta(path)
	if pcsErr2 != nil {
		return dlink, 0, 0, 0, toError(pcsErr2)
	}

	return dlink, fd.FsID, fd.Size, fd.Mtime, nil
}

func (c *coreLibrary) QuotaInfo(ctx context.Context) (total, used int64, err error) {
	quota, used, pcsErr := c.pcs.QuotaInfo()
	if pcsErr != nil {
		return 0, 0, toError(pcsErr)
	}
	return quota, used, nil
}

func (c *coreLibrary) Mkdir(ctx context.Context, path string) error {
	pcsErr := c.pcs.Mkdir(path)
	if pcsErr != nil {
		return toError(pcsErr)
	}
	return nil
}

func (c *coreLibrary) Remove(ctx context.Context, paths ...string) error {
	pcsErr := c.pcs.Remove(paths...)
	if pcsErr != nil {
		return toError(pcsErr)
	}
	return nil
}

func (c *coreLibrary) Rename(ctx context.Context, from, to string) error {
	pcsErr := c.pcs.Rename(from, to)
	if pcsErr != nil {
		return toError(pcsErr)
	}
	return nil
}

func (c *coreLibrary) RapidUpload(ctx context.Context, remotePath string, size int64, contentMD5, sliceMD5, dataContent, policy, uploadID string, blockListMD5 []string) error {
	pcsErr, _ := c.pcs.RapidUpload(remotePath, policy, uploadID, contentMD5, sliceMD5, dataContent, "", 0, 0, size, 0, blockListMD5)
	if pcsErr != nil {
		return toError(pcsErr)
	}
	return nil
}

func (c *coreLibrary) PrecreateUpload(ctx context.Context, remotePath string, size int64, blockListMD5 []string) (string, error) {
	pcsErr, jsonData := c.pcs.FakeRapidUpload(remotePath, "overwrite", size)
	if pcsErr != nil {
		return "", toError(pcsErr)
	}
	return jsonData.UploadID, nil
}

func (c *coreLibrary) UploadSlice(ctx context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (string, error) {
	reader := bytes.NewReader(data)
	uploadFunc := func(uploadURL string, jar http.CookieJar) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, reader)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
		client := &http.Client{Jar: jar, Timeout: c.timeout}
		return client.Do(req)
	}

	md5, pcsErr := c.pcs.UploadTmpFile(uploadID, remotePath, seq, offset, uploadFunc)
	if pcsErr != nil {
		return "", toError(pcsErr)
	}
	return md5, nil
}

func (c *coreLibrary) CreateSuperFile(ctx context.Context, uploadID, remotePath string, size int64, checksumMap map[int]string) error {
	pcsErr := c.pcs.UploadCreateSuperFile(uploadID, "overwrite", size, remotePath, checksumMap)
	if pcsErr != nil {
		return toError(pcsErr)
	}
	return nil
}

func (c *coreLibrary) UpdateStoken(newToken string) {}

// toError 将 pcserror.Error 转为 Go 标准 error。
// 百度 errno → Go error 映射（对齐 BaiduErrorToErrno）：
//
//	-6  → os.ErrPermission  (BDUSS/STOKEN 无效或过期)
//	-7  → os.ErrPermission  (权限不足)
//	-9  → os.ErrNotExist    (文件不存在)
//	-10 → os.ErrNotExist    (文件已删除)
//	-11 → os.ErrExist       (目标已存在)
func toError(pcsErr pcserror.Error) error {
	if pcsErr == nil {
		return nil
	}
	errCode := pcsErr.GetRemoteErrCode()
	errMsg := pcsErr.GetRemoteErrMsg()
	if errMsg == "" {
		errMsg = pcsErr.GetError().Error()
	}

	switch {
	case errCode == -6 || errCode == -7 || errCode == -12 || errCode == -14:
		return os.ErrPermission
	case errCode == -9 || errCode == -10:
		return os.ErrNotExist
	case errCode == -11:
		return os.ErrExist
	case strings.Contains(errMsg, "Permission"):
		return os.ErrPermission
	}

	return fmt.Errorf("pcs error %d: %s", errCode, errMsg)
}

func fileMode(fd *baidupcs.FileDirectory) uint32 {
	if fd.Isdir {
		return 0755
	}
	return 0644
}
