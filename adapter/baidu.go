package adapter

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"baidupcs-fuse/mount"
)

// ============================================================
// 编译期接口检查：确保实现匹配接口签名。
// ============================================================
var (
	_ mount.CloudFS     = (*BaiduCloudFS)(nil)
	_ mount.RemoteWriter = (*baiduRemoteWriter)(nil)
	_ PCSClient         = (*panClient)(nil)
)

// PCSClient BaiduPCS-Go Core 操作的接口抽象（T6.1）。
// 唯一允许 import baidupcs.* 的适配层（ADR-0001 隔离）。
type PCSClient interface {
	Stat(ctx context.Context, path string) (*mount.RemoteEntry, error)
	ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error)
	LocateDownload(ctx context.Context, path string) (dlink string, fsID int64, size int64, mtimeSec int64, err error)
	QuotaInfo(ctx context.Context) (total, used int64, err error)
	Mkdir(ctx context.Context, path string) error
	Remove(ctx context.Context, paths ...string) error
	Rename(ctx context.Context, from, to string) error

	// 写路径（Phase 5）
	RapidUpload(ctx context.Context, remotePath string, size int64, contentMD5, sliceMD5, dataContent, policy, uploadID string, blockListMD5 []string) error
	PrecreateUpload(ctx context.Context, remotePath string, size int64, blockListMD5 []string) (uploadID string, err error)
	UploadSlice(ctx context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (md5 string, err error)
	CreateSuperFile(ctx context.Context, uploadID, remotePath string, size int64, checksumMap map[int]string) error

	// STOKEN 管理
	UpdateStoken(newToken string)
}

// DlinkCache dlink 缓存，per-fs_id，TTL=30min（ADR-0003）。
// 并发安全（v1 单线程读写均安全，防未来扩展）。
type DlinkCache struct {
	mu         sync.RWMutex
	dlinks     map[int64]*dlinkEntry
	ttl        time.Duration
	maxEntries int // 最大缓存条目数（0=不限）
}

type dlinkEntry struct {
	dlink    string
	path     string    // 原始文件路径（用于按路径失效）
	expireAt time.Time
}

func NewDlinkCache(ttl time.Duration) *DlinkCache {
	return &DlinkCache{dlinks: make(map[int64]*dlinkEntry), ttl: ttl}
}

// TTL 返回 dlink 缓存 TTL。
func (c *DlinkCache) TTL() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ttl
}

func (c *DlinkCache) Get(fsID int64) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.dlinks[fsID]
	if !ok || time.Now().After(entry.expireAt) {
		return "", false
	}
	return entry.dlink, true
}

func (c *DlinkCache) Set(fsID int64, dlink string, path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 容量淘汰：超过上限时清除所有过期条目
	if c.maxEntries > 0 && len(c.dlinks) >= c.maxEntries {
		now := time.Now()
		for k, e := range c.dlinks {
			if now.After(e.expireAt) {
				delete(c.dlinks, k)
			}
		}
	}
	c.dlinks[fsID] = &dlinkEntry{dlink: dlink, path: path, expireAt: time.Now().Add(c.ttl)}
}

func (c *DlinkCache) Invalidate(fsID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.dlinks, fsID)
}

// InvalidateByPath 按路径失效 dlink（rename/上传后精确失效）。
func (c *DlinkCache) InvalidateByPath(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for fsID, e := range c.dlinks {
		if e.path == path {
			delete(c.dlinks, fsID)
		}
	}
}

// InvalidateAll 清空所有 dlink 缓存（finish_upload 后调用）。
func (c *DlinkCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dlinks = make(map[int64]*dlinkEntry)
}

// BaiduCloudFS mount.CloudFS 的百度网盘实现（T6.1）。
type BaiduCloudFS struct {
	client         PCSClient
	dlinkCache     *DlinkCache
	accountKey     string
	stagingMgr     *StagingManager
	uploadSess     *UploadSession
	mountCtx       context.Context // 挂载生命周期 ctx：取消即终止所有后台上传
	fetcher        FetchFunc // Scheduler.FetchRange 注入（v1 读路径）
	metaInvalidate func(path string)             // FUSE 层注入，finish_upload 后失效 metadata 缓存
	statCache      func(path string) (*mount.RemoteEntry, bool) // FUSE 层注入，OpenRead 首次 Stat 走缓存
	blockInvalidate func(accountID string, fsID int64) // FUSE 层注入，upload 成功后失效 BlockCache
	stats          *TransferStats // 全局传输统计
}

func NewBaiduCloudFS(client PCSClient, dlinkCache *DlinkCache, accountKey string) *BaiduCloudFS {
	return &BaiduCloudFS{client: client, dlinkCache: dlinkCache, accountKey: accountKey}
}

// SetWriteSupport 注入 staging + upload 会话，启用写路径（Phase 5）。
func (fs *BaiduCloudFS) SetWriteSupport(stagingMgr *StagingManager, uploadSess *UploadSession) {
	fs.stagingMgr = stagingMgr
	fs.uploadSess = uploadSess
}

// SetMountContext 注入挂载生命周期 ctx：卸载/退出时该 ctx 被取消，终止所有后台上传。
func (fs *BaiduCloudFS) SetMountContext(ctx context.Context) {
	fs.mountCtx = ctx
}

// SetMetaInvalidate 注入 metadata 缓存失效回调（finish_upload 后调用）。
func (fs *BaiduCloudFS) SetMetaInvalidate(fn func(path string)) {
	fs.metaInvalidate = fn
}

// SetStatCache 注入 metadata 缓存查询回调（OpenRead 首次 Stat 走缓存）。
func (fs *BaiduCloudFS) SetStatCache(fn func(path string) (*mount.RemoteEntry, bool)) {
	fs.statCache = fn
}

// SetFetcher 注入 Scheduler.FetchRange，使 OpenRead 返回的 reader 能真实读取网络数据。
func (fs *BaiduCloudFS) SetFetcher(fn FetchFunc) {
	fs.fetcher = fn
}

// SetBlockInvalidate 注入 BlockCache 失效回调（upload 成功后调用）。
func (fs *BaiduCloudFS) SetBlockInvalidate(fn func(accountID string, fsID int64)) {
	fs.blockInvalidate = fn
}

// SetTransferStats 注入全局传输统计。
func (fs *BaiduCloudFS) SetTransferStats(stats *TransferStats) {
	fs.stats = stats
}

// Stats 返回全局传输统计（可能为 nil）。
func (fs *BaiduCloudFS) Stats() *TransferStats {
	return fs.stats
}

// Stat → PCSClient.Stat（T6.4）。
func (fs *BaiduCloudFS) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	entry, err := fs.client.Stat(ctx, path)
	if err != nil {
		return nil, classifyErr(err)
	}
	return entry, nil
}

// ReadDir → PCSClient.ReadDir（T6.4）。
func (fs *BaiduCloudFS) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	entries, err := fs.client.ReadDir(ctx, path)
	if err != nil {
		return nil, classifyErr(err)
	}
	return entries, nil
}

// StatFS → PCSClient.QuotaInfo（T6.5）。
func (fs *BaiduCloudFS) StatFS(ctx context.Context) (*mount.FSStats, error) {
	total, used, err := fs.client.QuotaInfo(ctx)
	if err != nil {
		return nil, classifyErr(err)
	}
	return &mount.FSStats{Total: total, Used: used, Free: total - used}, nil
}

// Mkdir（T6.4）。
func (fs *BaiduCloudFS) Mkdir(ctx context.Context, path string) error {
	err := fs.client.Mkdir(ctx, path)
	if err != nil {
		return classifyErr(err)
	}
	return nil
}

// Remove（T6.4）。
func (fs *BaiduCloudFS) Remove(ctx context.Context, paths ...string) error {
	err := fs.client.Remove(ctx, paths...)
	if err != nil {
		return classifyErr(err)
	}
	return nil
}

// Rename（T6.4）。
func (fs *BaiduCloudFS) Rename(ctx context.Context, from, to string) error {
	err := fs.client.Rename(ctx, from, to)
	if err != nil {
		return classifyErr(err)
	}
	return nil
}

// CreateWriter Phase 5: staging + precreate/superfile2/create。
func (fs *BaiduCloudFS) CreateWriter(ctx context.Context, path string) (mount.RemoteWriter, error) {
	if fs.stagingMgr == nil || fs.uploadSess == nil {
		return nil, mount.ErrWriteNotEnabled
	}
	session, err := fs.stagingMgr.Open(path)
	if err != nil {
		return nil, err
	}
	// 构建 blockInvalidateAll 回调（闭包捕获 accountKey）
	var blockInvalidateAll func()
	if fs.blockInvalidate != nil {
		ak := fs.accountKey
		blockInvalidateAll = func() {
			// 无法精确知道新 fsID，清除该 accountKey 下所有块缓存
			// TODO: 优化为按 fsID 精确清除（需要 finish_upload 返回新 fsID）
			fs.blockInvalidate(ak, -1) // -1 表示全清
		}
	}
	return &baiduRemoteWriter{
		session:    session,
		uploadSess: fs.uploadSess,
		path:       path,
		stagingMgr: fs.stagingMgr,
		cloudFS:    fs,
		deps: writerDeps{
			dlinkCache:         fs.dlinkCache,
			blockInvalidateAll: blockInvalidateAll,
			metaCache:          &metaInvalidateAdapter{fn: fs.metaInvalidate},
		},
	}, nil
}

// GetDlinkDownload 使用 method=download 获取下载链接（fallback for locatedownload 403）。
func (fs *BaiduCloudFS) GetDlinkDownload(ctx context.Context, path string) (string, error) {
	pcs, ok := fs.client.(*panClient)
	if !ok {
		return "", fmt.Errorf("client is not panClient")
	}
	return pcs.getDlinkDownload(ctx, path)
}

// writerDeps baiduRemoteWriter 的可选依赖集合（减少结构体字段数）。
type writerDeps struct {
	dlinkCache         *DlinkCache
	blockInvalidateAll func()
	metaCache          interface{ Invalidate(path string) }
}

// baiduRemoteWriter 基于 staging 的 RemoteWriter 实现。
type baiduRemoteWriter struct {
	session    *writeSession
	uploadSess *UploadSession
	path       string
	stagingMgr *StagingManager
	cloudFS    mount.CloudFS      // 用于 backfillGap 时按需获取 reader
	reader     mount.RemoteReader // 用于写洞回填（覆盖写已有文件时注入）
	deps       writerDeps         // 可选依赖（缓存失效等）

	commitMu sync.Mutex // 保护提交状态：Flush/Sync/Close 任一首次 dirty 触发后台上传，后续调用立即返回
	commits  int        // 未完成的后台上传计数（含正在执行与已排程等待）
}

func (w *baiduRemoteWriter) SetReader(r mount.RemoteReader) {
	w.reader = r
}

// metaInvalidateAdapter 适配 func 到接口，避免循环依赖。
type metaInvalidateAdapter struct {
	fn func(path string)
}

func (a *metaInvalidateAdapter) Invalidate(path string) {
	if a.fn != nil {
		a.fn(path)
	}
}

func (w *baiduRemoteWriter) ReadFrom(_ context.Context, off int64, size int) ([]byte, error) {
	return w.session.ReadFrom(off, size)
}

func (w *baiduRemoteWriter) Dirty() bool {
	w.session.mu.Lock()
	defer w.session.mu.Unlock()
	return w.session.dirty
}
func (w *baiduRemoteWriter) ClearDirty() {
	w.session.mu.Lock()
	defer w.session.mu.Unlock()
	w.session.dirty = false
}
func (w *baiduRemoteWriter) Size() int64 { return w.session.Size() }

func (w *baiduRemoteWriter) Truncate(size int64) error {
	return w.session.Truncate(size)
}

func (w *baiduRemoteWriter) Reset() error {
	return w.session.Reset()
}

// backfillGap 从远端旧文件回填 [stagingLen, off) 的数据到 staging 文件。
// 仅在 off > stagingLen 时调用（写洞场景）。
func (w *baiduRemoteWriter) backfillGap(ctx context.Context, off int64) error {
	if w.reader == nil {
		// reader 未注入（Mknod/Create 新文件路径）→ 按需从远端获取
		if w.cloudFS == nil {
			return nil // 无 cloudFS，无法回填
		}
		r, err := w.cloudFS.OpenRead(ctx, w.path)
		if err != nil {
			// 文件在远端不存在（真正的新文件）→ 无旧数据可回填
			return nil
		}
		w.reader = r
	}
	fileSize := w.reader.Size()
	if fileSize == 0 {
		return nil
	}
	stagingLen := w.session.Size()
	if off <= stagingLen {
		return nil
	}
	// 回填范围：[stagingLen, min(off, fileSize))
	end := off
	if end > fileSize {
		end = fileSize
	}
	gap := end - stagingLen
	if gap <= 0 {
		return nil
	}

	// 分块回填（16MB/chunk）
	const chunkSize = 16 * 1024 * 1024
	pos := stagingLen
	for pos < end {
		toRead := end - pos
		if toRead > chunkSize {
			toRead = chunkSize
		}
		data, err := w.reader.ReadAt(ctx, pos, int(toRead))
		if err != nil {
			return fmt.Errorf("backfill read at %d: %w", pos, err)
		}
		if err := w.session.Write(pos, data); err != nil {
			return fmt.Errorf("backfill write at %d: %w", pos, err)
		}
		pos += int64(len(data))
	}
	return nil
}

func (w *baiduRemoteWriter) Write(ctx context.Context, off int64, data []byte) error {
	// 写洞检测：off 超出当前 staging 长度 → 先回填
	if off > w.session.Size() {
		if err := w.backfillGap(ctx, off); err != nil {
			return err
		}
	}
	return w.session.Write(off, data)
}

func (w *baiduRemoteWriter) Close(ctx context.Context) error {
	// 关闭 reader（如果有的话）
	if w.reader != nil {
		w.reader.Close()
		w.reader = nil
	}

	// 后台提交上传：首次 dirty 触发，后续调用（Flush/Sync/Close）一律立即返回。
	// 网络传输在后台 goroutine 中完成，不阻塞 FUSE handler——避免 stalled 网络调用冻住整条挂载。
	w.startCommit()
	return nil
}

// startCommit 原子地确保只启动一次后台提交：
//   - 已有后台提交在跑 → 立即返回（Flush/Sync/Close 任一触发后，其余均为空操作）。
//   - 无脏数据 → 同步清理 staging，立即返回。
//   - 有脏数据且尚未提交 → 排程后台 goroutine 执行网络上传，立即返回。
func (w *baiduRemoteWriter) startCommit() {
	w.commitMu.Lock()
	defer w.commitMu.Unlock()

	if w.commits > 0 {
		return
	}
	if !w.session.dirty {
		w.stagingMgr.CloseAndRemove(w.path)
		return
	}

	w.commits++
	go w.runCommit()
}

// runCommit 在后台 goroutine 中完成 staging 收尾与网络上传。
// 成功：清理 staging + 失效缓存；失败：保留磁盘文件并记录到 failed-uploads manifest，供 mount status / mount recover 使用。
func (w *baiduRemoteWriter) runCommit() {
	defer func() {
		w.commitMu.Lock()
		w.commits--
		w.commitMu.Unlock()
	}()

	stagingPath, err := w.session.Close()
	if err != nil {
		log.Printf("baiduRemoteWriter %s: close staging failed: %v", w.path, err)
		w.stagingMgr.CloseAndRemove(w.path)
		return
	}

	info, err := os.Stat(stagingPath)
	if err != nil {
		log.Printf("baiduRemoteWriter %s: stat staging failed: %v", w.path, err)
		w.stagingMgr.CloseAndRemove(w.path)
		return
	}

	size := info.Size()
	var uploadCtx context.Context
	if bfs, ok := w.cloudFS.(*BaiduCloudFS); ok {
		uploadCtx = bfs.mountCtx
	}
	if uploadCtx == nil {
		uploadCtx = context.Background()
	}
	commitCtx, cancelUpload := context.WithCancel(uploadCtx)
	defer cancelUpload()

	if _, err = w.uploadSess.Upload(commitCtx, stagingPath, w.path, size); err != nil {
		// 上传失败：保留磁盘文件（记录到 manifest）供 mount status / recover 使用；
		// ClearSession 从会话表移除以免阻塞该路径的后续写入，ClearDirty 防止同 handle 再次触发上传。
		// 每次打开生命周期仅一次上传机会，失败由 mount recover 显式重试。
		log.Printf("baiduRemoteWriter %s: async upload failed (保留 staging，可用 mount recover 重传): %v", w.path, err)
		SaveFailedUpload(w.stagingMgr.CacheDir(), FailedUploadEntry{
			RemotePath: w.path,
			LocalPath:  stagingPath,
			Size:       size,
			Error:      err.Error(),
		})
		w.stagingMgr.ClearSession(w.path)
		w.session.ClearDirty()
		return
	}

	// 成功：清理 staging + 失效远端缓存（dlink + metadata + block）
	w.stagingMgr.CloseAndRemove(w.path)
	w.invalidateCaches()
}

// Sync 后台提交当前脏数据到远端但不清除 writer（POSIX fsync 语义）。
// 与 Close 共用同一提交守卫：首次触发即排程后台上传，后续调用立即返回。
func (w *baiduRemoteWriter) Sync(ctx context.Context) error {
	// 后台提交：不阻塞 FUSE handler（stalled 网络调用不再冻住整条挂载）。
	w.startCommit()
	return nil
}

// invalidateCaches 完成上传后失效远端缓存（dlink + metadata + block）。
func (w *baiduRemoteWriter) invalidateCaches() {
	if w.deps.dlinkCache != nil {
		// 按路径精确失效（上传后旧 dlink 已过期）
		w.deps.dlinkCache.InvalidateByPath(w.path)
	}
	if w.deps.metaCache != nil {
		// 失效目标文件的 metadata
		w.deps.metaCache.Invalidate(w.path)
		// 失效父目录（新文件需要刷新目录列表）
		parent := w.path
		if i := len(parent) - 1; i > 0 {
			for i > 0 && parent[i] != '/' {
				i--
			}
			parent = parent[:i]
		}
		if parent == "" {
			parent = "/"
		}
		w.deps.metaCache.Invalidate(parent)
	}
	// BlockCache 失效：upload 后旧块可能与新版本不一致
	// fsID 需要从远端重新获取，这里用全清作为安全兜底
	if w.deps.blockInvalidateAll != nil {
		w.deps.blockInvalidateAll()
	}
}
