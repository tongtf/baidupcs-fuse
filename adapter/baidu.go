package adapter

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"baidupcs-fuse/mount"
)

// ============================================================
// 编译期接口检查：确保实现匹配接口签名。
// ============================================================
var (
	_ mount.CloudFS      = (*BaiduCloudFS)(nil)
	_ mount.RemoteWriter = (*baiduRemoteWriter)(nil)
	_ PCSClient          = (*panClient)(nil)
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

// DlinkCache dlink 缓存，按 (fs_id,size,mtime) 版本化，TTL=30min（ADR-0003）。
// 不能只按 fs_id：同一文件被改写后 fs_id 可能不变，旧 dlink 会读到旧版本内容。
// 并发安全。
type DlinkCache struct {
	mu         sync.RWMutex
	dlinks     map[dlinkKey]*dlinkEntry
	ttl        time.Duration
	maxEntries int // 最大缓存条目数（0=不限）
}

type dlinkKey struct {
	fsID  int64
	size  int64
	mtime int64
}

type dlinkEntry struct {
	dlink    string
	path     string // 原始文件路径（用于按路径失效）
	expireAt time.Time
}

func NewDlinkCache(ttl time.Duration) *DlinkCache {
	return &DlinkCache{dlinks: make(map[dlinkKey]*dlinkEntry), ttl: ttl, maxEntries: 2048}
}

// TTL 返回 dlink 缓存 TTL。
func (c *DlinkCache) TTL() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ttl
}

// Get 查询 dlink。version 可选：传入 size,mtime 时只命中同一版本；
// 不传时退化为旧测试/兼容语义（size=mtime=0）。
func (c *DlinkCache) Get(fsID int64, version ...int64) (string, bool) {
	key := dlinkKey{fsID: fsID}
	if len(version) >= 2 {
		key.size, key.mtime = version[0], version[1]
	}
	c.mu.RLock()
	entry, ok := c.dlinks[key]
	if !ok {
		c.mu.RUnlock()
		return "", false
	}
	if time.Now().After(entry.expireAt) {
		c.mu.RUnlock()
		c.mu.Lock()
		// 重新检查：RUnlock 到 Lock 之间可能已有并发 Set 写入新值。
		if cur, ok := c.dlinks[key]; ok && time.Now().After(cur.expireAt) {
			delete(c.dlinks, key)
		}
		c.mu.Unlock()
		return "", false
	}
	dlink := entry.dlink
	c.mu.RUnlock()
	return dlink, true
}

func (c *DlinkCache) Set(fsID int64, dlink string, path string, version ...int64) {
	key := dlinkKey{fsID: fsID}
	if len(version) >= 2 {
		key.size, key.mtime = version[0], version[1]
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// 容量淘汰：先清过期，再淘汰最早到期的条目。
	if c.maxEntries > 0 && len(c.dlinks) >= c.maxEntries {
		now := time.Now()
		for k, e := range c.dlinks {
			if now.After(e.expireAt) {
				delete(c.dlinks, k)
			}
		}
		for len(c.dlinks) >= c.maxEntries {
			var oldestKey dlinkKey
			var oldest time.Time
			first := true
			for k, e := range c.dlinks {
				if first || e.expireAt.Before(oldest) {
					oldestKey, oldest, first = k, e.expireAt, false
				}
			}
			if first {
				break
			}
			delete(c.dlinks, oldestKey)
		}
	}
	c.dlinks[key] = &dlinkEntry{dlink: dlink, path: path, expireAt: time.Now().Add(c.ttl)}
}

func (c *DlinkCache) Invalidate(fsID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.dlinks {
		if k.fsID == fsID {
			delete(c.dlinks, k)
		}
	}
}

// InvalidateByPath 按路径失效 dlink（rename/上传后精确失效）。
func (c *DlinkCache) InvalidateByPath(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.dlinks {
		if e.path == path {
			delete(c.dlinks, k)
		}
	}
}

// BaiduCloudFS mount.CloudFS 的百度网盘实现（T6.1）。
type BaiduCloudFS struct {
	client          PCSClient
	dlinkCache      *DlinkCache
	accountKey      string
	stagingMgr      *StagingManager
	uploadSess      *UploadSession
	mountCtx        context.Context                              // 挂载生命周期 ctx：取消即终止所有后台上传
	fetcher         FetchFunc                                    // Scheduler.FetchRange 注入（v1 读路径）
	metaInvalidate  func(path string)                            // FUSE 层注入，finish_upload 后失效 metadata 缓存
	statCache       func(path string) (*mount.RemoteEntry, bool) // FUSE 层注入，OpenRead 首次 Stat 走缓存
	blockInvalidate func(fsID int64)                             // FUSE 层注入，上传后清该 fsID 的块缓存
	stats           *TransferStats                               // 全局传输统计

	// blockEpoch 按 fsID 递增：commit/truncate 后旧 reader 即使继续读，
	// 也不允许把旧版本的块回填进新缓存。
	blockEpochMu sync.Mutex
	blockEpoch   map[int64]uint64

	commitMu      sync.Mutex
	commitCount   int
	commitIdle    chan struct{} // count==0 时关闭并置 nil；count>0 时用于等待 drain
	stopping      atomic.Bool
	asyncClose    bool
	commitTimeout time.Duration
}

func NewBaiduCloudFS(client PCSClient, dlinkCache *DlinkCache, accountKey string) *BaiduCloudFS {
	return &BaiduCloudFS{client: client, dlinkCache: dlinkCache, accountKey: accountKey, blockEpoch: make(map[int64]uint64)}
}

// BumpBlockEpoch 在文件内容变化后递增版本；旧 reader 的 Put 会被拒绝。
func (fs *BaiduCloudFS) BumpBlockEpoch(fsID int64) {
	if fsID <= 0 {
		return
	}
	fs.blockEpochMu.Lock()
	if fs.blockEpoch == nil {
		fs.blockEpoch = make(map[int64]uint64)
	}
	fs.blockEpoch[fsID]++
	fs.blockEpochMu.Unlock()
}

// BlockEpoch 返回当前块缓存版本。
func (fs *BaiduCloudFS) BlockEpoch(fsID int64) uint64 {
	fs.blockEpochMu.Lock()
	defer fs.blockEpochMu.Unlock()
	return fs.blockEpoch[fsID]
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

// SetAsyncClose 设置 Release 是否异步提交（默认 false，遵循 ADR-0004 的 sync-on-close）。
func (fs *BaiduCloudFS) SetAsyncClose(async bool) { fs.asyncClose = async }

// SetCommitTimeout 设置单次同步提交的最长等待时间（0=不额外限制）。
func (fs *BaiduCloudFS) SetCommitTimeout(d time.Duration) { fs.commitTimeout = d }

// BeginShutdown 停止接收新的写入提交，供卸载时 drain。
func (fs *BaiduCloudFS) BeginShutdown() { fs.stopping.Store(true) }

// WaitCommits 等待在途上传结束，最长等待 ctx 超时。
// 不使用 WaitGroup：Add 可能与 Wait 并发（Release 与卸载同时发生），
// 用计数 + idle channel 避免 “Add called concurrently with Wait” 的运行时 panic。
func (fs *BaiduCloudFS) WaitCommits(ctx context.Context) error {
	for {
		fs.commitMu.Lock()
		if fs.commitCount == 0 {
			fs.commitMu.Unlock()
			return nil
		}
		idle := fs.commitIdle
		fs.commitMu.Unlock()
		select {
		case <-idle:
			// 计数可能在 idle 关闭后又变为非 0（已有 writer 再次 Sync/Close），重新检查。
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (fs *BaiduCloudFS) beginCommit() {
	fs.commitMu.Lock()
	if fs.commitCount == 0 {
		fs.commitIdle = make(chan struct{})
	}
	fs.commitCount++
	fs.commitMu.Unlock()
}

func (fs *BaiduCloudFS) endCommit() {
	fs.commitMu.Lock()
	fs.commitCount--
	if fs.commitCount == 0 && fs.commitIdle != nil {
		close(fs.commitIdle)
		fs.commitIdle = nil
	}
	fs.commitMu.Unlock()
}

// SetBlockInvalidate 注入块缓存失效回调（上传/截断后清 fsID 旧块）。
func (fs *BaiduCloudFS) SetBlockInvalidate(fn func(fsID int64)) {
	fs.blockInvalidate = fn
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
	if fs.stopping.Load() {
		return nil, mount.ErrShuttingDown
	}
	if fs.stagingMgr == nil || fs.uploadSess == nil {
		return nil, mount.ErrWriteNotEnabled
	}
	session, err := fs.stagingMgr.Open(path)
	if err != nil {
		return nil, err
	}
	return &baiduRemoteWriter{
		session:       session,
		uploadSess:    fs.uploadSess,
		path:          path,
		stagingMgr:    fs.stagingMgr,
		cloudFS:       fs,
		asyncClose:    fs.asyncClose,
		commitTimeout: fs.commitTimeout,
		deps: writerDeps{
			dlinkCache:      fs.dlinkCache,
			metaCache:       &metaInvalidateAdapter{fn: fs.metaInvalidate},
			blockInvalidate: fs.blockInvalidate,
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
	dlinkCache      *DlinkCache
	metaCache       interface{ Invalidate(path string) }
	blockInvalidate func(fsID int64)
}

// baiduRemoteWriter 基于 staging 的 RemoteWriter 实现。
//
// 状态机：open → committing → open（Sync）或 closed（Close）。
// opMu 串行化 Write/Truncate/Sync/Close：commit 期间冻结写入，避免上传半个快照。
type baiduRemoteWriter struct {
	session    *writeSession
	uploadSess *UploadSession
	path       string
	stagingMgr *StagingManager
	cloudFS    mount.CloudFS
	reader     mount.RemoteReader // 用于尾部/写洞回填（覆盖写已有文件时注入）
	deps       writerDeps
	// pathFn 在 commit 时动态解析远端路径：打开中的文件被 rename 后，
	// 必须提交到新路径；若 inode 已被 unlink/孤立，返回 linked=false 丢弃数据。
	pathFn func() (string, bool)

	opMu          sync.Mutex
	targetSize    int64 // 逻辑目标大小（上传大小）
	closed        bool
	asyncClose    bool
	commitTimeout time.Duration
}

func (w *baiduRemoteWriter) SetReader(r mount.RemoteReader) {
	w.opMu.Lock()
	w.reader = r
	w.opMu.Unlock()
}

// SetPathProvider 注入 commit 时的动态远端路径解析器。
// linked=false 表示 inode 已从目录树移除（unlink/父目录删除），按 POSIX
// 语义丢弃未提交数据，禁止在原路径重建文件。
func (w *baiduRemoteWriter) SetPathProvider(fn func() (string, bool)) {
	w.opMu.Lock()
	w.pathFn = fn
	w.opMu.Unlock()
}

// remotePathLocked 返回本次提交的目标路径。调用方必须持有 opMu。
func (w *baiduRemoteWriter) remotePathLocked() (string, bool) {
	if w.pathFn != nil {
		if p, linked := w.pathFn(); p != "" {
			return p, linked
		}
	}
	return w.path, true
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
	w.opMu.Lock()
	defer w.opMu.Unlock()
	return w.session.ReadFrom(off, size)
}

func (w *baiduRemoteWriter) Dirty() bool {
	return w.session.Dirty()
}

func (w *baiduRemoteWriter) Size() int64 { return w.session.Size() }

func (w *baiduRemoteWriter) TargetSize() int64 {
	w.opMu.Lock()
	defer w.opMu.Unlock()
	return w.targetSize
}

func (w *baiduRemoteWriter) SetTargetSize(size int64) {
	w.opMu.Lock()
	defer w.opMu.Unlock()
	if size >= 0 {
		w.targetSize = size
	}
}

func (w *baiduRemoteWriter) MarkDirty() { w.session.MarkDirty() }

// ensureGapLocked 把 staging 扩展到 off，缺口优先用旧远端数据回填，超出旧文件的部分补零。
func (w *baiduRemoteWriter) ensureGapLocked(ctx context.Context, off int64) error {
	staging := w.session.Size()
	if off <= staging {
		return nil
	}
	if w.reader != nil {
		oldSize := w.reader.Size()
		end := off
		if end > oldSize {
			end = oldSize
		}
		// truncate 缩小后，目标大小之后的内容在 POSIX 语义下已经不存在；
		// 重新写大洞时必须补零，不能从旧远端版本回填已截掉的数据。
		if end > w.targetSize {
			end = w.targetSize
		}
		const chunk = 16 * 1024 * 1024
		for pos := staging; pos < end; {
			n := end - pos
			if n > chunk {
				n = chunk
			}
			data, err := w.reader.ReadAt(ctx, pos, int(n))
			if err != nil {
				return fmt.Errorf("backfill read at %d: %w", pos, err)
			}
			if len(data) != int(n) {
				return fmt.Errorf("backfill short read at %d: %d != %d", pos, len(data), n)
			}
			if err := w.session.Write(pos, data); err != nil {
				return fmt.Errorf("backfill write at %d: %w", pos, err)
			}
			pos += n
		}
	}
	staging = w.session.Size()
	if staging < off {
		// 新文件/超出旧文件范围的洞按 POSIX 语义补零。
		if err := w.session.Truncate(off); err != nil {
			return fmt.Errorf("zero-fill staging to %d: %w", off, err)
		}
	}
	return nil
}

func (w *baiduRemoteWriter) Write(ctx context.Context, off int64, data []byte) error {
	w.opMu.Lock()
	defer w.opMu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	if len(data) == 0 {
		return nil // POSIX pwrite(0 字节) 不得扩展文件或制造写洞
	}
	if off > w.session.Size() {
		if err := w.ensureGapLocked(ctx, off); err != nil {
			return err
		}
	}
	if err := w.session.Write(off, data); err != nil {
		return err
	}
	if end := off + int64(len(data)); end > w.targetSize {
		w.targetSize = end
	}
	return nil
}

func (w *baiduRemoteWriter) Truncate(size int64) error {
	w.opMu.Lock()
	defer w.opMu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	if size < 0 {
		return fmt.Errorf("truncate to negative size %d", size)
	}
	// 增大目标时不要立刻用 0 扩展 staging：旧远端尾部必须由 ensureTailLocked
	// 在 commit 时回填，超过旧文件的部分才补零。否则 truncate -s +N 会把
	// 原有内容全部替换成 0。
	if size <= w.session.Size() || w.reader == nil {
		if err := w.session.Truncate(size); err != nil {
			return err
		}
	}
	w.targetSize = size
	w.session.MarkDirty()
	return nil
}

func (w *baiduRemoteWriter) Reset() error {
	w.opMu.Lock()
	defer w.opMu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	if err := w.session.Reset(); err != nil {
		return err
	}
	w.targetSize = 0
	w.session.MarkDirty()
	return nil
}

// ensureTailLocked 把 [stagingLen, target) 从旧远端回填；超出旧文件的部分补零。
func (w *baiduRemoteWriter) ensureTailLocked(ctx context.Context) error {
	staging := w.session.Size()
	if staging >= w.targetSize {
		return nil
	}
	if w.reader == nil {
		if w.targetSize > 0 {
			return fmt.Errorf("cannot preserve remote tail [%d,%d): reader not available", staging, w.targetSize)
		}
		return nil
	}
	oldSize := w.reader.Size()
	end := w.targetSize
	if end > oldSize {
		end = oldSize
	}
	const chunk = 16 * 1024 * 1024
	for pos := staging; pos < end; {
		n := end - pos
		if n > chunk {
			n = chunk
		}
		data, err := w.reader.ReadAt(ctx, pos, int(n))
		if err != nil {
			return fmt.Errorf("preserve tail read at %d: %w", pos, err)
		}
		if len(data) != int(n) {
			return fmt.Errorf("preserve tail short read at %d: %d != %d", pos, len(data), n)
		}
		if err := w.session.Write(pos, data); err != nil {
			return fmt.Errorf("preserve tail write at %d: %w", pos, err)
		}
		pos += n
	}
	if w.session.Size() < w.targetSize {
		if err := w.session.Truncate(w.targetSize); err != nil {
			return fmt.Errorf("zero-fill tail to %d: %w", w.targetSize, err)
		}
	}
	return nil
}

func (w *baiduRemoteWriter) commitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	if w.commitTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, w.commitTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	if bfs, ok := w.cloudFS.(*BaiduCloudFS); ok && bfs.mountCtx != nil {
		go func() {
			select {
			case <-bfs.mountCtx.Done():
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancel
}

// syncLocked 同步提交当前快照。成功清 dirty 但保留 staging（允许后续继续写）；
// 失败保留 staging 并写入失败清单，由上层返回真实错误。
func (w *baiduRemoteWriter) syncLocked(ctx context.Context, track bool) error {
	if !w.session.Dirty() {
		return nil
	}
	// 从回填/fsync 阶段就纳入 drain 计数，不能只覆盖 Upload：
	// 否则 WaitCommits 可能在 Release 仍在回填旧尾部时提前返回。
	if track {
		endTrack := w.beginTrack(true)
		defer endTrack()
	}
	remotePath, linked := w.remotePathLocked()
	if !linked {
		// 文件已被 unlink（或父目录被删除）：close 应丢弃未提交内容，
		// 而不是在原路径重新创建它。
		w.session.ClearDirty()
		w.invalidateCachesAt(remotePath)
		return nil
	}
	if staging := w.session.Size(); staging > w.targetSize {
		w.targetSize = staging
	}
	target := w.targetSize
	if err := w.ensureTailLocked(ctx); err != nil {
		w.recordFailure(target, err)
		return err
	}
	if err := w.session.Sync(); err != nil {
		err = fmt.Errorf("fsync staging: %w", err)
		w.recordFailure(target, err)
		return err
	}
	commitCtx, cancel := w.commitContext(ctx)
	defer cancel()

	if _, err := w.uploadSess.Upload(commitCtx, w.session.Path(), remotePath, target); err != nil {
		w.recordFailure(target, err)
		return err
	}
	w.session.ClearDirty()
	if w.stagingMgr != nil {
		// 成功的提交覆盖了此前的失败记录，避免 recover 重传旧 staging。
		// rename 后路径已变化，旧路径的失败记录也要清理。
		for _, p := range uniquePaths(w.path, remotePath) {
			if rerr := RemoveFailedUpload(w.stagingMgr.CacheDir(), p); rerr != nil {
				log.Printf("baiduRemoteWriter %s: 清理失败清单失败: %v", p, rerr)
			}
		}
	}
	w.invalidateCachesAt(remotePath)
	return nil
}

// recordFailure 把失败提交写入 manifest，供 bdfs recover 重传。
// 记录的目标大小是逻辑大小；recover 必须校验本地 staging 长度，避免上传半截文件。
func (w *baiduRemoteWriter) recordFailure(target int64, err error) {
	if w.stagingMgr == nil {
		return
	}
	remotePath, _ := w.remotePathLocked()
	if remotePath != w.path {
		// rename 后旧路径的记录已失效，避免 recover 把内容传回旧名字。
		if rerr := RemoveFailedUpload(w.stagingMgr.CacheDir(), w.path); rerr != nil {
			log.Printf("baiduRemoteWriter %s: 清理旧失败清单失败: %v", w.path, rerr)
		}
	}
	if ferr := SaveFailedUpload(w.stagingMgr.CacheDir(), FailedUploadEntry{
		RemotePath: remotePath,
		LocalPath:  w.session.Path(),
		Size:       target,
		Error:      err.Error(),
	}); ferr != nil {
		log.Printf("baiduRemoteWriter %s: 保存失败清单失败: %v", remotePath, ferr)
	}
}

// beginTrack 在真正开始 commit 前注册到 commit 计数器，保证 WaitCommits 不漏掉
// “已从 Release 返回、但 goroutine 尚未执行”的异步提交。
func (w *baiduRemoteWriter) beginTrack(track bool) func() {
	if !track {
		return func() {}
	}
	bfs, ok := w.cloudFS.(*BaiduCloudFS)
	if !ok {
		return func() {}
	}
	bfs.beginCommit()
	return bfs.endCommit
}

// Close 默认同步提交（sync-on-close）；asyncClose=true 时后台提交。
func (w *baiduRemoteWriter) Close(ctx context.Context) error {
	w.opMu.Lock()
	if w.closed {
		w.opMu.Unlock()
		return nil
	}
	if w.asyncClose && w.session.Dirty() {
		endTrack := w.beginTrack(true)
		w.closed = true
		w.opMu.Unlock()
		go func() {
			defer endTrack()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("baiduRemoteWriter %s: async commit panic: %v", w.path, r)
				}
			}()
			w.opMu.Lock()
			defer w.opMu.Unlock()
			commitCtx, cancel := w.commitContext(context.Background())
			defer cancel()
			if err := w.syncLocked(commitCtx, false); err != nil {
				log.Printf("baiduRemoteWriter %s: async upload failed (保留 staging，可用 mount recover 重传): %v", w.path, err)
				w.failLocked()
				return
			}
			w.stagingMgr.CloseAndRemove(w.path)
			w.closeReaderLocked()
		}()
		return nil
	}

	err := w.finishLocked(ctx, true)
	w.closed = true
	w.opMu.Unlock()
	return err
}

func (w *baiduRemoteWriter) finishLocked(ctx context.Context, track bool) error {
	if err := w.syncLocked(ctx, track); err != nil {
		w.failLocked()
		return err
	}
	w.stagingMgr.CloseAndRemove(w.path)
	w.closeReaderLocked()
	return nil
}

// failLocked 保留 staging 文件供 recover，同时释放 session 占位与 reader。
func (w *baiduRemoteWriter) failLocked() {
	_, _ = w.session.Close()
	w.stagingMgr.ClearSession(w.path)
	w.closeReaderLocked()
}

func (w *baiduRemoteWriter) closeReaderLocked() {
	if w.reader != nil {
		_ = w.reader.Close()
		w.reader = nil
	}
}

// Sync 同步提交当前脏数据但保持 writer 可用（POSIX fsync 语义）。
func (w *baiduRemoteWriter) Sync(ctx context.Context) error {
	w.opMu.Lock()
	defer w.opMu.Unlock()
	if w.closed {
		return nil
	}
	return w.syncLocked(ctx, true)
}

// invalidateCachesAt 完成上传后失效 dlink + metadata + 该 fsID 的块缓存。
// 不能只依赖 fsID+size+mtime：百度 mtime 不保证变化，同尺寸覆盖会命中旧块。
func (w *baiduRemoteWriter) invalidateCachesAt(remotePath string) {
	if remotePath == "" {
		remotePath = w.path
	}
	for _, p := range uniquePaths(w.path, remotePath) {
		if w.deps.dlinkCache != nil {
			w.deps.dlinkCache.InvalidateByPath(p)
		}
		if w.deps.metaCache != nil {
			w.deps.metaCache.Invalidate(p)
			w.deps.metaCache.Invalidate(parentPath(p))
		}
	}
	if w.deps.blockInvalidate != nil && w.reader != nil && w.reader.FSID() > 0 {
		w.deps.blockInvalidate(w.reader.FSID())
	}
}

func uniquePaths(paths ...string) []string {
	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// parentPath 返回远端路径的父目录（根目录仍返回 "/"）。
func parentPath(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	i := len(p) - 1
	for i >= 0 && p[i] == '/' {
		i--
	}
	for i >= 0 && p[i] != '/' {
		i--
	}
	if i <= 0 {
		return "/"
	}
	return p[:i]
}
