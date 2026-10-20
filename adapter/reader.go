package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"baidupcs-fuse/download"
	"baidupcs-fuse/mount"
)

// refreshInFlight 追踪正在刷新的 (account, fsID)，防止多实例同时刷新同一文件。
// 必须带 accountKey：不同账户的 fsID 会重复。
var refreshInFlight sync.Map

type refreshKey struct {
	account string
	fsID    int64
	path    string
}

// ============================================================
// 编译期接口检查
// ============================================================
var _ mount.RemoteReader = (*remoteReader)(nil)

// FetchFunc Scheduler.FetchRange 的函数签名（T6.7）。
type FetchFunc func(ctx context.Context, fsID int64, dlink string, off, length int64) ([]byte, error)

// remoteReader mount.RemoteReader 的百度网盘实现。
// 一次 OpenRead = 一个被 pin 的版本快照 + dlink（带 TTL）。
//
// 并发约定：
//   - dlink 由 dlinkMu 保护，后台刷新与 ReadAt 可并发；
//   - Close 幂等（closeOnce），关闭后 ReadAt 立即返回 os.ErrClosed；
//   - Reload 在 403/416 时由 ReadAt 内部调用，保证不会复用坏链接。
type remoteReader struct {
	fsID    int64
	size    int64
	mtime   int64 // pinned mtime（BlockCache key 用）
	fetcher FetchFunc
	bfs     *BaiduCloudFS // 用于 Reload / 统计
	path    string        // 文件路径
	// cacheEpoch 是 open 时 fsID 的块缓存版本；commit 后旧 reader 不得回填缓存。
	cacheEpoch uint64

	dlinkMu  sync.RWMutex
	dlink    string
	reloadMu sync.Mutex // 合并同 reader 的并发 403 刷新

	stopCh    chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	statsOnce sync.Once
}

// CacheEpoch 实现 FUSE 层的可选版本检查接口。
func (r *remoteReader) CacheEpoch() uint64 { return r.cacheEpoch }

func (r *remoteReader) currentDlink() string {
	r.dlinkMu.RLock()
	defer r.dlinkMu.RUnlock()
	return r.dlink
}

func (r *remoteReader) setDlink(dlink string) {
	r.dlinkMu.Lock()
	r.dlink = dlink
	r.dlinkMu.Unlock()
}

func (r *remoteReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	if r.closed.Load() {
		return nil, os.ErrClosed
	}
	if off < 0 || size < 0 || off > r.size || int64(size) > r.size-off {
		return nil, fmt.Errorf("read range [%d, %d) exceeds file size %d", off, off+int64(size), r.size)
	}

	badDlink := r.currentDlink()
	data, err := r.fetcher(ctx, r.fsID, badDlink, off, int64(size))
	if err != nil && isDlinkExpired(err) {
		// 第一现场失效 dlink，刷新后只重试一次；并发 403 由 reloadMu 合并。
		if rerr := r.reloadDlink(ctx, badDlink); rerr != nil {
			return nil, rerr
		}
		data, err = r.fetcher(ctx, r.fsID, r.currentDlink(), off, int64(size))
	}
	if err != nil {
		return nil, err
	}
	if len(data) != size {
		return nil, download.ErrShortRead
	}
	return data, nil
}

func isDlinkExpired(err error) bool {
	return errors.Is(err, download.ErrCDNForbidden) || errors.Is(err, download.ErrRangeNotSatisfiable)
}

func (r *remoteReader) Size() int64  { return r.size }
func (r *remoteReader) FSID() int64  { return r.fsID }
func (r *remoteReader) Mtime() int64 { return r.mtime }

// Reload 失效并重新获取 dlink，同时校验 pinned 版本没有变化。
func (r *remoteReader) Reload(ctx context.Context) error {
	return r.reloadDlink(ctx, "")
}

// reloadDlink 合并并发刷新：如果 badDlink 已经被其它 goroutine 替换，直接复用。
func (r *remoteReader) reloadDlink(ctx context.Context, badDlink string) error {
	if r.bfs == nil {
		return fmt.Errorf("Reload: BaiduCloudFS not available")
	}
	if r.closed.Load() {
		return os.ErrClosed
	}
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	if badDlink != "" && r.currentDlink() != badDlink {
		return nil
	}
	dlink, err := r.bfs.reloadDlink(ctx, r.path, r.fsID, r.size, r.mtime)
	if err != nil {
		return err
	}
	r.setDlink(dlink)
	return nil
}

func (r *remoteReader) Close() error {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		close(r.stopCh)
		if r.bfs != nil && r.bfs.stats != nil {
			r.statsOnce.Do(func() {
				r.bfs.stats.EndDownload(r.path)
			})
		}
	})
	return nil
}

// reloadDlink 失效 dlink 缓存 → 重新取链 → 校验 pinned 版本。
// 供 remoteReader.Reload 与 OpenRead 的 403 自愈共用。
func (fs *BaiduCloudFS) reloadDlink(ctx context.Context, path string, fsID, size, mtime int64) (string, error) {
	if fs.dlinkCache != nil {
		fs.dlinkCache.Invalidate(fsID)
		fs.dlinkCache.InvalidateByPath(path)
	}

	// 取链前确认远端版本没有变化：变了就干净失败，绝不把新旧数据混在一个句柄里。
	cur, err := fs.client.Stat(ctx, path)
	if err != nil {
		return "", classifyErr(err)
	}
	if cur == nil {
		return "", fmt.Errorf("reloadDlink %s: stat returned nil entry", path)
	}
	if cur.FSID != fsID || cur.Size != size || cur.MtimeSec != mtime {
		return "", mount.ErrVersionConflict
	}

	// 403/416 现场优先走 locatedownload：method=download 在链接失效时仍会返回
	// 一个能构造出来的 URL，单纯重试同一个 URL 无法自愈。locatedownload 的
	// 返回带有版本元数据，必须再核对 pinned 版本，避免把新版本链接用于旧句柄。
	dlink, gotFSID, gotSize, gotMtime, locateErr := fs.client.LocateDownload(ctx, path)
	if locateErr == nil {
		if gotFSID != fsID || gotSize != size || gotMtime != mtime {
			if fs.metaInvalidate != nil {
				fs.metaInvalidate(path)
			}
			return "", mount.ErrVersionConflict
		}
	} else {
		log.Printf("reloadDlink %s: locatedownload failed (%v), trying method=download", path, locateErr)
		dlink, err = fs.GetDlinkDownload(ctx, path)
		if err != nil {
			return "", classifyErr(err)
		}
		// method=download 不带版本元数据：取链后再 Stat 一次，避免并发改写后
		// 把新版本链接交给 pinned 旧句柄。
		check, cerr := fs.client.Stat(ctx, path)
		if cerr != nil {
			return "", classifyErr(cerr)
		}
		if check == nil || check.FSID != fsID || check.Size != size || check.MtimeSec != mtime {
			if fs.metaInvalidate != nil {
				fs.metaInvalidate(path)
			}
			return "", mount.ErrVersionConflict
		}
	}
	if fs.dlinkCache != nil {
		fs.dlinkCache.Set(fsID, dlink, path, size, mtime)
	}
	return dlink, nil
}

// resolveDlink 解析下载 URL：命中 dlinkCache 直接复用，miss 时走 method=download，
// 失败 fallback locatedownload，结果写入缓存供下次 OpenRead 复用。
func (fs *BaiduCloudFS) resolveDlink(ctx context.Context, path string, fsID, size, mtime int64) (string, error) {
	if fs.dlinkCache != nil {
		if cached, ok := fs.dlinkCache.Get(fsID, size, mtime); ok && cached != "" {
			return cached, nil
		}
	}
	dlink, err := fs.GetDlinkDownload(ctx, path)
	if err != nil {
		log.Printf("OpenRead %s: method=download failed (%v), trying locatedownload", path, err)
		gotDlink, gotFSID, gotSize, gotMtime, lerr := fs.client.LocateDownload(ctx, path)
		if lerr != nil {
			return "", classifyErr(lerr)
		}
		if gotFSID != fsID || gotSize != size || gotMtime != mtime {
			return "", mount.ErrVersionConflict
		}
		dlink = gotDlink
	}
	if fs.dlinkCache != nil {
		fs.dlinkCache.Set(fsID, dlink, path, size, mtime)
	}
	return dlink, nil
}

// OpenRead 版本 pin（T6.2 + AUDIT-002）：
// ①Stat pin (FSID,Size,MtimeSec) — 优先从 metadata cache 取
// ②method=download 获取 CDN URL（优先，实测可靠），失败 fallback locatedownload
// ③再 Stat 比对（与取链并行）
func (fs *BaiduCloudFS) OpenRead(ctx context.Context, path string) (mount.RemoteReader, error) {
	var entry *mount.RemoteEntry
	if fs.statCache != nil {
		if e, ok := fs.statCache(path); ok {
			entry = e
		}
	}
	if entry == nil {
		var err error
		entry, err = fs.client.Stat(ctx, path)
		if err != nil {
			return nil, classifyErr(err)
		}
	}
	if entry == nil {
		return nil, fmt.Errorf("OpenRead %s: stat returned nil entry", path)
	}
	pinned := *entry

	type statResult struct {
		entry *mount.RemoteEntry
		err   error
	}
	type dlinkResult struct {
		dlink string
		err   error
	}
	verifyCh := make(chan statResult, 1)
	dlinkCh := make(chan dlinkResult, 1)

	go func() {
		e, err := fs.client.Stat(ctx, path)
		verifyCh <- statResult{entry: e, err: err}
	}()
	go func() {
		dlink, err := fs.resolveDlink(ctx, path, pinned.FSID, pinned.Size, pinned.MtimeSec)
		dlinkCh <- dlinkResult{dlink: dlink, err: err}
	}()

	verify := <-verifyCh
	linkRes := <-dlinkCh
	if verify.err != nil {
		return nil, classifyErr(verify.err)
	}
	if linkRes.err != nil {
		return nil, classifyErr(linkRes.err)
	}
	dlink := linkRes.dlink

	if verify.entry == nil {
		return nil, fmt.Errorf("OpenRead %s: stat returned nil entry", path)
	}
	if verify.entry.FSID != pinned.FSID || verify.entry.Size != pinned.Size || verify.entry.MtimeSec != pinned.MtimeSec {
		// pinned 来源可能是 metadata 缓存；冲突后立即失效，下一次 open 才能取到新版本。
		if fs.metaInvalidate != nil {
			fs.metaInvalidate(path)
		}
		log.Printf("OpenRead %s: version conflict (pinned fsID=%d size=%d mtime=%d vs current fsID=%d size=%d mtime=%d)",
			path, pinned.FSID, pinned.Size, pinned.MtimeSec, verify.entry.FSID, verify.entry.Size, verify.entry.MtimeSec)
		return nil, mount.ErrVersionConflict
	}

	r := &remoteReader{
		fsID:       verify.entry.FSID,
		dlink:      dlink,
		size:       verify.entry.Size,
		mtime:      verify.entry.MtimeSec,
		fetcher:    fs.fetchRange,
		stopCh:     make(chan struct{}),
		bfs:        fs,
		path:       path,
		cacheEpoch: fs.BlockEpoch(verify.entry.FSID),
	}

	if fs.stats != nil {
		fs.stats.StartDownload(path, pinned.Size)
	}

	// 后台 dlink 刷新：TTL 过期前 5 分钟预刷新，避免首次读触发 403。
	if fs.dlinkCache != nil {
		ttl := fs.dlinkCache.TTL()
		refreshAfter := ttl - 5*time.Minute
		if refreshAfter > 0 {
			go r.backgroundRefresh(fs, path, pinned.FSID, pinned.Size, pinned.MtimeSec, refreshAfter)
		}
	}

	return r, nil
}

// fetchRange 适配 Scheduler.FetchRange 到 remoteReader 需要的函数签名（T6.7）。
func (fs *BaiduCloudFS) fetchRange(ctx context.Context, fsID int64, dlink string, off, length int64) ([]byte, error) {
	if fs.fetcher == nil {
		return nil, fmt.Errorf("fetcher not injected: call SetFetcher before OpenRead")
	}
	return fs.fetcher(ctx, fsID, dlink, off, length)
}

// backgroundRefresh 后台 dlink 刷新 goroutine。
// 去重后调用 reloadDlink（内部会校验版本），避免多实例同时刷新同一文件。
func (r *remoteReader) backgroundRefresh(fs *BaiduCloudFS, path string, fsID, size, mtime int64, after time.Duration) {
	jitter := time.Duration(rand.Int63n(int64(5 * time.Minute)))
	after += jitter

	timer := time.NewTimer(after)
	defer timer.Stop()
	select {
	case <-timer.C:
		key := refreshKey{account: fs.accountKey, fsID: fsID, path: path}
		if _, loaded := refreshInFlight.LoadOrStore(key, struct{}{}); loaded {
			return
		}
		defer refreshInFlight.Delete(key)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		dlink, err := fs.reloadDlink(ctx, path, fsID, size, mtime)
		if err != nil {
			if !errors.Is(err, mount.ErrVersionConflict) {
				log.Printf("backgroundRefresh %s: reload dlink failed: %v", path, err)
			}
			return
		}
		r.setDlink(dlink)
	case <-r.stopCh:
	}
}
