package adapter

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"baidupcs-fuse/download"
	"baidupcs-fuse/mount"
)

// refreshInFlight 追踪正在刷新的 fsID，防止多实例同时刷新同一文件。
var refreshInFlight sync.Map

// ============================================================
// 编译期接口检查
// ============================================================
var _ mount.RemoteReader = (*remoteReader)(nil)

// FetchFunc Scheduler.FetchRange 的函数签名（T6.7）。
type FetchFunc func(ctx context.Context, fsID int64, dlink string, off, length int64) ([]byte, error)

// remoteReader mount.RemoteReader 的百度网盘实现。
// 一次 OpenRead = 一个被 pin 的版本快照 + dlink（带 TTL）。
type remoteReader struct {
	fsID    int64
	dlink   string
	size    int64
	mtime   int64 // pinned mtime（BlockCache key 用）
	fetcher FetchFunc
	stopCh  chan struct{} // 关闭时停止后台刷新
	bfs     *BaiduCloudFS // 用于 RefreshDlink fallback
	path    string        // 文件路径（RefreshDlink 用）
}

func (r *remoteReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	if off < 0 || size < 0 || off+int64(size) > r.size {
		return nil, fmt.Errorf("read range [%d, %d) exceeds file size %d", off, off+int64(size), r.size)
	}
	data, err := r.fetcher(ctx, r.fsID, r.dlink, off, int64(size))
	if err != nil {
		// 403/416: dlink 失效，上层需要重新 OpenRead
		if err == download.ErrCDNForbidden || err == download.ErrRangeNotSatisfiable {
			return nil, err
		}
		return nil, err
	}
	if len(data) != size {
		return nil, download.ErrShortRead
	}
	return data, nil
}

func (r *remoteReader) Size() int64  { return r.size }
func (r *remoteReader) FSID() int64  { return r.fsID }
func (r *remoteReader) Mtime() int64 { return r.mtime }
func (r *remoteReader) Close() error {
	if r.stopCh != nil {
		close(r.stopCh)
	}
	// 结束下载追踪
	if r.bfs != nil && r.bfs.stats != nil {
		r.bfs.stats.EndDownload(r.path)
	}
	return nil
}

// RefreshDlink 使用 method=download 获取新 dlink（重试多次获取不同 CDN 节点）。
func (r *remoteReader) RefreshDlink(ctx context.Context, path string) error {
	if r.bfs == nil {
		return fmt.Errorf("RefreshDlink: BaiduCloudFS not available")
	}
	// CDN 节点不稳定，重试多次可能命中不同节点
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(200*attempt) * time.Millisecond):
			}
		}
		newDlink, err := r.bfs.GetDlinkDownload(ctx, path)
		if err != nil {
			lastErr = err
			continue
		}
		r.dlink = newDlink
		return nil
	}
	return lastErr
}

// resolveDlink 解析下载 URL：命中 dlinkCache 直接复用（避免每次打开都 curl method=download），
// miss 时走 method=download，失败 fallback locatedownload，结果写入缓存供下次 OpenRead 复用。
func (fs *BaiduCloudFS) resolveDlink(ctx context.Context, path string, fsID int64) (string, error) {
	if fs.dlinkCache != nil {
		if cached, ok := fs.dlinkCache.Get(fsID); ok && cached != "" {
			log.Printf("resolveDlink: %s reuse cached url (fsID=%d)", path, fsID)
			return cached, nil
		}
	}
	dlink, err := fs.GetDlinkDownload(ctx, path)
	if err != nil {
		log.Printf("OpenRead %s: method=download failed (%v), trying locatedownload", path, err)
		dlink, _, _, _, err = fs.client.LocateDownload(ctx, path)
		if err != nil {
			return "", classifyErr(err)
		}
	}
	if fs.dlinkCache != nil {
		fs.dlinkCache.Set(fsID, dlink, path)
	}
	return dlink, nil
}

// OpenRead 版本 pin（T6.2 + AUDIT-002）：
// ①Stat pin (FSID,Size,MtimeSec) — 优先从 metadata cache 取
// ②method=download 获取 CDN URL（优先，实测可靠）
// ③fallback: locatedownload 获取 dlink（部分文件类型 CDN 签名失败）
func (fs *BaiduCloudFS) OpenRead(ctx context.Context, path string) (mount.RemoteReader, error) {
	// ① Stat pin：优先从 metadata cache 取（省 1 次 API）
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

	// ② method=download 获取 CDN URL（优先，实测可靠；命中 dlinkCache 则省掉一次 curl）
	dlink, err := fs.resolveDlink(ctx, path, entry.FSID)
	if err != nil {
		return nil, classifyErr(err)
	}

	// ③ 再 Stat 比对（AUDIT-002）：访问期检测到远端文件被替换 → ErrVersionConflict。
	// pinned 为 step① 的快照，verify 为取完 dlink 后的最新版本；三者任一不一致即视为版本漂移。
	verify, err := fs.client.Stat(ctx, path)
	if err != nil {
		return nil, classifyErr(err)
	}
	if verify.FSID != entry.FSID || verify.Size != entry.Size || verify.MtimeSec != entry.MtimeSec {
		log.Printf("OpenRead %s: version conflict (pinned fsID=%d size=%d mtime=%d vs current fsID=%d size=%d mtime=%d)",
			path, entry.FSID, entry.Size, entry.MtimeSec, verify.FSID, verify.Size, verify.MtimeSec)
		return nil, mount.ErrVersionConflict
	}

	r := &remoteReader{
		fsID:    verify.FSID,
		dlink:   dlink,
		size:    verify.Size,
		mtime:   verify.MtimeSec,
		fetcher: fs.fetchRange,
		stopCh:  make(chan struct{}),
		bfs:     fs,
		path:    path,
	}

	// 追踪下载任务
	if fs.stats != nil {
		fs.stats.StartDownload(path, entry.Size)
	}

	// 后台 dlink 刷新：TTL 过期前 5 分钟预刷新，避免首次读触发 403
	if fs.dlinkCache != nil {
		ttl := fs.dlinkCache.TTL()
		refreshAfter := ttl - 5*time.Minute
		if refreshAfter > 0 {
			go r.backgroundRefresh(fs, path, entry.FSID, refreshAfter)
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
// 在 dlink 过期前5分钟调用 LocateDownload 获取新 dlink 并更新 DlinkCache，
// 避免下次读触发 403 错误。
// 添加随机延迟（0-5分钟）和去重机制，防止多实例同时刷新。
func (r *remoteReader) backgroundRefresh(fs *BaiduCloudFS, path string, fsID int64, after time.Duration) {
	// 添加随机延迟：避免多实例同时刷新同一文件
	jitter := time.Duration(rand.Int63n(int64(5 * time.Minute)))
	after += jitter

	timer := time.NewTimer(after)
	defer timer.Stop()
	select {
	case <-timer.C:
		// 去重检查：相同 fsID 正在刷新则跳过
		if _, loaded := refreshInFlight.LoadOrStore(fsID, struct{}{}); loaded {
			log.Printf("backgroundRefresh %s: fsID %d already in flight, skipping", path, fsID)
			return
		}
		defer refreshInFlight.Delete(fsID)

		// 刷新 dlink
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		newDlink, _, _, _, err := fs.client.LocateDownload(ctx, path)
		if err != nil {
			log.Printf("backgroundRefresh %s: LocateDownload failed: %v", path, err)
			return
		}
		// 更新 DlinkCache（下次 ReadAt 用新 dlink）
		r.dlink = newDlink
		if fs.dlinkCache != nil {
			fs.dlinkCache.Set(fsID, newDlink, path)
		}
		log.Printf("backgroundRefresh %s: dlink refreshed successfully", path)
	case <-r.stopCh:
		// reader 已关闭
	}
}
