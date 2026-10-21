// Package fuse 实现 go-fuse/v2 NodeFileSystem 接口。
// MultiThreaded 模式，per-handle state 支持并发读写。
package fuse

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"baidupcs-fuse/adapter"
	"baidupcs-fuse/cache"
	"baidupcs-fuse/config"
	"baidupcs-fuse/download"
	"baidupcs-fuse/mount"
	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sync/singleflight"
)

// ============================================================
// 编译期接口检查：确保所有方法签名匹配 go-fuse 接口。
// 如果签名不匹配，编译时立即报错（而非运行时 ENOTSUP）。
// ============================================================
var (
	_ gofuse.NodeLookuper  = (*BaiduFS)(nil)
	_ gofuse.NodeGetattrer = (*BaiduFS)(nil)
	_ gofuse.NodeReaddirer = (*BaiduFS)(nil)
	_ gofuse.NodeCreater   = (*BaiduFS)(nil)
	_ gofuse.NodeMkdirer   = (*BaiduFS)(nil)
	_ gofuse.NodeMknoder   = (*BaiduFS)(nil)
	_ gofuse.NodeUnlinker  = (*BaiduFS)(nil)
	_ gofuse.NodeRmdirer   = (*BaiduFS)(nil)
	_ gofuse.NodeStatfser  = (*BaiduFS)(nil)
	_ gofuse.NodeRenamer   = (*BaiduFS)(nil)

	_ gofuse.NodeLookuper  = (*dirNode)(nil)
	_ gofuse.NodeGetattrer = (*dirNode)(nil)
	_ gofuse.NodeReaddirer = (*dirNode)(nil)
	_ gofuse.NodeCreater   = (*dirNode)(nil)
	_ gofuse.NodeMkdirer   = (*dirNode)(nil)
	_ gofuse.NodeMknoder   = (*dirNode)(nil)
	_ gofuse.NodeRenamer   = (*dirNode)(nil)
	_ gofuse.NodeUnlinker  = (*dirNode)(nil)
	_ gofuse.NodeRmdirer   = (*dirNode)(nil)

	_ gofuse.NodeOpener    = (*fileNode)(nil)
	_ gofuse.NodeGetattrer = (*fileNode)(nil)
	_ gofuse.NodeSetattrer = (*fileNode)(nil)
	_ gofuse.NodeReleaser  = (*fileNode)(nil)
	_ gofuse.NodeFlusher   = (*fileNode)(nil)
	_ gofuse.NodeFsyncer   = (*fileNode)(nil)

	_ gofuse.FileReader = (*readFileHandle)(nil)
	_ gofuse.FileReader = (*writeFileHandle)(nil)
	_ gofuse.FileWriter = (*writeFileHandle)(nil)
)

// ============================================================
// 根节点 + 共享上下文
// ============================================================

// BaiduFS 根 inode。
type BaiduFS struct {
	gofuse.Inode
	fs       *baiduFSContext
	rootPath string
}

// baiduFSContext 跨 inode 共享资源。
type baiduFSContext struct {
	opts       config.MountOptions
	cloudFS    mount.CloudFS
	meta       *cache.MetadataCache
	blocks     *cache.BlockCache
	disk       *cache.DiskCache
	sched      *download.Scheduler
	bfs        *adapter.BaiduCloudFS // 依赖注入入口（仅 Mount 时 type-assert）
	blockGroup singleflight.Group
	quota      *quotaCache   // 配额缓存（Statfs 用）
	refreshMu  sync.Mutex    // 保护后台目录刷新不并发执行
	rootInode  *gofuse.Inode // 根 inode，用于按内核目录树解析 rename 后的动态路径
	rootRemote string        // 远端根路径
	// 下载/上传追踪回调（可选，由 Mount 注入）
	onTrackDownload func(path string, bytes int64)
	onTrackUpload   func(path string, bytes int64)
}

// quotaCache 配额信息缓存（避免频繁调 QuotaInfo API）。
type quotaCache struct {
	mu       sync.RWMutex
	stats    *mount.FSStats
	expireAt time.Time
	ttl      time.Duration
}

func newQuotaCache(ttl time.Duration) *quotaCache {
	return &quotaCache{ttl: ttl}
}

func (c *quotaCache) Get() (*mount.FSStats, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.stats == nil || time.Now().After(c.expireAt) {
		return nil, false
	}
	return c.stats, true
}

func (c *quotaCache) Set(stats *mount.FSStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = stats
	c.expireAt = time.Now().Add(c.ttl)
}

func newContext(opts config.MountOptions, cloudFS mount.CloudFS) *baiduFSContext {
	// 库调用/测试可能传零值；这里统一兜底，避免除零、无效缓存与无界并发。
	if opts.AccountKey == "" {
		opts.AccountKey = "default"
	}
	if opts.CacheDir == "" {
		opts.CacheDir = filepath.Join(os.TempDir(), "baidupcs-fuse-cache", opts.AccountKey)
	}
	if opts.ReadBlockSize <= 0 {
		opts.ReadBlockSize = 32 * 1024 * 1024
	}
	if opts.BlockCacheSize <= 0 {
		opts.BlockCacheSize = 512 * 1024 * 1024
	}
	if opts.MetadataTTL <= 0 {
		opts.MetadataTTL = 30 * time.Second
	}
	if opts.DirTTL <= 0 {
		opts.DirTTL = 5 * time.Minute
	}
	if opts.MaxParallel <= 0 {
		opts.MaxParallel = 10
	}
	if opts.UploadSliceSize <= 0 {
		opts.UploadSliceSize = 8 * 1024 * 1024
	}
	if opts.CurlTimeout <= 0 {
		opts.CurlTimeout = 60 * time.Second
	}
	if opts.HttpTimeout <= 0 {
		opts.HttpTimeout = 120 * time.Second
	}
	return &baiduFSContext{
		opts:    opts,
		cloudFS: cloudFS,
		meta:    cache.NewMetadataCache(opts.AccountKey, opts.MetadataTTL, opts.DirTTL, 5*time.Second),
		blocks:  cache.NewBlockCache(opts.AccountKey, opts.BlockCacheSize, opts.ReadBlockSize),
		disk:    cache.NewDiskCache(opts.CacheDir, opts.DiskCacheSize),
		sched:   download.NewScheduler(opts.MaxParallel, opts.HttpTimeout, opts.CurlTimeout),
		quota:   newQuotaCache(5 * time.Minute),
	}
}

// remotePath 根据内核 inode 树动态计算远端路径。
//
// 直接缓存 node.path 会在 rename 后失效：内核把 dentry 移到新名字，但
// 我们的 dirNode/fileNode 仍持有旧路径。gofuse.Inode.Path(root) 会沿
// 当前 parent 链重新计算，因此目录 rename 后子节点无需逐个更新。
// 测试中未设置 rootInode 时退回 fallback（构造时传入的路径）。
func (fs *baiduFSContext) remotePath(inode *gofuse.Inode, fallback string) string {
	p, _ := fs.resolveRemotePath(inode, fallback)
	return p
}

// resolveRemotePath 返回动态远端路径以及 inode 是否仍在目录树中。
// linked=false 表示已被 unlink/父目录删除；写路径必须丢弃未提交数据，
// 不能在旧路径重建文件。
func (fs *baiduFSContext) resolveRemotePath(inode *gofuse.Inode, fallback string) (string, bool) {
	if fs.rootInode == nil || inode == nil {
		return fallback, true
	}
	p := inode.Path(fs.rootInode)
	if p == "" {
		return fs.rootRemote, true
	}
	if strings.Contains(p, ".go-fuse.") {
		return fallback, false
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if fs.rootRemote == "/" || fs.rootRemote == "" {
		return p, true
	}
	return fs.rootRemote + p, true
}

func (n *dirNode) remotePath() string {
	return n.fs.remotePath(n.EmbeddedInode(), n.path)
}

func (n *fileNode) remotePath() string {
	return n.fs.remotePath(n.EmbeddedInode(), n.path)
}

func (n *fileNode) remotePathStatus() (string, bool) {
	return n.fs.resolveRemotePath(n.EmbeddedInode(), n.path)
}

// attachWriterPathProvider 让 commit 时动态解析路径：rename 后写新路径，
// unlink 后丢弃未提交数据。
type writerPathProvider interface {
	SetPathProvider(func() (string, bool))
}

func attachWriterPathProvider(w mount.RemoteWriter, n *fileNode) {
	if p, ok := w.(writerPathProvider); ok {
		p.SetPathProvider(n.remotePathStatus)
	}
}

// invalidateBlocks 清掉某 fsID 的内存/磁盘块缓存（写入或截断后调用），
// 并递增块缓存版本，阻止并发中的旧 reader 把旧版本块回填进新缓存。
func (fs *baiduFSContext) invalidateBlocks(fsID int64) {
	if fsID <= 0 {
		return
	}
	fs.blocks.InvalidateFSID(fs.opts.AccountKey, fsID)
	fs.disk.InvalidateFSID(fs.opts.AccountKey, fsID)
	if fs.bfs != nil {
		fs.bfs.BumpBlockEpoch(fsID)
	}
}

// Mount 挂载 FUSE 文件系统（MultiThreaded 并发模式）。
// 返回 server + shutdown 函数（取消所有进行中的 curl 下载）。
func Mount(opts config.MountOptions, cloudFS mount.CloudFS) (*fuse.Server, func(), error) {
	// 必须在 flock 之前兜底，否则空 CacheDir 会尝试锁当前目录/空路径。
	if opts.AccountKey == "" {
		opts.AccountKey = "default"
	}
	if opts.CacheDir == "" {
		opts.CacheDir = filepath.Join(os.TempDir(), "baidupcs-fuse-cache", opts.AccountKey)
	}
	// 远端根路径统一去掉尾部斜杠，避免 /docs/ + /name 变成 /docs//name。
	opts.RemotePath = strings.TrimRight(opts.RemotePath, "/")
	if opts.RemotePath == "" {
		opts.RemotePath = "/"
	}
	if _, err := exec.LookPath("curl"); err != nil {
		return nil, nil, fmt.Errorf("curl not found in PATH (required for Baidu CDN downloads): %w", err)
	}
	instanceLock, err := acquireInstanceLock(opts.CacheDir)
	if err != nil {
		return nil, nil, err
	}
	ctx := newContext(opts, cloudFS)
	root := &BaiduFS{fs: ctx, rootPath: opts.RemotePath}
	ctx.rootInode = root.EmbeddedInode()
	ctx.rootRemote = opts.RemotePath

	// 挂载生命周期 ctx：所有后台上传随取消而终止（卸载/退出时由 shutdownFn 触发）
	mountCtx, mountCancel := context.WithCancel(context.Background())

	// CDN 下载需要 BDUSS Cookie 认证
	if opts.BDUSS != "" {
		ctx.sched.SetExtraHeaders(map[string]string{
			"Cookie": "BDUSS=" + opts.BDUSS,
		})
	}

	// 单文件并发下载块数（默认1=串行；显式 >1 才启用单文件并行，全局仍受 max-parallel 约束）
	ctx.sched.SetPerFileCap(opts.PerFileSem)

	// 注入缓存回调（finish_upload 后失效 + OpenRead 首次 Stat 走缓存 + upload 后清 BlockCache）
	if bfs, ok := cloudFS.(*adapter.BaiduCloudFS); ok {
		ctx.bfs = bfs
		bfs.SetMetaInvalidate(ctx.meta.Invalidate)
		bfs.SetStatCache(ctx.meta.Get)
		bfs.SetBlockInvalidate(ctx.invalidateBlocks)
		bfs.SetFetcher(ctx.sched.FetchRange)
		bfs.SetMountContext(mountCtx)
		bfs.SetAsyncClose(opts.AsyncClose)
		bfs.SetCommitTimeout(opts.CommitTimeout)
		// 注入传输追踪回调（消除 FUSE 层运行时 type-assert）
		if stats := bfs.Stats(); stats != nil {
			ctx.onTrackDownload = stats.TrackDownload
			ctx.onTrackUpload = stats.TrackUpload
		}
	}

	fsOpts := &gofuse.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:     opts.AllowOther,
			SingleThreaded: false, // Phase 3: 启用并发 FUSE 操作
			MaxWrite:       1 << 20,
			MaxBackground:  64, // 允许足够多的并发请求排队，避免顺序读被 stat 阻塞
		},
		NullPermissions: true,
		EntryTimeout:    durationPtr(opts.MetadataTTL),
		AttrTimeout:     durationPtr(opts.MetadataTTL),
	}

	server, err := gofuse.Mount(opts.MountPoint, root, fsOpts)
	if err != nil {
		releaseInstanceLock(instanceLock)
		mountCancel()
		return nil, nil, err
	}

	// 启动后台目录刷新 + Unix socket 监听 + DiskCache 定期刷盘
	stopRefresh := ctx.startBackgroundRefresh(opts)
	stopSocket := ctx.startRefreshSocket(opts)
	stopDiskFlush := ctx.startDiskFlush()

	var shutdownOnce sync.Once
	shutdownFn := func() {
		shutdownOnce.Do(func() {
			// 1) 先停止接受新写入；2) 停后台任务；3) drain 在途 commit（有上限）；4) 再取消 mountCtx。
			if ctx.bfs != nil {
				ctx.bfs.BeginShutdown()
			}
			stopRefresh()
			stopSocket()
			stopDiskFlush()

			if ctx.bfs != nil {
				drainTimeout := opts.CommitTimeout
				if drainTimeout <= 0 {
					drainTimeout = 30 * time.Second
				}
				drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
				if err := ctx.bfs.WaitCommits(drainCtx); err != nil {
					log.Printf("shutdown: 等待在途上传超时（%v），未完成项保留 staging 供 recover", err)
				}
				cancel()
			}
			ctx.disk.Close() // 最后一次刷盘
			ctx.sched.Shutdown()
			mountCancel()
			releaseInstanceLock(instanceLock)
		})
	}
	return server, shutdownFn, nil
}

func durationPtr(d time.Duration) *time.Duration { return &d }

// ============================================================
// 后台目录刷新
// ============================================================

// startBackgroundRefresh 启动后台目录刷新 goroutine。
// 每 interval 检查一次缓存目录，对需要刷新的目录重新拉取。
func (ctx *baiduFSContext) startBackgroundRefresh(opts config.MountOptions) func() {
	interval := opts.DirRefreshInterval
	if interval <= 0 {
		return func() {} // 禁用
	}

	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if ctx.refreshMu.TryLock() { // 非阻塞：上一次没完成就跳过
					ctx.refreshStaleDirs(opts)
					ctx.refreshMu.Unlock()
				}
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

// refreshStaleDirs 刷新所有需要更新的缓存目录。
func (ctx *baiduFSContext) refreshStaleDirs(opts config.MountOptions) {
	dirs := ctx.meta.ListDirs()
	refreshed := 0
	for _, d := range dirs {
		if !ctx.meta.NeedsRefresh(d.Path, opts.DirRefreshInterval) {
			continue
		}
		// 重新拉取目录列表；必须有超时，否则一次网络卡死会永久占用刷新锁。
		timeout := opts.HttpTimeout
		if timeout <= 0 || timeout > 30*time.Second {
			timeout = 30 * time.Second
		}
		rctx, cancel := context.WithTimeout(context.Background(), timeout)
		entries, err := ctx.cloudFS.ReadDir(rctx, d.Path)
		cancel()
		if err != nil {
			log.Printf("后台刷新 %s 失败: %v", d.Path, err)
			continue
		}
		ctx.meta.SetDir(d.Path, entries)
		ctx.meta.SetDirRefreshed(d.Path)
		// 同时更新文件 stat 缓存
		for _, e := range entries {
			if e == nil {
				continue
			}
			childPath := d.Path + "/" + e.Name
			if d.Path == "/" {
				childPath = "/" + e.Name
			}
			ctx.meta.Set(childPath, e)
		}
		refreshed++
	}
	if refreshed > 0 {
		log.Printf("后台刷新完成: %d 个目录", refreshed)
	}
}

// ============================================================
// DiskCache 定期刷盘
// ============================================================

// startDiskFlush 每 5 分钟将 manifest 刷盘 + 清理空闲信号量。
func (ctx *baiduFSContext) startDiskFlush() func() {
	return ctx.startDiskFlushInterval(5 * time.Minute)
}

// startDiskFlushInterval 允许测试用较短周期验证刷盘循环。
func (ctx *baiduFSContext) startDiskFlushInterval(interval time.Duration) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ctx.disk.Flush()
				ctx.sched.CleanupFileSems()
			case <-done:
				return
			}
		}
	}()
	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			close(done)
			<-stopped
		})
	}
}

// ============================================================
// Unix Socket 刷新命令监听
// ============================================================

// refreshRequest 刷新请求。Type="health" 时为健康查询。
type refreshRequest struct {
	Type string `json:"type"` // "refresh"（默认）或 "health"
	Path string `json:"path"` // 要刷新的路径（空=全部）
}

// refreshResponse 刷新响应。含活跃下载监控信息。
type refreshResponse struct {
	OK            bool     `json:"ok"`
	Message       string   `json:"message"`
	Refreshed     int      `json:"refreshed"`
	ActiveFetches int      `json:"active_fetches"` // 当前活跃的 Range GET 数量
	StuckCount    int      `json:"stuck_count"`    // 运行超时的 fetch 数量
	StuckDetail   []string `json:"stuck_detail"`   // 卡死 fetch 详情（路径/范围/时长）
}

// refreshSocketPath 返回 Unix socket 路径。
func refreshSocketPath(opts config.MountOptions) string {
	return filepath.Join(opts.CacheDir, "refresh.sock")
}

// startRefreshSocket 启动 Unix socket 监听，接收 bdfs refresh 命令。
func (ctx *baiduFSContext) startRefreshSocket(opts config.MountOptions) func() {
	sockPath := refreshSocketPath(opts)
	// 清理旧 socket
	os.Remove(sockPath)

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		log.Printf("刷新 socket 监听失败: %v", err)
		return func() {}
	}
	// 设置权限（仅当前用户可访问）
	os.Chmod(sockPath, 0600)

	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					log.Printf("刷新 socket accept 失败: %v", err)
					continue
				}
			}
			go ctx.handleRefreshConn(conn, opts)
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			close(done)
			ln.Close()
			<-stopped
			os.Remove(sockPath)
		})
	}
}

// handleRefreshConn 处理单个刷新连接。
func (ctx *baiduFSContext) handleRefreshConn(conn net.Conn, opts config.MountOptions) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second)) // 防止客户端不关连接导致永久阻塞

	var req refreshRequest
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&req); err != nil {
		resp := refreshResponse{OK: false, Message: fmt.Sprintf("解析请求失败: %v", err)}
		json.NewEncoder(conn).Encode(resp)
		return
	}

	// 健康查询：返回活跃下载与卡死检测，供 `mount status` 使用。
	if req.Type == "health" {
		resp := ctx.doHealth()
		json.NewEncoder(conn).Encode(resp)
		return
	}

	resp := ctx.doRefresh(req.Path, opts)
	json.NewEncoder(conn).Encode(resp)
}

// doHealth 返回当前下载调度器的活跃/卡死状态。
func (ctx *baiduFSContext) doHealth() refreshResponse {
	stucks := ctx.sched.StuckFetches(30 * time.Second) // >30s 视为疑似卡死
	resp := refreshResponse{OK: true, ActiveFetches: ctx.sched.ActiveFetchCount()}
	if len(stucks) > 0 {
		resp.StuckCount = len(stucks)
		for _, st := range stucks {
			dur := time.Since(st.Started).Round(time.Second)
			resp.StuckDetail = append(resp.StuckDetail, fmt.Sprintf("fsID=%d off=%d len=%d 已运行%v", st.FsID, st.Off, st.Length, dur))
		}
	}
	return resp
}

// doRefresh 执行刷新操作。
func (ctx *baiduFSContext) doRefresh(path string, opts config.MountOptions) refreshResponse {
	refreshCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if path == "" {
		// 全量刷新：刷新所有缓存目录
		dirs := ctx.meta.ListDirs()
		refreshed := 0
		for _, d := range dirs {
			entries, err := ctx.cloudFS.ReadDir(refreshCtx, d.Path)
			if err != nil {
				log.Printf("手动刷新 %s 失败: %v", d.Path, err)
				continue
			}
			ctx.meta.SetDir(d.Path, entries)
			ctx.meta.SetDirRefreshed(d.Path)
			for _, e := range entries {
				if e == nil {
					continue
				}
				childPath := d.Path + "/" + e.Name
				if d.Path == "/" {
					childPath = "/" + e.Name
				}
				ctx.meta.Set(childPath, e)
			}
			refreshed++
		}
		return refreshResponse{OK: true, Message: fmt.Sprintf("已刷新 %d 个目录", refreshed), Refreshed: refreshed}
	}

	// 单路径刷新
	entries, err := ctx.cloudFS.ReadDir(refreshCtx, path)
	if err != nil {
		return refreshResponse{OK: false, Message: fmt.Sprintf("刷新失败: %v", err)}
	}
	ctx.meta.SetDir(path, entries)
	ctx.meta.SetDirRefreshed(path)
	for _, e := range entries {
		if e == nil {
			continue
		}
		childPath := path + "/" + e.Name
		if path == "/" {
			childPath = "/" + e.Name
		}
		ctx.meta.Set(childPath, e)
	}
	// 同时失效文件 stat 缓存（文件内容可能已变；dlink 由版本化 key 隔离）。
	for _, e := range entries {
		if e != nil && !e.IsDir {
			ctx.meta.Invalidate(joinPath(path, e.Name))
		}
	}
	return refreshResponse{OK: true, Message: fmt.Sprintf("已刷新 %s（%d 个条目）", path, len(entries)), Refreshed: 1}
}

// ============================================================
// BaiduFS 根节点操作
// ============================================================

func (n *BaiduFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	return n.fs.lookupChild(ctx, n.EmbeddedInode(), n.rootPath, name, out)
}

func (n *BaiduFS) Getattr(ctx context.Context, f gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Ino = 1
	out.Attr.Mode = syscall.S_IFDIR | 0755
	out.Attr.Nlink = 2
	out.Attr.Size = 4096
	out.AttrValid = uint64(n.fs.opts.DirTTL / time.Second)
	return 0
}

func (n *BaiduFS) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	entries, errno := n.fs.readDirEntries(ctx, n.rootPath)
	if errno != 0 {
		return nil, errno
	}
	return dirStream(entries), 0
}

// Unlink 删除根目录下的文件。
func (n *BaiduFS) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.fs.removePath(ctx, n.childPath(name), false)
}

// Rmdir 删除根目录下的空目录。
func (n *BaiduFS) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.fs.removePath(ctx, n.childPath(name), true)
}

// Rename 支持根目录下的条目重命名/移动。
func (n *BaiduFS) Rename(ctx context.Context, name string, newParent gofuse.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newDirPath, ok := n.fs.embeddedPath(newParent)
	if !ok {
		return syscall.EINVAL
	}
	newPath := joinPath(newDirPath, newName)
	if errno := n.fs.checkRenameFlags(ctx, newPath, flags); errno != 0 {
		return errno
	}
	return n.fs.renamePath(ctx, n.childPath(name), newPath, n.rootPath, newDirPath)
}

func (n *BaiduFS) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// 配额缓存：5min TTL，避免频繁 API 调用
	if stats, ok := n.fs.quota.Get(); ok {
		out.Blocks = uint64(stats.Total / 512)
		out.Bfree = uint64(stats.Free / 512)
		out.Bavail = uint64(stats.Free / 512)
		out.Bsize = 512
		out.NameLen = 255
		out.Frsize = 512
		return 0
	}

	stats, err := n.fs.cloudFS.StatFS(ctx)
	if err != nil {
		return mount.BaiduErrorToErrno(err)
	}
	n.fs.quota.Set(stats)

	out.Blocks = uint64(stats.Total / 512)
	out.Bfree = uint64(stats.Free / 512)
	out.Bavail = uint64(stats.Free / 512)
	out.Bsize = 512
	out.NameLen = 255
	out.Frsize = 512
	return 0
}

func (n *BaiduFS) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	return n.fs.mkdirChild(ctx, n.EmbeddedInode(), n.rootPath, name, out)
}

// Create 创建新文件（FUSE write 路径入口）。
func (n *BaiduFS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, gofuse.FileHandle, uint32, syscall.Errno) {
	return n.fs.createFile(ctx, n.EmbeddedInode(), n.rootPath, name, flags, out)
}

// Mknod 兜底：某些系统调 mknod 而非 create 创建普通文件。
func (n *BaiduFS) Mknod(ctx context.Context, name string, mode uint32, rdev uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	return n.fs.mknodFile(ctx, n.EmbeddedInode(), n.rootPath, name, mode, out)
}

// ============================================================
// dirNode
// ============================================================

type dirNode struct {
	gofuse.Inode
	fs   *baiduFSContext
	path string
}

func (n *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	return n.fs.lookupChild(ctx, n.EmbeddedInode(), n.remotePath(), name, out)
}

func (n *dirNode) Getattr(ctx context.Context, f gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = syscall.S_IFDIR | 0755
	out.Attr.Nlink = 2
	out.Attr.Size = 4096
	out.AttrValid = uint64(n.fs.opts.DirTTL / time.Second)
	return 0
}

func (n *dirNode) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	entries, errno := n.fs.readDirEntries(ctx, n.remotePath())
	if errno != 0 {
		return nil, errno
	}
	return dirStream(entries), 0
}

func (n *dirNode) Rename(ctx context.Context, name string, newParent gofuse.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	newDirPath, ok := n.fs.embeddedPath(newParent)
	if !ok {
		return syscall.EINVAL
	}
	newPath := joinPath(newDirPath, newName)
	if errno := n.fs.checkRenameFlags(ctx, newPath, flags); errno != 0 {
		return errno
	}
	oldParent := n.remotePath()
	return n.fs.renamePath(ctx, joinPath(oldParent, name), newPath, oldParent, newDirPath)
}

// Linux renameat2 flags：RENAME_NOREPLACE=1，RENAME_EXCHANGE=2。
const (
	renameNoReplace = 1
	renameExchange  = 2
)

func (fs *baiduFSContext) checkRenameFlags(ctx context.Context, newPath string, flags uint32) syscall.Errno {
	if flags&renameExchange != 0 {
		return syscall.EINVAL // 远端 API 不支持原子交换
	}
	if flags&renameNoReplace != 0 {
		if _, err := fs.cloudFS.Stat(ctx, newPath); err == nil {
			return syscall.EEXIST
		} else if errno := mount.BaiduErrorToErrno(err); errno != syscall.ENOENT {
			return errno
		}
	}
	return 0
}

// renamePath 统一处理同目录/跨目录 rename：只有确认目标已存在才删除目标；
// 其余错误直接返回 errno，绝不因为网络失败先删用户文件。
func (fs *baiduFSContext) renamePath(ctx context.Context, oldPath, newPath, oldParent, newParent string) syscall.Errno {
	if oldPath == newPath {
		return 0 // POSIX: 同一路径 rename 是 no-op；不能走 EEXIST 的删除目标分支
	}
	if isSubpath(newPath, oldPath) {
		return syscall.EINVAL // 不能把目录移动到自身内部
	}
	err := fs.cloudFS.Rename(ctx, oldPath, newPath)
	if err != nil {
		errno := mount.BaiduErrorToErrno(err)
		if errno != syscall.EEXIST {
			log.Printf("Rename %s → %s: %v", oldPath, newPath, err)
			return errno
		}
		// 目标已存在：先确认源仍在，避免删掉唯一副本。
		srcEntry, serr := fs.cloudFS.Stat(ctx, oldPath)
		if serr != nil {
			return mount.BaiduErrorToErrno(serr)
		}
		if srcEntry == nil {
			return syscall.EIO
		}
		dstEntry, derr := fs.cloudFS.Stat(ctx, newPath)
		if derr != nil {
			if mount.BaiduErrorToErrno(derr) == syscall.ENOENT {
				// 目标在两次调用之间消失：直接重试 rename。
				if err = fs.cloudFS.Rename(ctx, oldPath, newPath); err != nil {
					return mount.BaiduErrorToErrno(err)
				}
				goto renamed
			}
			return mount.BaiduErrorToErrno(derr)
		}
		if dstEntry == nil {
			return syscall.EIO
		}
		if srcEntry.IsDir != dstEntry.IsDir {
			if dstEntry.IsDir {
				return syscall.EISDIR
			}
			return syscall.ENOTDIR
		}
		if dstEntry.IsDir {
			// 百度 PCS delete 可能递归删除。POSIX rename 只允许覆盖空目录，
			// 因此必须先确认目标目录为空，绝不能直接 Remove。
			entries, rerr := fs.cloudFS.ReadDir(ctx, newPath)
			if rerr != nil {
				return mount.BaiduErrorToErrno(rerr)
			}
			if len(entries) > 0 {
				return syscall.ENOTEMPTY
			}
		}
		// 不能先删目标：源 rename 若失败，目标就永久丢失。先把目标改名到同目录
		// 的隐藏备份，源 rename 成功后再删备份；失败则回滚备份。
		var backup string
		for attempt := 0; attempt < 3; attempt++ {
			backup = joinPath(newParent, fmt.Sprintf(".bdfs-rename-%d-%d", os.Getpid(), time.Now().UnixNano()+int64(attempt)))
			berr := fs.cloudFS.Rename(ctx, newPath, backup)
			if berr == nil {
				break
			}
			errno := mount.BaiduErrorToErrno(berr)
			if errno == syscall.ENOENT {
				// 目标在检查后消失：直接重试源 rename。
				if err = fs.cloudFS.Rename(ctx, oldPath, newPath); err != nil {
					return mount.BaiduErrorToErrno(err)
				}
				goto renamed
			}
			if errno != syscall.EEXIST {
				return errno
			}
			backup = ""
		}
		if backup == "" {
			return syscall.EEXIST
		}
		if err = fs.cloudFS.Rename(ctx, oldPath, newPath); err != nil {
			errno := mount.BaiduErrorToErrno(err)
			if rbErr := fs.cloudFS.Rename(ctx, backup, newPath); rbErr != nil {
				// 回滚也失败：目标内容仍在 backup，不能静默丢弃路径。
				log.Printf("Rename %s → %s 失败且回滚失败；原目标保留在 %s: %v", oldPath, newPath, backup, rbErr)
				return syscall.EIO
			}
			return errno
		}
		if rerr := fs.cloudFS.Remove(ctx, backup); rerr != nil {
			// 源已经改名成功，不能把操作报成失败；残留隐藏备份只记录日志。
			log.Printf("Rename %s → %s 成功，但清理备份 %s 失败: %v", oldPath, newPath, backup, rerr)
		}
	}

renamed:
	fs.meta.InvalidatePrefix(oldPath)
	fs.meta.InvalidatePrefix(newPath)
	fs.meta.InvalidateParent(oldParent)
	fs.meta.InvalidateParent(newParent)
	return 0
}

func (n *dirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	return n.fs.mkdirChild(ctx, n.EmbeddedInode(), n.remotePath(), name, out)
}

// Unlink 删除文件。
func (n *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return n.fs.removePath(ctx, joinPath(n.remotePath(), name), false)
}

// Rmdir 删除空目录。
func (n *dirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return n.fs.removePath(ctx, joinPath(n.remotePath(), name), true)
}

// Create 在子目录中创建新文件。
func (n *dirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, gofuse.FileHandle, uint32, syscall.Errno) {
	return n.fs.createFile(ctx, n.EmbeddedInode(), n.remotePath(), name, flags, out)
}

// Mknod 兜底：某些系统调 mknod 而非 create 创建普通文件。
func (n *dirNode) Mknod(ctx context.Context, name string, mode uint32, rdev uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	return n.fs.mknodFile(ctx, n.EmbeddedInode(), n.remotePath(), name, mode, out)
}

// ============================================================
// fileNode — per-inode 元数据（reader/writer 移到 FileHandle）
// ============================================================

type fileNode struct {
	gofuse.Inode
	fs      *baiduFSContext
	path    string
	entry   *mount.RemoteEntry
	entryAt time.Time // 最近一次从远端/缓存填充 entry 的时间
	mu      sync.RWMutex

	// 并发写保护：跟踪活跃写 handle（同文件多 fd 写 → EBUSY）
	writers     map[*writeFileHandle]struct{}
	opening     bool
	writersMu   sync.Mutex
	prefetching atomic.Bool
	statMu      sync.Mutex
}

func (n *fileNode) addWriter(h *writeFileHandle) {
	n.writersMu.Lock()
	if n.writers == nil {
		n.writers = make(map[*writeFileHandle]struct{})
	}
	n.writers[h] = struct{}{}
	n.writersMu.Unlock()
}

func (n *fileNode) removeWriter(h *writeFileHandle) {
	n.writersMu.Lock()
	delete(n.writers, h)
	n.writersMu.Unlock()
}

func (n *fileNode) setSize(size int64) {
	n.mu.Lock()
	if n.entry != nil {
		n.entry.Size = size
	}
	n.mu.Unlock()
}

// ============================================================
// Per-Handle State（Phase 2: 并发安全）
// ============================================================

// readFileHandle 读文件句柄。每个 fd 独立 reader + streak（并发读不竞争）。
type readFileHandle struct {
	node   *fileNode // 回源用（BlockCache/Streak/path）
	reader mount.RemoteReader
	streak *cache.Streak // per-handle 顺序读追踪
}

// writeFileHandle 写文件句柄。每个 fd 独立 writer + reader（写洞回源）。
type writeFileHandle struct {
	node   *fileNode
	writer mount.RemoteWriter
	reader mount.RemoteReader // 写洞回填用（per-handle，不共享）
}

func (n *fileNode) Getattr(ctx context.Context, f gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// 超过 MetadataTTL 后回源一次，保证网页端/其它客户端修改可见。
	path := n.remotePath()
	n.statMu.Lock()
	n.mu.RLock()
	stale := n.entry == nil || time.Since(n.entryAt) > n.fs.opts.MetadataTTL
	n.mu.RUnlock()
	if stale {
		e, err := n.fs.cloudFS.Stat(ctx, path)
		if err == nil {
			if e == nil {
				n.statMu.Unlock()
				return syscall.EIO
			}
			cp := cloneEntry(e)
			n.mu.Lock()
			n.entry = cp
			n.entryAt = time.Now()
			n.mu.Unlock()
			n.fs.meta.Set(path, cp)
		} else if os.IsNotExist(err) || mount.BaiduErrorToErrno(err) == syscall.ENOENT {
			n.fs.meta.Invalidate(path)
			n.statMu.Unlock()
			return syscall.ENOENT
		}
	}
	n.statMu.Unlock()

	n.mu.RLock()
	if n.entry == nil {
		n.mu.RUnlock()
		return syscall.ENOENT
	}
	size := n.entry.Size
	mtime := n.entry.MtimeSec
	mode := n.entry.Mode
	n.mu.RUnlock()
	out.Attr.Size = uint64(size)
	out.Attr.Mtime = uint64(mtime)
	out.Attr.Mode = syscall.S_IFREG | mode
	out.Attr.Nlink = 1
	out.AttrValid = uint64(n.fs.opts.MetadataTTL / time.Second)
	return 0
}

func (n *fileNode) Open(ctx context.Context, flags uint32) (handle gofuse.FileHandle, openedFlags uint32, errno syscall.Errno) {
	return n.openAt(ctx, n.remotePath(), flags)
}

// openAt 用显式远端路径打开。createFile 复用它时节点尚未 NewInode，
// 不能调用 remotePath（go-fuse Inode 尚未初始化，Path 会 panic）。
func (n *fileNode) openAt(ctx context.Context, path string, flags uint32) (handle gofuse.FileHandle, openedFlags uint32, errno syscall.Errno) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Open %s: PANIC recovered: %v", path, r)
			errno = syscall.EIO
			handle = nil
		}
	}()

	// O_DIRECT/O_SYNC 对网盘无意义：忽略，避免内核要求不支持的语义。
	flags &^= syscall.O_SYNC

	writeMode := flags & syscall.O_ACCMODE
	isWrite := writeMode == syscall.O_WRONLY || writeMode == syscall.O_RDWR || flags&syscall.O_TRUNC != 0

	if isWrite {
		if !n.fs.opts.EnableWrite {
			return nil, 0, syscall.EROFS
		}

		// 锁只保护 writers map 与 opening 占位，绝不跨网络调用。
		n.writersMu.Lock()
		if n.writers == nil {
			n.writers = make(map[*writeFileHandle]struct{})
		}
		if n.opening || len(n.writers) > 0 {
			n.writersMu.Unlock()
			return nil, 0, syscall.EBUSY
		}
		n.opening = true
		n.writersMu.Unlock()

		writer, err := n.fs.cloudFS.CreateWriter(ctx, path)
		if err != nil {
			n.writersMu.Lock()
			n.opening = false
			n.writersMu.Unlock()
			return nil, 0, mount.BaiduErrorToErrno(err)
		}
		attachWriterPathProvider(writer, n)

		var reader mount.RemoteReader
		if flags&syscall.O_TRUNC != 0 {
			if err := writer.Reset(); err != nil {
				n.writersMu.Lock()
				n.opening = false
				n.writersMu.Unlock()
				writer.Close(ctx)
				return nil, 0, mount.BaiduErrorToErrno(err)
			}
			n.mu.RLock()
			fsID := int64(0)
			if n.entry != nil {
				fsID = n.entry.FSID
			}
			n.mu.RUnlock()
			n.fs.invalidateBlocks(fsID)
			n.setSize(0)
			writer.SetTargetSize(0)
		} else {
			// 非 O_TRUNC：必须 pin 当前远端版本（即使 entry 看起来是空文件），
			// 否则 metadata 过期时 targetSize 会偏小，commit 会截断远端尾部。
			r, rerr := n.fs.cloudFS.OpenRead(ctx, path)
			if rerr != nil {
				n.writersMu.Lock()
				n.opening = false
				n.writersMu.Unlock()
				writer.Close(ctx)
				return nil, 0, mount.BaiduErrorToErrno(rerr)
			}
			reader = r
			writer.SetReader(r)
			writer.SetTargetSize(r.Size())
			n.setSize(r.Size())
			n.mu.Lock()
			if n.entry != nil {
				n.entry.Size = r.Size()
				n.entry.MtimeSec = r.Mtime()
			}
			n.mu.Unlock()
		}

		// 同步更新 metadata 缓存：O_TRUNC/打开期间其它 lookup 不应继续看到旧大小。
		n.mu.RLock()
		if n.entry != nil {
			entryCopy := cloneEntry(n.entry)
			n.mu.RUnlock()
			n.fs.meta.Set(path, entryCopy)
		} else {
			n.mu.RUnlock()
		}

		wh := &writeFileHandle{node: n, writer: writer, reader: reader}
		n.writersMu.Lock()
		n.opening = false
		if n.writers == nil {
			n.writers = make(map[*writeFileHandle]struct{})
		}
		n.writers[wh] = struct{}{}
		n.writersMu.Unlock()
		return wh, 0, 0
	}

	r, err := n.fs.cloudFS.OpenRead(ctx, path)
	if err != nil {
		log.Printf("Open %s: OpenRead failed: %v", path, err)
		return nil, 0, mount.BaiduErrorToErrno(err)
	}
	return &readFileHandle{node: n, reader: r, streak: cache.NewStreak()}, 0, 0
}

func (h *readFileHandle) Read(ctx context.Context, buf []byte, off int64) (result fuse.ReadResult, errno syscall.Errno) {
	n := h.node
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Read %s: PANIC recovered: %v", n.remotePath(), r)
			errno = syscall.EIO
			result = nil
		}
	}()

	if n.fs.sched.IsShutdown() {
		return nil, syscall.EIO
	}

	r := h.reader
	if r == nil {
		return nil, syscall.EBADF
	}
	size := len(buf)
	if size == 0 {
		return fuse.ReadResultData(nil), 0
	}
	if off < 0 {
		return nil, syscall.EINVAL
	}
	if off >= r.Size() {
		return fuse.ReadResultData(nil), 0 // EOF
	}

	h.streak.Advance(pathHash(n.remotePath()), off+int64(size))

	blockSize := n.fs.opts.ReadBlockSize
	blockOff := off / blockSize * blockSize

	// ReadAt 内部已在 403/416 第一现场刷新 dlink 并只重试一次；这里不再重复刷新。
	data, err := n.fs.readBlock(ctx, r, blockOff)
	if err != nil {
		log.Printf("Read %s: block %d failed: %v", n.remotePath(), blockOff, err)
		return nil, mount.BaiduErrorToErrno(err)
	}

	// 顺序读确认后异步预读下一块；预读自身有界，失败静默。
	if h.streak.SequentialCount() >= 2 {
		go n.prefetch(r, blockOff+blockSize)
	}

	start := off - blockOff
	if start >= int64(len(data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := start + int64(size)
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	got := end - start
	if got < int64(size) && blockOff+int64(len(data)) < r.Size() {
		// 跨块且未到 EOF：配置的块小于内核单次读时会跨多块，
		// 统一交给 readRange，禁止只补第二块后返回短读。
		readEnd := off + int64(size)
		if readEnd > r.Size() {
			readEnd = r.Size()
		}
		full, rerr := n.fs.readRange(ctx, r, off, readEnd)
		if rerr != nil {
			return nil, mount.BaiduErrorToErrno(rerr)
		}
		n.trackDownload(int64(len(full)))
		return fuse.ReadResultData(full), 0
	}

	n.trackDownload(got)
	return fuse.ReadResultData(data[start:end]), 0
}

func readerCacheEpoch(r mount.RemoteReader) uint64 {
	if versioned, ok := r.(interface{ CacheEpoch() uint64 }); ok {
		return versioned.CacheEpoch()
	}
	return 0
}

// blockEpochCurrent 判断 reader 打开时的块缓存版本是否仍是当前版本。
// 没有注入真实 BaiduCloudFS 或 reader 不提供版本时保持旧行为（可缓存）。
func (fs *baiduFSContext) blockEpochCurrent(r mount.RemoteReader) bool {
	if fs.bfs == nil {
		return true
	}
	versioned, ok := r.(interface{ CacheEpoch() uint64 })
	if !ok {
		return true
	}
	return versioned.CacheEpoch() == fs.bfs.BlockEpoch(r.FSID())
}

// readBlock 是唯一的读块原语：缓存（内存→磁盘）→ single-flight 回源 → 校验/回填。
// 缓存项不可变，返回的切片只允许读。
//
// 版本保护：commit/truncate 会失效块缓存并递增 epoch。打开于旧版本的 reader
// 即使还在读，也不得命中或回填新版本缓存，否则会把旧内容写回刚失效的缓存。
func (fs *baiduFSContext) readBlock(ctx context.Context, r mount.RemoteReader, blockOff int64) ([]byte, error) {
	if blockOff < 0 || blockOff >= r.Size() {
		return nil, nil
	}
	key := cache.BlockKeyFromPath(fs.opts.AccountKey, r.FSID(), r.Size(), r.Mtime(), blockOff)
	key.Epoch = readerCacheEpoch(r)
	if fs.blockEpochCurrent(r) {
		if data, ok := fs.blocks.Get(key); ok {
			return data, nil
		}
		if data, ok := fs.disk.Get(key); ok {
			fs.blocks.Put(key, data)
			return data, nil
		}
	}

	fetchSize := fs.opts.ReadBlockSize
	if remain := r.Size() - blockOff; fetchSize > remain {
		fetchSize = remain
	}
	if fetchSize <= 0 {
		return nil, nil
	}

	v, err, _ := fs.blockGroup.Do(key.String(), func() (interface{}, error) {
		// 等锁期间可能已被其它 goroutine 填充。
		if fs.blockEpochCurrent(r) {
			if data, ok := fs.blocks.Get(key); ok {
				return data, nil
			}
			if data, ok := fs.disk.Get(key); ok {
				fs.blocks.Put(key, data)
				return data, nil
			}
		}
		data, ferr := r.ReadAt(ctx, blockOff, int(fetchSize))
		if ferr != nil {
			return nil, ferr
		}
		if len(data) != int(fetchSize) {
			return nil, download.ErrShortRead
		}
		// 回源期间可能发生了 commit/truncate：旧版本数据不得回填。
		if fs.blockEpochCurrent(r) {
			fs.blocks.Put(key, data)
			fs.disk.Put(key, data)
		}
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	data, _ := v.([]byte)
	if len(data) != int(fetchSize) {
		return nil, download.ErrShortRead
	}
	return data, nil
}

// readRange 通过 readBlock 读取 [off,end)，用于写句柄的远端回源。
func (fs *baiduFSContext) readRange(ctx context.Context, r mount.RemoteReader, off, end int64) ([]byte, error) {
	if off < 0 || end < off || end > r.Size() {
		return nil, fmt.Errorf("invalid range [%d,%d) size=%d", off, end, r.Size())
	}
	out := make([]byte, 0, end-off)
	for pos := off; pos < end; {
		blockOff := pos / fs.opts.ReadBlockSize * fs.opts.ReadBlockSize
		block, err := fs.readBlock(ctx, r, blockOff)
		if err != nil {
			return nil, err
		}
		start := pos - blockOff
		if start >= int64(len(block)) {
			return nil, download.ErrShortRead
		}
		n := end - pos
		if avail := int64(len(block)) - start; n > avail {
			n = avail
		}
		out = append(out, block[start:start+n]...)
		pos += n
	}
	return out, nil
}

// readCrossBlock 保留为跨块读取的兼容入口（旧测试/调用方使用）。
// 新读路径统一走 readBlock/readRange；这里复用同一套缓存与校验。
func readCrossBlock(ctx context.Context, fs *baiduFSContext, r mount.RemoteReader, nodePath string, fsID, mtime, blockOff, blockSize, off int64, size int, firstBlockData []byte, startInBlock int64) (fuse.ReadResult, bool, error) {
	need := int64(size) - (int64(len(firstBlockData)) - startInBlock)
	if need <= 0 {
		return nil, false, nil
	}
	secondOff := blockOff + blockSize
	if secondOff >= r.Size() {
		combined := append([]byte(nil), firstBlockData[startInBlock:]...)
		return fuse.ReadResultData(combined), true, nil
	}
	data2, err := fs.readBlock(ctx, r, secondOff)
	if err != nil {
		combined := append([]byte(nil), firstBlockData[startInBlock:]...)
		return fuse.ReadResultData(combined), true, err
	}
	take := need
	if take > int64(len(data2)) {
		take = int64(len(data2))
	}
	combined := make([]byte, 0, int64(len(firstBlockData))-startInBlock+take)
	combined = append(combined, firstBlockData[startInBlock:]...)
	combined = append(combined, data2[:take]...)
	return fuse.ReadResultData(combined), true, nil
}

// trackDownload 记录下载字节（仅用户实际读到的，不含预读/重试）。
func (n *fileNode) trackDownload(bytes int64) {
	if n.fs.onTrackDownload != nil {
		n.fs.onTrackDownload(n.remotePath(), bytes)
	}
}

// readFromWriter 从 staging 读取，不足部分通过统一 readRange 从远端回源。
func (h *writeFileHandle) readFromWriter(ctx context.Context, off int64, size int) (fuse.ReadResult, syscall.Errno) {
	n := h.node
	data, err := h.writer.ReadFrom(ctx, off, size)
	if err != nil {
		return nil, mount.BaiduErrorToErrno(err)
	}
	if len(data) >= size {
		return fuse.ReadResultData(data[:size]), 0
	}

	total := h.writer.TargetSize()
	if s := h.writer.Size(); s > total {
		total = s
	}
	n.mu.RLock()
	if n.entry != nil && n.entry.Size > total {
		total = n.entry.Size
	}
	n.mu.RUnlock()
	start := off + int64(len(data))
	if start >= total {
		return fuse.ReadResultData(data), 0 // EOF
	}
	end := start + int64(size-len(data))
	if end > total {
		end = total
	}
	r := h.reader
	if r == nil {
		if end > start {
			return nil, syscall.EIO
		}
		return fuse.ReadResultData(data), 0
	}
	// truncate 增大后，目标区间可能超出 pinned 远端版本。超出 oldSize 的部分
	// 在 POSIX 语义下是零填充，绝不能把 [oldSize,end) 当作远端 Range 读取
	// （那会返回 EINVAL/EIO，导致 truncate -s +N 后读扩展区失败）。
	remoteEnd := end
	if remoteEnd > r.Size() {
		remoteEnd = r.Size()
	}
	var rest []byte
	if start < remoteEnd {
		var rerr error
		rest, rerr = n.fs.readRange(ctx, r, start, remoteEnd)
		if rerr != nil {
			log.Printf("readFromWriter %s: remote fallback [%d,%d) failed: %v", n.remotePath(), start, remoteEnd, rerr)
			return nil, mount.BaiduErrorToErrno(rerr)
		}
	}
	if zeroes := end - start - int64(len(rest)); zeroes > 0 {
		rest = append(rest, make([]byte, zeroes)...)
	}
	return fuse.ReadResultData(append(data, rest...)), 0
}

// prefetch 异步预读下一块；每个句柄最多 1 个在途，重复预读由 readBlock single-flight 合并。
func (n *fileNode) prefetch(r mount.RemoteReader, blockOff int64) {
	if !n.prefetching.CompareAndSwap(false, true) {
		return
	}
	defer n.prefetching.Store(false)
	if blockOff < 0 || blockOff >= r.Size() {
		return
	}
	key := cache.BlockKeyFromPath(n.fs.opts.AccountKey, r.FSID(), r.Size(), r.Mtime(), blockOff)
	key.Epoch = readerCacheEpoch(r)
	if n.fs.blocks.Has(key) || n.fs.disk.Has(key) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := n.fs.readBlock(ctx, r, blockOff); err != nil {
		log.Printf("prefetch %s: block %d failed: %v", n.remotePath(), blockOff, err)
	}
}

func (n *fileNode) Release(ctx context.Context, f gofuse.FileHandle) syscall.Errno {
	switch h := f.(type) {
	case *readFileHandle:
		if h.reader != nil {
			_ = h.reader.Close()
		}
	case *writeFileHandle:
		n.removeWriter(h)
		if h.writer != nil {
			// writer 拥有 reader 生命周期；Release 不再对 h.reader 二次 Close。
			if err := h.writer.Close(ctx); err != nil {
				log.Printf("Release %s: commit failed: %v", n.remotePath(), err)
				return mount.BaiduErrorToErrno(err)
			}
		}
	}
	return 0
}

// Setattr 处理文件属性变更（truncate 等）。
func (n *fileNode) Setattr(ctx context.Context, f gofuse.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// 处理 truncate
	if in.Valid&(fuse.FATTR_SIZE) != 0 {
		newSize := int64(in.Size)
		if wh, ok := f.(*writeFileHandle); ok && wh != nil && wh.writer != nil {
			if newSize != wh.writer.TargetSize() {
				if err := wh.writer.Truncate(newSize); err != nil {
					return mount.BaiduErrorToErrno(err)
				}
			}
			n.setSize(newSize)
		} else if errno := n.fs.truncatePath(ctx, n.remotePath(), newSize); errno != 0 {
			return errno
		} else {
			n.setSize(newSize)
		}
		n.mu.Lock()
		n.entryAt = time.Now()
		n.mu.Unlock()
	}

	// 保留 mtime：rsync 等工具写入后会 utimensat，不记录会导致全量重传死循环。
	// 远端接口不保证可写 mtime，至少要在本地元数据缓存内保持到 TTL 之后。
	if in.Valid&(fuse.FATTR_MTIME) != 0 {
		n.mu.Lock()
		if n.entry != nil {
			n.entry.MtimeSec = int64(in.Mtime)
		}
		n.entryAt = time.Now()
		snapshot := cloneEntry(n.entry)
		n.mu.Unlock()
		if snapshot != nil {
			n.fs.meta.Set(n.remotePath(), snapshot)
		}
	}

	// 刷新当前属性（字段必须在锁内读取，entry 本身可能被并发替换）。
	n.mu.RLock()
	var size, mtime int64
	if n.entry != nil {
		size, mtime = n.entry.Size, n.entry.MtimeSec
	}
	n.mu.RUnlock()
	out.Attr.Size = uint64(size)
	out.Attr.Mtime = uint64(mtime)
	out.Attr.Mode = syscall.S_IFREG | 0644
	out.Attr.Nlink = 1
	return 0
}

// Write 写入数据到文件（通过 RemoteWriter → staging）。
func (h *writeFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n := h.node
	if err := h.writer.Write(ctx, off, data); err != nil {
		return 0, mount.BaiduErrorToErrno(err)
	}
	// 更新 entry.Size（Getattr 依赖此值，否则 cat 看到 size=0 不读取）
	newEnd := off + int64(len(data))
	n.mu.Lock()
	if n.entry != nil && newEnd > n.entry.Size {
		n.entry.Size = newEnd
	}
	n.mu.Unlock()
	return uint32(len(data)), 0
}

// Read 从写 handle 读取（read_session：暂存 + 回源）。
func (h *writeFileHandle) Read(ctx context.Context, buf []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return h.readFromWriter(ctx, off, len(buf))
}

// Flush 提交脏数据（close = commit）。
// 对齐 POSIX flush 语义：用户可见错误，上传失败返回 EIO。
func (n *fileNode) Flush(ctx context.Context, f gofuse.FileHandle) syscall.Errno {
	wh, ok := f.(*writeFileHandle)
	if !ok || wh == nil {
		return 0
	}
	w := wh.writer
	if w == nil {
		return 0
	}
	// 只在有脏数据时上传（Sync 内部已置 dirty=false，Release 据此跳过）
	if !w.Dirty() {
		return 0
	}
	if err := w.Sync(ctx); err != nil {
		return mount.BaiduErrorToErrno(err)
	}
	return 0
}

// Fsync 显式落盘：调用 RemoteWriter.Sync（POSIX fsync 语义）。
func (n *fileNode) Fsync(ctx context.Context, f gofuse.FileHandle, flags uint32) syscall.Errno {
	wh, ok := f.(*writeFileHandle)
	if !ok || wh == nil {
		return 0
	}
	w := wh.writer
	if w == nil {
		return 0
	}
	if err := w.Sync(ctx); err != nil {
		return mount.BaiduErrorToErrno(err)
	}
	return 0
}

// truncatePath 无活跃写 handle 时的 truncate：创建一次性 writer，同步提交后清理。
func (fs *baiduFSContext) truncatePath(ctx context.Context, path string, size int64) syscall.Errno {
	// 先 Stat 再做 no-op/类型检查：避免无意义地创建（并截断）staging session，
	// 也避免同尺寸 truncate 触发整文件重传。
	entry, err := fs.cloudFS.Stat(ctx, path)
	if err != nil {
		return mount.BaiduErrorToErrno(err)
	}
	if entry == nil {
		return syscall.EIO
	}
	if entry.IsDir {
		return syscall.EISDIR
	}
	oldSize := entry.Size
	if size == oldSize {
		return 0
	}

	writer, err := fs.cloudFS.CreateWriter(ctx, path)
	if err != nil {
		return mount.BaiduErrorToErrno(err)
	}
	writer.SetTargetSize(oldSize)
	// 只要截断后仍保留非空内容，就必须回填 [0,min(size,oldSize))；
	// 否则 staging 会从 0 扩展成零字节，把原文件内容清零。
	if size > 0 && oldSize > 0 {
		r, rerr := fs.cloudFS.OpenRead(ctx, path)
		if rerr != nil {
			_ = writer.Close(ctx)
			return mount.BaiduErrorToErrno(rerr)
		}
		writer.SetReader(r)
	}

	if err := writer.Truncate(size); err != nil {
		_ = writer.Close(ctx)
		return mount.BaiduErrorToErrno(err)
	}
	if err := writer.Close(ctx); err != nil {
		return mount.BaiduErrorToErrno(err)
	}
	fs.invalidateBlocks(entry.FSID)
	fs.meta.Invalidate(path)
	fs.meta.InvalidateParent(path)
	return 0
}

func (fs *baiduFSContext) lookupChild(ctx context.Context, parent *gofuse.Inode, parentPath, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	childPath := joinPath(parentPath, name)
	if fs.meta.GetNegative(childPath) {
		return nil, syscall.ENOENT
	}
	if entry, ok := fs.meta.Get(childPath); ok {
		return fs.makeChild(ctx, parent, childPath, entry, out), 0
	}
	entry, err := fs.cloudFS.Stat(ctx, childPath)
	if err != nil {
		errno := mount.BaiduErrorToErrno(err)
		if errno == syscall.ENOENT {
			fs.meta.SetNegative(childPath)
		}
		return nil, errno
	}
	if entry == nil {
		return nil, syscall.EIO
	}
	fs.meta.Set(childPath, entry)
	return fs.makeChild(ctx, parent, childPath, entry, out), 0
}

func (fs *baiduFSContext) makeChild(ctx context.Context, parent *gofuse.Inode, childPath string, entry *mount.RemoteEntry, out *fuse.EntryOut) *gofuse.Inode {
	entry = cloneEntry(entry)
	var ops gofuse.InodeEmbedder
	if entry.IsDir {
		ops = &dirNode{fs: fs, path: childPath}
	} else {
		ops = &fileNode{fs: fs, path: childPath, entry: entry, entryAt: time.Now()}
	}
	child := parent.NewInode(ctx, ops, stableAttr(entry))
	setEntryOut(out, child, entry)
	return child
}

func (fs *baiduFSContext) readDirEntries(ctx context.Context, path string) ([]*mount.RemoteEntry, syscall.Errno) {
	if entries, ok := fs.meta.GetDir(path); ok {
		return entries, 0
	}
	entries, err := fs.cloudFS.ReadDir(ctx, path)
	if err != nil {
		return nil, mount.BaiduErrorToErrno(err)
	}
	fs.meta.SetDir(path, entries)
	for _, e := range entries {
		if e != nil {
			fs.meta.Set(joinPath(path, e.Name), e)
		}
	}
	return entries, 0
}

func dirStream(entries []*mount.RemoteEntry) gofuse.DirStream {
	r := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		if e == nil {
			continue
		}
		mode := uint32(syscall.S_IFREG)
		if e.IsDir {
			mode = uint32(syscall.S_IFDIR)
		}
		r = append(r, fuse.DirEntry{Name: e.Name, Mode: mode})
	}
	return gofuse.NewListDirStream(r)
}

func (fs *baiduFSContext) mkdirChild(ctx context.Context, parent *gofuse.Inode, parentPath, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	childPath := joinPath(parentPath, name)
	// API 层把 EEXIST 视为幂等成功；FUSE 的 mkdir(2) 仍需向用户返回 EEXIST。
	if e, err := fs.cloudFS.Stat(ctx, childPath); err == nil && e != nil {
		return nil, syscall.EEXIST
	} else if err != nil && !os.IsNotExist(err) && mount.BaiduErrorToErrno(err) != syscall.ENOENT {
		return nil, mount.BaiduErrorToErrno(err)
	}
	if err := fs.cloudFS.Mkdir(ctx, childPath); err != nil {
		return nil, mount.BaiduErrorToErrno(err)
	}
	entry := &mount.RemoteEntry{Path: childPath, Name: name, IsDir: true, Mode: 0755}
	fs.meta.Set(childPath, entry)
	fs.meta.InvalidateParent(childPath)
	child := parent.NewInode(ctx, &dirNode{fs: fs, path: childPath}, stableAttr(entry))
	setEntryOut(out, child, entry)
	return child, 0
}

// ============================================================
// 共享节点操作（根节点与子目录共用，避免两套实现漂移）
// ============================================================

func (fs *baiduFSContext) embeddedPath(n gofuse.InodeEmbedder) (string, bool) {
	switch v := n.(type) {
	case *BaiduFS:
		return v.rootPath, true
	case *dirNode:
		return v.remotePath(), true
	default:
		return "", false
	}
}

func joinPath(dir, name string) string {
	if dir == "" || dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

func cloneEntry(e *mount.RemoteEntry) *mount.RemoteEntry {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}

func (fs *baiduFSContext) createFile(ctx context.Context, parent *gofuse.Inode, parentPath, name string, flags uint32, out *fuse.EntryOut) (*gofuse.Inode, gofuse.FileHandle, uint32, syscall.Errno) {
	if !fs.opts.EnableWrite {
		return nil, 0, 0, syscall.EROFS
	}
	childPath := joinPath(parentPath, name)

	existing, err := fs.cloudFS.Stat(ctx, childPath)
	if err == nil && existing != nil {
		if existing.IsDir {
			return nil, 0, 0, syscall.EISDIR
		}
		if flags&syscall.O_EXCL != 0 {
			return nil, 0, 0, syscall.EEXIST
		}
		entry := cloneEntry(existing)
		node := &fileNode{fs: fs, path: childPath, entry: entry, entryAt: time.Now()}
		// 复用 Open 的 writers/opening 互斥和 O_TRUNC/版本 pin 逻辑，
		// 避免 Create 与 Open 并发时同一路径出现两个写 handle。
		h, _, errno := node.openAt(ctx, childPath, flags)
		if errno != 0 {
			return nil, 0, 0, errno
		}
		child := parent.NewInode(ctx, node, stableAttr(entry))
		if wh, ok := h.(*writeFileHandle); ok && wh != nil {
			attachWriterPathProvider(wh.writer, node)
		}
		setEntryOutFile(out, child, entry)
		return child, h, 0, 0
	}
	if err != nil && !os.IsNotExist(err) {
		return nil, 0, 0, mount.BaiduErrorToErrno(err)
	}

	writer, err := fs.cloudFS.CreateWriter(ctx, childPath)
	if err != nil {
		return nil, 0, 0, mount.BaiduErrorToErrno(err)
	}
	// 新文件即使没有写入也必须在 close 时落一个 0 字节远端文件（touch 语义）。
	writer.MarkDirty()
	entry := &mount.RemoteEntry{Path: childPath, Name: name, IsDir: false, Mode: 0644}
	node := &fileNode{fs: fs, path: childPath, entry: entry, entryAt: time.Now()}
	child := parent.NewInode(ctx, node, stableAttr(entry))
	setEntryOutFile(out, child, entry)
	fs.meta.Set(childPath, cloneEntry(entry))
	fs.meta.InvalidateParent(childPath)

	wh := &writeFileHandle{node: node, writer: writer}
	attachWriterPathProvider(writer, node)
	node.addWriter(wh)
	return child, wh, 0, 0
}

func (fs *baiduFSContext) mknodFile(ctx context.Context, parent *gofuse.Inode, parentPath, name string, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	if mode&syscall.S_IFMT != syscall.S_IFREG {
		return nil, syscall.EINVAL
	}
	if !fs.opts.EnableWrite {
		return nil, syscall.EROFS
	}
	childPath := joinPath(parentPath, name)
	if e, err := fs.cloudFS.Stat(ctx, childPath); err == nil {
		if e == nil {
			return nil, syscall.EIO
		}
		if e.IsDir {
			return nil, syscall.EISDIR
		}
		return nil, syscall.EEXIST
	} else if !os.IsNotExist(err) {
		return nil, mount.BaiduErrorToErrno(err)
	}

	writer, err := fs.cloudFS.CreateWriter(ctx, childPath)
	if err != nil {
		return nil, mount.BaiduErrorToErrno(err)
	}
	writer.MarkDirty()
	if err := writer.Close(ctx); err != nil {
		return nil, mount.BaiduErrorToErrno(err)
	}

	entry := &mount.RemoteEntry{Path: childPath, Name: name, IsDir: false, Size: 0, Mode: 0644}
	node := &fileNode{fs: fs, path: childPath, entry: entry, entryAt: time.Now()}
	child := parent.NewInode(ctx, node, stableAttr(entry))
	setEntryOutFile(out, child, entry)
	fs.meta.Set(childPath, cloneEntry(entry))
	fs.meta.InvalidateParent(childPath)
	return child, 0
}

func (fs *baiduFSContext) removePath(ctx context.Context, path string, isDir bool) syscall.Errno {
	// 删除前确认类型：unlink 绝不能落到目录上（百度 delete 可能递归）。
	entry, serr := fs.cloudFS.Stat(ctx, path)
	if serr != nil {
		errno := mount.BaiduErrorToErrno(serr)
		if errno == syscall.ENOENT {
			// 幂等：目标已经不存在视为成功；仍要清掉可能残留的缓存，
			// 否则后续 lookup 会在 TTL 内继续命中旧条目。
			fs.invalidateRemoved(path, isDir)
			return 0
		}
		return errno
	}
	if entry == nil {
		return syscall.EIO
	}
	if isDir && !entry.IsDir {
		return syscall.ENOTDIR
	}
	if !isDir && entry.IsDir {
		return syscall.EISDIR
	}
	// rmdir 必须在本地确认目录为空：百度 delete 对非空目录的行为因接口而异，
	// 不能把“是否递归删除”交给远端默认策略。
	if isDir {
		entries, derr := fs.cloudFS.ReadDir(ctx, path)
		if derr == nil && len(entries) > 0 {
			return syscall.ENOTEMPTY
		}
		if derr != nil {
			errno := mount.BaiduErrorToErrno(derr)
			if errno == syscall.ENOENT {
				fs.invalidateRemoved(path, isDir)
				return 0
			}
			log.Printf("Rmdir %s: readdir failed: %v", path, derr)
			return errno
		}
	}
	if err := fs.cloudFS.Remove(ctx, path); err != nil {
		errno := mount.BaiduErrorToErrno(err)
		if errno == syscall.ENOENT {
			// 幂等：目标已经不存在视为成功。
			fs.invalidateRemoved(path, isDir)
			return 0
		}
		if isDir && errno == syscall.EEXIST {
			return syscall.ENOTEMPTY
		}
		log.Printf("Remove %s: %v", path, err)
		return errno
	}
	fs.invalidateRemoved(path, isDir)
	return 0
}

func (fs *baiduFSContext) invalidateRemoved(path string, isDir bool) {
	fs.meta.Invalidate(path)
	fs.meta.InvalidatePrefix(path)
	if isDir {
		fs.meta.InvalidateDir(path)
	}
	fs.meta.InvalidateParent(path)
}

func stableAttr(e *mount.RemoteEntry) gofuse.StableAttr {
	if e == nil {
		return gofuse.StableAttr{Mode: uint32(syscall.S_IFREG)}
	}
	mode := uint32(syscall.S_IFREG)
	if e.IsDir {
		mode = uint32(syscall.S_IFDIR)
	}
	ino := uint64(0)
	if e.FSID > 0 {
		ino = uint64(e.FSID)
	}
	return gofuse.StableAttr{Ino: ino, Mode: mode}
}

// ============================================================
// 辅助
// ============================================================

func (n *BaiduFS) childPath(name string) string {
	if n.rootPath == "/" {
		return "/" + name
	}
	return n.rootPath + "/" + name
}

func setEntryOut(out *fuse.EntryOut, child *gofuse.Inode, entry *mount.RemoteEntry) {
	out.Attr.Ino = child.StableAttr().Ino
	if entry.IsDir {
		out.Attr.Mode = uint32(syscall.S_IFDIR) | 0755
		out.Attr.Nlink = 2
	} else {
		out.Attr.Mode = uint32(syscall.S_IFREG) | 0644
		out.Attr.Size = uint64(entry.Size)
		out.Attr.Mtime = uint64(entry.MtimeSec)
		out.Attr.Nlink = 1
	}
}

func setEntryOutFile(out *fuse.EntryOut, child *gofuse.Inode, entry *mount.RemoteEntry) {
	out.Attr.Ino = child.StableAttr().Ino
	out.Attr.Mode = uint32(syscall.S_IFREG) | 0644
	out.Attr.Size = uint64(entry.Size)
	out.Attr.Mtime = uint64(entry.MtimeSec)
	out.Attr.Nlink = 1
}

// isSubpath 判断 child 是否位于 parent 目录内部（parent 不是 child 本身）。
func isSubpath(child, parent string) bool {
	if parent == "/" {
		return strings.HasPrefix(child, "/") && child != "/"
	}
	return strings.HasPrefix(child, parent+"/")
}

// pathHash 将路径转为稳定的 uint64 用于 streak inode 标识。
func pathHash(p string) uint64 {
	var h uint64
	for i := 0; i < len(p); i++ {
		h = h*31 + uint64(p[i])
	}
	return h
}
