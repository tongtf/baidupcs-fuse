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
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"baidupcs-fuse/adapter"
	"baidupcs-fuse/cache"
	"baidupcs-fuse/config"
	"baidupcs-fuse/download"
	"baidupcs-fuse/mount"
	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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
	opts      config.MountOptions
	cloudFS   mount.CloudFS
	meta      *cache.MetadataCache
	blocks    *cache.BlockCache
	disk      *cache.DiskCache
	sched     *download.Scheduler
	im        *inodeManager
	quota     *quotaCache // 配额缓存（Statfs 用）
	refreshMu sync.Mutex  // 保护后台目录刷新不并发执行
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
	return &baiduFSContext{
		opts:    opts,
		cloudFS: cloudFS,
		meta:    cache.NewMetadataCache(opts.AccountKey, opts.MetadataTTL, opts.DirTTL, 5*time.Second),
		blocks:  cache.NewBlockCache(opts.AccountKey, opts.BlockCacheSize, opts.ReadBlockSize),
		disk:    cache.NewDiskCache(opts.CacheDir, opts.DiskCacheSize),
		sched:   download.NewScheduler(opts.MaxParallel, opts.HttpTimeout, opts.CurlTimeout),
		im:      newInodeManager(opts.DirTTL),
		quota:   newQuotaCache(5 * time.Minute),
	}
}

// Mount 挂载 FUSE 文件系统（MultiThreaded 并发模式）。
// 返回 server + shutdown 函数（取消所有进行中的 curl 下载）。
func Mount(opts config.MountOptions, cloudFS mount.CloudFS) (*fuse.Server, func(), error) {
	ctx := newContext(opts, cloudFS)
	root := &BaiduFS{fs: ctx, rootPath: opts.RemotePath}

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
		bfs.SetMetaInvalidate(ctx.meta.Invalidate)
		bfs.SetStatCache(ctx.meta.Get)
		bfs.SetFetcher(ctx.sched.FetchRange)
		bfs.SetBlockInvalidate(func(accountID string, fsID int64) {
			if fsID == -1 {
				ctx.blocks.InvalidateAllByAccount(accountID)
			} else {
				ctx.blocks.InvalidateByFSID(accountID, fsID)
			}
		})
		bfs.SetMountContext(mountCtx)
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
		},
		NullPermissions: true,
		EntryTimeout:    durationPtr(opts.MetadataTTL),
		AttrTimeout:     durationPtr(opts.MetadataTTL),
	}

	server, err := gofuse.Mount(opts.MountPoint, root, fsOpts)
	if err != nil {
		mountCancel()
		return nil, nil, err
	}

	// 启动后台目录刷新 + Unix socket 监听 + DiskCache 定期刷盘
	stopRefresh := ctx.startBackgroundRefresh(opts)
	stopSocket := ctx.startRefreshSocket(opts)
	stopDiskFlush := ctx.startDiskFlush()

	shutdownFn := func() {
		stopRefresh()
		stopSocket()
		stopDiskFlush()
		ctx.disk.Close() // 最后一次刷盘
		ctx.sched.Shutdown()
		ctx.im.Stop() // 停止 inodeManager 后台清理
		mountCancel() // 取消所有后台上传（回写到 mountCtx）
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
	go func() {
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
	return func() { close(done) }
}

// refreshStaleDirs 刷新所有需要更新的缓存目录。
func (ctx *baiduFSContext) refreshStaleDirs(opts config.MountOptions) {
	dirs := ctx.meta.ListDirs()
	refreshed := 0
	for _, d := range dirs {
		if !ctx.meta.NeedsRefresh(d.Path, opts.DirRefreshInterval) {
			continue
		}
		// 重新拉取目录列表
		entries, err := ctx.cloudFS.ReadDir(context.Background(), d.Path)
		if err != nil {
			log.Printf("后台刷新 %s 失败: %v", d.Path, err)
			continue
		}
		ctx.meta.SetDir(d.Path, entries)
		ctx.meta.SetDirRefreshed(d.Path)
		// 同时更新文件 stat 缓存
		for _, e := range entries {
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
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
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
	return func() { close(done) }
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
	go func() {
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

	return func() {
		close(done)
		ln.Close()
		os.Remove(sockPath)
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
		childPath := path + "/" + e.Name
		if path == "/" {
			childPath = "/" + e.Name
		}
		ctx.meta.Set(childPath, e)
	}
	// 同时失效 dlink（文件内容可能已变）
	for _, e := range entries {
		if !e.IsDir {
			ctx.meta.Invalidate(path + "/" + e.Name)
		}
	}
	return refreshResponse{OK: true, Message: fmt.Sprintf("已刷新 %s（%d 个条目）", path, len(entries)), Refreshed: 1}
}

// ============================================================
// BaiduFS 根节点操作
// ============================================================

func (n *BaiduFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	childPath := n.childPath(name)

	// 缓存检查：TTL 内命中直接返回，不打 API
	if entry, ok := n.fs.meta.Get(childPath); ok {
		return n.createChild(ctx, childPath, entry, out), 0
	}

	entry, err := n.fs.cloudFS.Stat(ctx, childPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, syscall.ENOENT
		}
		log.Printf("Lookup %s: %v", childPath, err)
		return nil, syscall.EIO
	}
	n.fs.meta.Set(childPath, entry)
	return n.createChild(ctx, childPath, entry, out), 0
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
	// 缓存检查
	if entries, ok := n.fs.meta.GetDir(n.rootPath); ok {
		r := make([]fuse.DirEntry, 0, len(entries))
		for _, e := range entries {
			mode := uint32(syscall.S_IFREG)
			if e.IsDir {
				mode = uint32(syscall.S_IFDIR)
			}
			r = append(r, fuse.DirEntry{Name: e.Name, Mode: mode})
		}
		return gofuse.NewListDirStream(r), 0
	}

	entries, err := n.fs.cloudFS.ReadDir(ctx, n.rootPath)
	if err != nil {
		return nil, syscall.EIO
	}
	n.fs.meta.SetDir(n.rootPath, entries)

	// 预填充 metadata 缓存（同 dirNode.Readdir）
	for _, e := range entries {
		childPath := n.rootPath + "/" + e.Name
		if n.rootPath == "/" {
			childPath = "/" + e.Name
		}
		n.fs.meta.Set(childPath, e)
	}

	r := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir {
			mode = uint32(syscall.S_IFDIR)
		}
		r = append(r, fuse.DirEntry{Name: e.Name, Mode: mode})
	}
	return gofuse.NewListDirStream(r), 0
}

// Unlink 删除根目录下的文件。
func (n *BaiduFS) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := n.childPath(name)
	if err := n.fs.cloudFS.Remove(ctx, childPath); err != nil {
		log.Printf("Unlink %s: %v", childPath, err)
		return syscall.EIO
	}
	n.fs.meta.Invalidate(childPath)       // 删文件自身的 metadata 缓存
	n.fs.meta.InvalidateParent(childPath) // 删父目录的 dir 缓存（ls 刷新）
	return 0
}

// Rmdir 删除根目录下的空目录。
func (n *BaiduFS) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := n.childPath(name)
	if err := n.fs.cloudFS.Remove(ctx, childPath); err != nil {
		log.Printf("Rmdir %s: %v", childPath, err)
		return syscall.EIO
	}
	n.fs.meta.Invalidate(childPath)       // 删目录自身的 metadata 缓存
	n.fs.meta.InvalidateDir(childPath)    // 删目录的子项列表缓存
	n.fs.meta.InvalidateParent(childPath) // 删父目录的 dir 缓存
	return 0
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
		return syscall.EIO
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
	childPath := n.childPath(name)
	if err := n.fs.cloudFS.Mkdir(ctx, childPath); err != nil {
		return nil, syscall.EIO
	}
	// 直接构造 entry，省掉一次 Stat API 调用
	entry := &mount.RemoteEntry{
		Path:  childPath,
		Name:  name,
		IsDir: true,
		Mode:  0755,
	}
	n.fs.meta.Set(childPath, entry)
	n.fs.meta.InvalidateParent(childPath)
	return n.createChild(ctx, childPath, entry, out), 0
}

// Create 创建新文件（FUSE write 路径入口）。
func (n *BaiduFS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, gofuse.FileHandle, uint32, syscall.Errno) {
	childPath := n.childPath(name)

	// 检查写是否启用
	if !n.fs.opts.EnableWrite {
		return nil, 0, 0, syscall.EROFS
	}

	// #8: 先 stat_remote，文件已存在则返回已有 inode
	existing, err := n.fs.cloudFS.Stat(ctx, childPath)
	if err == nil && existing != nil {
		node := &fileNode{fs: n.fs, path: childPath, entry: existing}
		stable := gofuse.StableAttr{Mode: uint32(syscall.S_IFREG)}
		child := n.EmbeddedInode().NewInode(ctx, node, stable)
		setEntryOutFile(out, child, existing)
		// 创建 writer + 返回 writeFileHandle
		writer, werr := n.fs.cloudFS.CreateWriter(ctx, childPath)
		if werr != nil {
			return child, nil, 0, 0
		}
		var reader mount.RemoteReader
		if existing.Size > 0 {
			r, rerr := n.fs.cloudFS.OpenRead(ctx, childPath)
			if rerr == nil {
				reader = r
				writer.SetReader(r)
			}
		}
		wh := &writeFileHandle{node: node, writer: writer, reader: reader}
		node.writersMu.Lock()
		if node.writers == nil {
			node.writers = make(map[*writeFileHandle]struct{})
		}
		node.writers[wh] = struct{}{}
		node.writersMu.Unlock()
		return child, wh, 0, 0
	}

	// 创建 RemoteWriter
	writer, err := n.fs.cloudFS.CreateWriter(ctx, childPath)
	if err != nil {
		return nil, 0, 0, syscall.EIO
	}

	node := &fileNode{
		fs:    n.fs,
		path:  childPath,
		entry: &mount.RemoteEntry{Path: childPath, Name: name, IsDir: false, Mode: 0644},
	}

	stable := gofuse.StableAttr{Mode: uint32(syscall.S_IFREG)}
	child := n.EmbeddedInode().NewInode(ctx, node, stable)
	out.Attr.Ino = child.StableAttr().Ino
	out.Attr.Mode = uint32(syscall.S_IFREG) | 0644
	out.Attr.Nlink = 1

	// 失效父目录列表缓存（新文件需刷新 ls）
	n.fs.meta.InvalidateParent(childPath)

	wh := &writeFileHandle{node: node, writer: writer}
	node.writersMu.Lock()
	node.writers = make(map[*writeFileHandle]struct{})
	node.writers[wh] = struct{}{}
	node.writersMu.Unlock()
	return child, wh, 0, 0
}

// Mknod 兜底：某些系统调 mknod 而非 create 创建普通文件。
func (n *BaiduFS) Mknod(ctx context.Context, name string, mode uint32, rdev uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	if mode&syscall.S_IFMT != syscall.S_IFREG {
		return nil, syscall.EINVAL
	}
	childPath := n.childPath(name)
	if !n.fs.opts.EnableWrite {
		return nil, syscall.EROFS
	}
	node := &fileNode{
		fs:    n.fs,
		path:  childPath,
		entry: &mount.RemoteEntry{Path: childPath, Name: name, IsDir: false, Mode: 0644},
	}
	stable := gofuse.StableAttr{Mode: uint32(syscall.S_IFREG)}
	child := n.EmbeddedInode().NewInode(ctx, node, stable)
	out.Attr.Ino = child.StableAttr().Ino
	out.Attr.Mode = uint32(syscall.S_IFREG) | 0644
	out.Attr.Nlink = 1

	// 失效父目录列表缓存（同 Create）
	n.fs.meta.InvalidateParent(childPath)

	return child, 0
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
	childPath := n.path + "/" + name

	// 缓存检查：TTL 内命中直接返回
	if entry, ok := n.fs.meta.Get(childPath); ok {
		mode := uint32(syscall.S_IFREG)
		if entry.IsDir {
			mode = uint32(syscall.S_IFDIR)
		}
		stable := gofuse.StableAttr{Mode: mode}
		var childOps gofuse.InodeEmbedder
		if entry.IsDir {
			childOps = &dirNode{fs: n.fs, path: childPath}
		} else {
			childOps = &fileNode{fs: n.fs, path: childPath, entry: entry}
		}
		child := n.EmbeddedInode().NewInode(ctx, childOps, stable)
		setEntryOut(out, child, entry)
		return child, 0
	}

	entry, err := n.fs.cloudFS.Stat(ctx, childPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, syscall.ENOENT
		}
		log.Printf("dirNode.Lookup %s: %v", childPath, err)
		return nil, syscall.EIO
	}
	n.fs.meta.Set(childPath, entry)

	mode := uint32(syscall.S_IFREG)
	if entry.IsDir {
		mode = uint32(syscall.S_IFDIR)
	}
	stable := gofuse.StableAttr{Mode: mode}
	var childOps gofuse.InodeEmbedder
	if entry.IsDir {
		childOps = &dirNode{fs: n.fs, path: childPath}
	} else {
		childOps = &fileNode{fs: n.fs, path: childPath, entry: entry}
	}
	child := n.EmbeddedInode().NewInode(ctx, childOps, stable)
	setEntryOut(out, child, entry)
	return child, 0
}

func (n *dirNode) Getattr(ctx context.Context, f gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = syscall.S_IFDIR | 0755
	out.Attr.Nlink = 2
	out.Attr.Size = 4096
	out.AttrValid = uint64(n.fs.opts.DirTTL / time.Second)
	return 0
}

func (n *dirNode) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	// 缓存检查
	if entries, ok := n.fs.meta.GetDir(n.path); ok {
		r := make([]fuse.DirEntry, 0, len(entries))
		for _, e := range entries {
			mode := uint32(syscall.S_IFREG)
			if e.IsDir {
				mode = uint32(syscall.S_IFDIR)
			}
			r = append(r, fuse.DirEntry{Name: e.Name, Mode: mode})
		}
		return gofuse.NewListDirStream(r), 0
	}

	entries, err := n.fs.cloudFS.ReadDir(ctx, n.path)
	if err != nil {
		return nil, syscall.EIO
	}
	n.fs.meta.SetDir(n.path, entries)

	// 预填充 metadata 缓存：ls 后内核会对每个文件发 LOOKUP，
	// 不预填充则 N 个文件 = N 次 Stat API（SingleThreaded 串行卡死）
	for _, e := range entries {
		childPath := n.path + "/" + e.Name
		n.fs.meta.Set(childPath, e)
	}

	r := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir {
			mode = uint32(syscall.S_IFDIR)
		}
		r = append(r, fuse.DirEntry{Name: e.Name, Mode: mode})
	}
	return gofuse.NewListDirStream(r), 0
}

func (n *dirNode) Rename(ctx context.Context, name string, newParent gofuse.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	oldPath := n.path + "/" + name

	// 获取目标目录路径
	var newPath string
	var newDirPath string
	if dp, ok := newParent.(*dirNode); ok {
		newPath = dp.path + "/" + newName
		newDirPath = dp.path
	} else {
		return syscall.EINVAL
	}

	// 同目录重命名
	if n.path == newDirPath {
		err := n.fs.cloudFS.Rename(ctx, oldPath, newPath)
		if err != nil {
			return syscall.EIO
		}
		n.fs.meta.Invalidate(oldPath)
		n.fs.meta.Invalidate(newPath)
		n.fs.meta.InvalidateParent(newDirPath)
		return 0
	}

	// 跨目录 rename：先尝试 Rename，失败且目标已存在时先删后移
	err := n.fs.cloudFS.Rename(ctx, oldPath, newPath)
	if err == nil {
		// Rename 成功
		n.fs.meta.Invalidate(oldPath)
		n.fs.meta.Invalidate(newPath)
		n.fs.meta.InvalidateParent(n.path)
		n.fs.meta.InvalidateParent(newDirPath)
		return 0
	}
	// Rename 失败 → 如果是目标已存在，先删再移
	_ = n.fs.cloudFS.Remove(ctx, newPath)
	err = n.fs.cloudFS.Rename(ctx, oldPath, newPath)
	if err != nil {
		return syscall.EIO
	}
	// 失效缓存
	n.fs.meta.Invalidate(oldPath)
	n.fs.meta.Invalidate(newPath)
	n.fs.meta.InvalidateParent(n.path)
	n.fs.meta.InvalidateParent(newDirPath)
	return 0
}

func (n *dirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	childPath := n.path + "/" + name
	if err := n.fs.cloudFS.Mkdir(ctx, childPath); err != nil {
		return nil, syscall.EIO
	}
	// 直接构造 entry，省掉一次 Stat API 调用
	entry := &mount.RemoteEntry{
		Path:  childPath,
		Name:  name,
		IsDir: true,
		Mode:  0755,
	}
	n.fs.meta.Set(childPath, entry)
	n.fs.meta.InvalidateParent(childPath)
	stable := gofuse.StableAttr{Mode: uint32(syscall.S_IFDIR)}
	child := n.EmbeddedInode().NewInode(ctx, &dirNode{fs: n.fs, path: childPath}, stable)
	setEntryOutDir(out, child)
	return child, 0
}

// Unlink 删除文件。
func (n *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	childPath := n.path + "/" + name
	if err := n.fs.cloudFS.Remove(ctx, childPath); err != nil {
		log.Printf("Unlink %s: %v", childPath, err)
		return syscall.EIO
	}
	n.fs.meta.Invalidate(childPath)       // 删文件自身的 metadata 缓存
	n.fs.meta.InvalidateParent(childPath) // 删父目录的 dir 缓存
	return 0
}

// Rmdir 删除空目录。
func (n *dirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	childPath := n.path + "/" + name
	if err := n.fs.cloudFS.Remove(ctx, childPath); err != nil {
		log.Printf("Rmdir %s: %v", childPath, err)
		return syscall.EIO
	}
	n.fs.meta.Invalidate(childPath)       // 删目录自身的 metadata 缓存
	n.fs.meta.InvalidateDir(childPath)    // 删目录的子项列表缓存
	n.fs.meta.InvalidateParent(childPath) // 删父目录的 dir 缓存
	return 0
}

// Create 在子目录中创建新文件。
func (n *dirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofuse.Inode, gofuse.FileHandle, uint32, syscall.Errno) {
	childPath := n.path + "/" + name

	if !n.fs.opts.EnableWrite {
		return nil, 0, 0, syscall.EROFS
	}

	// #9: 先 stat_remote，文件已存在则返回已有 inode（对齐 BaiduFS.Create）
	existing, err := n.fs.cloudFS.Stat(ctx, childPath)
	if err == nil && existing != nil {
		node := &fileNode{fs: n.fs, path: childPath, entry: existing}
		stable := gofuse.StableAttr{Mode: uint32(syscall.S_IFREG)}
		child := n.EmbeddedInode().NewInode(ctx, node, stable)
		setEntryOutFile(out, child, existing)
		// 创建 writer + 返回 writeFileHandle
		writer, werr := n.fs.cloudFS.CreateWriter(ctx, childPath)
		if werr != nil {
			return child, nil, 0, 0
		}
		var reader mount.RemoteReader
		if flags&syscall.O_TRUNC != 0 {
			_ = writer.Reset()
			node.entry.Size = 0
		} else if existing.Size > 0 {
			r, rerr := n.fs.cloudFS.OpenRead(ctx, childPath)
			if rerr == nil {
				reader = r
				writer.SetReader(r)
			}
		}
		wh := &writeFileHandle{node: node, writer: writer, reader: reader}
		node.writersMu.Lock()
		if node.writers == nil {
			node.writers = make(map[*writeFileHandle]struct{})
		}
		node.writers[wh] = struct{}{}
		node.writersMu.Unlock()
		return child, wh, 0, 0
	}

	writer, err := n.fs.cloudFS.CreateWriter(ctx, childPath)
	if err != nil {
		return nil, 0, 0, syscall.EIO
	}

	node := &fileNode{
		fs:    n.fs,
		path:  childPath,
		entry: &mount.RemoteEntry{Path: childPath, Name: name, IsDir: false, Mode: 0644},
	}

	stable := gofuse.StableAttr{Mode: uint32(syscall.S_IFREG)}
	child := n.EmbeddedInode().NewInode(ctx, node, stable)
	out.Attr.Ino = child.StableAttr().Ino
	out.Attr.Mode = uint32(syscall.S_IFREG) | 0644
	out.Attr.Nlink = 1

	// 失效父目录列表缓存（新文件需刷新 ls）
	n.fs.meta.InvalidateParent(childPath)

	wh := &writeFileHandle{node: node, writer: writer}
	node.writersMu.Lock()
	node.writers = make(map[*writeFileHandle]struct{})
	node.writers[wh] = struct{}{}
	node.writersMu.Unlock()
	return child, wh, 0, 0
}

// Mknod 兜底：某些系统调 mknod 而非 create 创建普通文件。
func (n *dirNode) Mknod(ctx context.Context, name string, mode uint32, rdev uint32, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	if mode&syscall.S_IFMT != syscall.S_IFREG {
		return nil, syscall.EINVAL
	}
	childPath := n.path + "/" + name
	if !n.fs.opts.EnableWrite {
		return nil, syscall.EROFS
	}
	node := &fileNode{
		fs:    n.fs,
		path:  childPath,
		entry: &mount.RemoteEntry{Path: childPath, Name: name, IsDir: false, Mode: 0644},
	}
	stable := gofuse.StableAttr{Mode: uint32(syscall.S_IFREG)}
	child := n.EmbeddedInode().NewInode(ctx, node, stable)
	out.Attr.Ino = child.StableAttr().Ino
	out.Attr.Mode = uint32(syscall.S_IFREG) | 0644
	out.Attr.Nlink = 1

	// 失效父目录列表缓存（同 Create）
	n.fs.meta.InvalidateParent(childPath)

	return child, 0
}

// ============================================================
// fileNode — per-inode 元数据（reader/writer 移到 FileHandle）
// ============================================================

type fileNode struct {
	gofuse.Inode
	fs    *baiduFSContext
	path  string
	entry *mount.RemoteEntry
	mu    sync.RWMutex // 保护 entry 字段

	// 并发写保护：跟踪活跃写 handle（同文件多 fd 写 → EBUSY）
	writers   map[*writeFileHandle]struct{}
	writersMu sync.Mutex
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
	n.mu.RLock()
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
	// 防御性 panic 捕获
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Open %s: PANIC recovered: %v", n.path, r)
			errno = syscall.EIO
		}
	}()

	log.Printf("Open %s: flags=0x%x", n.path, flags)

	// 检查是否为写打开
	writeMode := (flags & syscall.O_ACCMODE)
	isWrite := writeMode == syscall.O_WRONLY || writeMode == syscall.O_RDWR || flags&syscall.O_TRUNC != 0

	if isWrite {
		if !n.fs.opts.EnableWrite {
			return nil, 0, syscall.EROFS
		}
		// 并发写保护：同文件已有活跃 writer → EBUSY
		n.writersMu.Lock()
		if n.writers == nil {
			n.writers = make(map[*writeFileHandle]struct{})
		}
		if len(n.writers) > 0 {
			n.writersMu.Unlock()
			return nil, 0, syscall.EBUSY
		}
		// 对已存在文件的写打开：创建新 writer（覆盖写）
		writer, err := n.fs.cloudFS.CreateWriter(ctx, n.path)
		if err != nil {
			return nil, 0, syscall.EIO
		}
		var reader mount.RemoteReader
		// O_TRUNC：截断 staging 到 0，不注入 reader（不需要回填）
		if flags&syscall.O_TRUNC != 0 {
			_ = writer.Reset()
			n.mu.Lock()
			if n.entry != nil {
				n.entry.Size = 0
			}
			n.mu.Unlock()
		} else {
			n.mu.RLock()
			entryNotNil := n.entry != nil
			entrySize := int64(0)
			if n.entry != nil {
				entrySize = n.entry.Size
			}
			n.mu.RUnlock()
			if entryNotNil && entrySize > 0 {
				// 非 O_TRUNC 覆盖写：注入 reader 用于写洞回填
				r, rerr := n.fs.cloudFS.OpenRead(ctx, n.path)
				if rerr == nil {
					reader = r
					writer.SetReader(r)
				}
			}
		}
		wh := &writeFileHandle{node: n, writer: writer, reader: reader}
		n.writers[wh] = struct{}{}
		n.writersMu.Unlock()
		return wh, 0, 0
	}

	// 读打开
	log.Printf("Open %s: calling OpenRead...", n.path)
	r, err := n.fs.cloudFS.OpenRead(ctx, n.path)
	if err != nil {
		log.Printf("Open %s: OpenRead failed: %v", n.path, err)
		return nil, 0, syscall.EIO
	}
	log.Printf("Open %s: OpenRead ok, fsID=%d size=%d", n.path, r.FSID(), r.Size())
	return &readFileHandle{node: n, reader: r, streak: cache.NewStreak()}, 0, 0
}

func (h *readFileHandle) Read(ctx context.Context, buf []byte, off int64) (result fuse.ReadResult, errno syscall.Errno) {
	n := h.node
	// 防御性 panic 捕获：避免 FUSE 进程崩溃导致挂载点失效
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Read %s: PANIC recovered: %v", n.path, r)
			errno = syscall.EIO
		}
	}()

	// shutdown 检查：Server 正在关闭时立即返回 EIO，避免阻塞在 curl 下载上
	if n.fs.sched.IsShutdown() {
		return nil, syscall.EIO
	}

	size := len(buf)

	r := h.reader
	if r == nil {
		log.Printf("Read %s: reader is nil (EBADF)", n.path)
		return nil, syscall.EBADF
	}

	fsID := r.FSID()
	mtime := r.Mtime()

	h.streak.Advance(pathHash(n.path), off+int64(size))

	// 计算块边界（整块缓存，避免同块内多次 HTTP）
	blockSize := n.fs.opts.ReadBlockSize
	blockOff := off / blockSize * blockSize
	cacheKey := cache.BlockKeyFromPath(n.fs.opts.AccountKey, fsID, r.Size(), mtime, blockOff)
	startInBlock := off - blockOff

	// BlockCache 命中 → 检查是否跨块
	if cached, ok := n.fs.blocks.Get(cacheKey); ok {
		endInBlock := startInBlock + int64(size)
		if endInBlock <= int64(len(cached)) {
			n.trackDownload(int64(size))
			return fuse.ReadResultData(cached[startInBlock:endInBlock]), 0
		}
		// 跨块：拉第二个块并拼接
		if result, ok, fetchErr := readCrossBlock(ctx, n.fs, r, n.path, fsID, mtime, blockOff, blockSize, off, size, cached, startInBlock); ok {
			if fetchErr != nil {
				return nil, syscall.EIO
			}
			n.trackDownload(int64(size))
			return result, 0
		}
	}

	// DiskCache 命中 → 填充 BlockCache + 检查跨块
	if cached, ok := n.fs.disk.Get(cacheKey); ok {
		n.fs.blocks.Put(cacheKey, cached)
		endInBlock := startInBlock + int64(size)
		if endInBlock <= int64(len(cached)) {
			n.trackDownload(int64(size))
			return fuse.ReadResultData(cached[startInBlock:endInBlock]), 0
		}
		// 跨块：拉第二个块并拼接
		if result, ok, fetchErr := readCrossBlock(ctx, n.fs, r, n.path, fsID, mtime, blockOff, blockSize, off, size, cached, startInBlock); ok {
			if fetchErr != nil {
				return nil, syscall.EIO
			}
			n.trackDownload(int64(size))
			return result, 0
		}
	}

	// Cache miss → 拉取整块 [blockOff, blockOff+blockSize)
	fetchEnd := blockOff + blockSize
	if fetchEnd > r.Size() {
		fetchEnd = r.Size()
	}
	fetchSize := int(fetchEnd - blockOff)
	if fetchSize <= 0 {
		log.Printf("Read %s: fetchSize=%d (EIO)", n.path, fetchSize)
		return nil, syscall.EIO
	}

	log.Printf("Read %s: off=%d size=%d blockOff=%d fetchSize=%d fsID=%d", n.path, off, size, blockOff, fetchSize, fsID)
	data, err := r.ReadAt(ctx, blockOff, fetchSize)
	if err != nil {
		log.Printf("Read %s: ReadAt error: %v", n.path, err)
		// dlink 403/416 → 自动刷新：多次重试获取不同 CDN 节点
		if err == download.ErrCDNForbidden || err == download.ErrRangeNotSatisfiable {
			for refreshAttempt := 0; refreshAttempt < 3; refreshAttempt++ {
				// shutdown 检查：避免在关闭期间无限重试
				if n.fs.sched.IsShutdown() {
					return nil, syscall.EIO
				}
				if refreshAttempt > 0 {
					log.Printf("Read %s: refresh attempt %d", n.path, refreshAttempt+1)
					time.Sleep(time.Duration(200*refreshAttempt) * time.Millisecond)
				}
				log.Printf("Read %s: dlink expired (%v), refreshing... (attempt %d)", n.path, err, refreshAttempt+1)
				h.reader.Close()
				r2, err2 := n.fs.cloudFS.OpenRead(ctx, n.path)
				if err2 != nil {
					log.Printf("Read %s: OpenRead refresh error: %v", n.path, err2)
					continue
				}
				h.reader = r2
				data, err = r2.ReadAt(ctx, blockOff, fetchSize)
				if err == nil {
					n.fs.blocks.Put(cacheKey, data)
					n.fs.disk.Put(cacheKey, data)
					startInBlock := off - blockOff
					endInBlock := startInBlock + int64(size)
					if endInBlock <= int64(len(data)) {
						n.trackDownload(int64(size))
						return fuse.ReadResultData(data[startInBlock:endInBlock]), 0
					}
					// 跨块：拉第二个块并拼接
					if result, ok, fetchErr := readCrossBlock(ctx, n.fs, r2, n.path, fsID, mtime, blockOff, blockSize, off, size, data, startInBlock); ok {
						if fetchErr != nil {
							return nil, syscall.EIO
						}
						n.trackDownload(int64(size))
						return result, 0
					}
					return nil, syscall.EIO
				}
				// 尝试 RefreshDlink（内部也有多次重试）
				if err == download.ErrCDNForbidden {
					log.Printf("Read %s: trying RefreshDlink", n.path)
					if refreshErr := r2.RefreshDlink(ctx, n.path); refreshErr == nil {
						data, err = r2.ReadAt(ctx, blockOff, fetchSize)
						if err == nil {
							n.fs.blocks.Put(cacheKey, data)
							n.fs.disk.Put(cacheKey, data)
							startInBlock := off - blockOff
							endInBlock := startInBlock + int64(size)
							if endInBlock <= int64(len(data)) {
								n.trackDownload(int64(size))
								return fuse.ReadResultData(data[startInBlock:endInBlock]), 0
							}
							// 跨块：拉第二个块并拼接
							if result, ok, fetchErr := readCrossBlock(ctx, n.fs, r2, n.path, fsID, mtime, blockOff, blockSize, off, size, data, startInBlock); ok {
								if fetchErr != nil {
									return nil, syscall.EIO
								}
								n.trackDownload(int64(size))
								return result, 0
							}
							return nil, syscall.EIO
						}
					}
				}
			}
			log.Printf("Read %s: all refresh attempts failed", n.path)
		}
		return nil, syscall.EIO
	}

	// 写入 BlockCache + DiskCache（整块）
	n.fs.blocks.Put(cacheKey, data)
	n.fs.disk.Put(cacheKey, data)

	// 预读：顺序读时异步拉取后续块，减少下次 Read 的等待
	if h.streak.SequentialCount() >= 2 {
		go n.prefetch(r, fsID, mtime, blockOff+blockSize, blockSize)
	}

	// 从整块中切出请求区间
	endInBlock := startInBlock + int64(size)
	if endInBlock <= int64(len(data)) {
		n.trackDownload(int64(size))
		return fuse.ReadResultData(data[startInBlock:endInBlock]), 0
	}

	// 跨块：拉第二个块并拼接
	if result, ok, fetchErr := readCrossBlock(ctx, n.fs, r, n.path, fsID, mtime, blockOff, blockSize, off, size, data, startInBlock); ok {
		if fetchErr != nil {
			return nil, syscall.EIO
		}
		n.trackDownload(int64(size))
		return result, 0
	}

	// 兜底：数据不足
	return nil, syscall.EIO
}

// trackDownload 记录下载字节（仅用户实际读到的，不含预读/重试）。
func (n *fileNode) trackDownload(bytes int64) {
	if n.fs.onTrackDownload != nil {
		n.fs.onTrackDownload(n.path, bytes)
	}
}

// readCrossBlock 处理跨块边界读取：拉取第二个块并拼接返回。
// 独立函数，可供 readFileHandle 和 writeFileHandle 共用。
// 返回值: (结果, ok, fetchErr)
//
//	ok=true: 成功获取完整或部分数据
//	ok=false, fetchErr=nil: 不需要跨块（调用者继续原逻辑）
//	ok=true, fetchErr!=nil: 部分数据但第二块获取失败（调用者应返回 EIO）
func readCrossBlock(ctx context.Context, fs *baiduFSContext, r mount.RemoteReader, nodePath string, fsID, mtime, blockOff, blockSize, off int64, size int, firstBlockData []byte, startInBlock int64) (fuse.ReadResult, bool, error) {
	needFromSecond := int64(size) - (int64(len(firstBlockData)) - startInBlock)
	if needFromSecond <= 0 {
		return nil, false, nil // 不需要第二个块
	}

	// 计算第二个块的偏移和 key
	secondBlockOff := blockOff + blockSize
	if secondBlockOff >= r.Size() {
		// 第二个块超出文件范围 → EOF，返回第一个块的剩余部分
		combined := make([]byte, 0, int64(len(firstBlockData))-startInBlock)
		combined = append(combined, firstBlockData[startInBlock:]...)
		return fuse.ReadResultData(combined), true, nil
	}
	secondKey := cache.BlockKeyFromPath(fs.opts.AccountKey, fsID, r.Size(), mtime, secondBlockOff)

	// 尝试从 BlockCache 获取第二个块
	if cached2, ok := fs.blocks.Get(secondKey); ok {
		take := needFromSecond
		if take > int64(len(cached2)) {
			take = int64(len(cached2))
		}
		combined := make([]byte, 0, size)
		combined = append(combined, firstBlockData[startInBlock:]...)
		combined = append(combined, cached2[:take]...)
		return fuse.ReadResultData(combined), true, nil
	}

	// 尝试从 DiskCache 获取第二个块
	if cached2, ok := fs.disk.Get(secondKey); ok {
		fs.blocks.Put(secondKey, cached2)
		take := needFromSecond
		if take > int64(len(cached2)) {
			take = int64(len(cached2))
		}
		combined := make([]byte, 0, size)
		combined = append(combined, firstBlockData[startInBlock:]...)
		combined = append(combined, cached2[:take]...)
		return fuse.ReadResultData(combined), true, nil
	}

	// 从远端拉取第二个块
	fetchEnd2 := secondBlockOff + blockSize
	if fetchEnd2 > r.Size() {
		fetchEnd2 = r.Size()
	}
	fetchSize2 := int(fetchEnd2 - secondBlockOff)
	if fetchSize2 <= 0 {
		combined := make([]byte, 0, int64(len(firstBlockData))-startInBlock)
		combined = append(combined, firstBlockData[startInBlock:]...)
		return fuse.ReadResultData(combined), true, nil
	}
	data2, err := r.ReadAt(ctx, secondBlockOff, fetchSize2)
	if err != nil {
		log.Printf("readCrossBlock %s: second block fetch failed: %v", nodePath, err)
		// 第二块拉取失败 → 返回第一个块的剩余部分 + 错误标记（调用者应返回 EIO）
		combined := make([]byte, 0, int64(len(firstBlockData))-startInBlock)
		combined = append(combined, firstBlockData[startInBlock:]...)
		return fuse.ReadResultData(combined), true, err
	}
	fs.blocks.Put(secondKey, data2)
	fs.disk.Put(secondKey, data2)

	take := needFromSecond
	if take > int64(len(data2)) {
		take = int64(len(data2))
	}
	combined := make([]byte, 0, size)
	combined = append(combined, firstBlockData[startInBlock:]...)
	combined = append(combined, data2[:take]...)
	return fuse.ReadResultData(combined), true, nil
}

// readFromWriter 从 writer 读取：先查 staging，不足部分从远端回源。
func (h *writeFileHandle) readFromWriter(ctx context.Context, off int64, size int) (fuse.ReadResult, syscall.Errno) {
	n := h.node
	w := h.writer
	// 从 staging 读取
	data, err := w.ReadFrom(ctx, off, size)
	if err != nil {
		return nil, syscall.EIO
	}
	// 如果 staging 数据不足，从远端 reader 回源
	if len(data) < size {
		// 获取文件总大小：取 max(entry.Size, stagingSize) 以处理 write-extend 场景
		var totalSize int64
		n.mu.RLock()
		if n.entry != nil {
			totalSize = n.entry.Size
		}
		stagingSize := w.Size()
		n.mu.RUnlock()
		if stagingSize > totalSize {
			totalSize = stagingSize
		}

		need := size - len(data)
		start := off + int64(len(data))
		if start >= totalSize {
			// 超出文件范围 → 返回已有数据（POSIX 允许 EOF 短读）
			return fuse.ReadResultData(data), 0
		}
		end := start + int64(need)
		if end > totalSize {
			end = totalSize
		}
		fetchLen := int(end - start)

		// 先查 BlockCache（避免重复 HTTP 回源）
		r := h.reader
		if r == nil {
			// reader 不可用（OpenRead 失败），无法回源
			if start+int64(len(data)) < totalSize {
				return nil, syscall.EIO
			}
			return fuse.ReadResultData(data), 0
		}
		fsID := r.FSID()
		mtime := r.Mtime()
		blockSize := n.fs.opts.ReadBlockSize
		blockOff := start / blockSize * blockSize
		cacheKey := cache.BlockKeyFromPath(n.fs.opts.AccountKey, fsID, r.Size(), mtime, blockOff)
		if cached, ok := n.fs.blocks.Get(cacheKey); ok {
			startInBlock := start - blockOff
			endInBlock := startInBlock + int64(fetchLen)
			if endInBlock <= int64(len(cached)) {
				data = append(data, cached[startInBlock:endInBlock]...)
				return fuse.ReadResultData(data), 0
			}
		}

		// Cache miss → 从远端回源（单次 HTTP：结果同时用于返回和缓存）
		fetchBlockEnd := blockOff + blockSize
		if fetchBlockEnd > r.Size() {
			fetchBlockEnd = r.Size()
		}
		if fetchBlockEnd > blockOff {
			blockData, bErr := h.reader.ReadAt(ctx, blockOff, int(fetchBlockEnd-blockOff))
			if bErr == nil {
				n.fs.blocks.Put(cacheKey, blockData)
				n.fs.disk.Put(cacheKey, blockData)
				// 从整块中切出需要的区间
				startInBlock := start - blockOff
				endInBlock := startInBlock + int64(fetchLen)
				if endInBlock <= int64(len(blockData)) {
					data = append(data, blockData[startInBlock:endInBlock]...)
					return fuse.ReadResultData(data), 0
				}
				// 跨块：取第一个块的剩余部分
				data = append(data, blockData[startInBlock:]...)
				remaining := fetchLen - len(blockData) + int(startInBlock)
				if remaining > 0 {
					secondBlockOff := blockOff + blockSize
					if secondBlockOff < r.Size() {
						secondKey := cache.BlockKeyFromPath(n.fs.opts.AccountKey, fsID, r.Size(), mtime, secondBlockOff)
						fetchEnd2 := secondBlockOff + blockSize
						if fetchEnd2 > r.Size() {
							fetchEnd2 = r.Size()
						}
						data2, err2 := h.reader.ReadAt(ctx, secondBlockOff, int(fetchEnd2-secondBlockOff))
						if err2 == nil {
							n.fs.blocks.Put(secondKey, data2)
							n.fs.disk.Put(secondKey, data2)
							take := remaining
							if take > len(data2) {
								take = len(data2)
							}
							data = append(data, data2[:take]...)
						} else {
							// 第二块拉取失败 → EIO（避免短读被内核当 EOF）
							return nil, syscall.EIO
						}
					} else {
						// 第二块超出文件范围 → 文件末尾，返回已有数据（POSIX 合法短读）
						return fuse.ReadResultData(data), 0
					}
				}
				return fuse.ReadResultData(data), 0
			}
		}
		// 远端回源也失败 → EIO（避免短读误判 EOF）
		if len(data) < size {
			return nil, syscall.EIO
		}
	}
	return fuse.ReadResultData(data), 0
}

// prefetch 异步预读下一块到 BlockCache（顺序读优化）。
// 在 goroutine 中运行，使用独立 context 不依赖调用者。
func (n *fileNode) prefetch(r mount.RemoteReader, fsID, mtime, blockOff, blockSize int64) {
	// 跳过已缓存的块
	cacheKey := cache.BlockKeyFromPath(n.fs.opts.AccountKey, fsID, r.Size(), mtime, blockOff)
	if _, ok := n.fs.blocks.Get(cacheKey); ok {
		return
	}
	// 跳过超出文件范围的块
	if blockOff >= r.Size() {
		return
	}
	fetchEnd := blockOff + blockSize
	if fetchEnd > r.Size() {
		fetchEnd = r.Size()
	}
	fetchSize := int(fetchEnd - blockOff)
	// 使用独立 context + 超时，不依赖调用者（避免 Open 的 ctx 已取消）
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	data, err := r.ReadAt(ctx, blockOff, fetchSize)
	if err != nil {
		return // 预读失败静默忽略，下次 Read 会重试
	}
	n.fs.blocks.Put(cacheKey, data)
	n.fs.disk.Put(cacheKey, data)
}

func (n *fileNode) Release(ctx context.Context, f gofuse.FileHandle) syscall.Errno {
	// 清理 per-handle 资源
	switch h := f.(type) {
	case *readFileHandle:
		if h.reader != nil {
			h.reader.Close()
		}
	case *writeFileHandle:
		// 从活跃写 handle 集合中移除
		n.writersMu.Lock()
		if n.writers != nil {
			delete(n.writers, h)
		}
		n.writersMu.Unlock()
		if h.writer != nil {
			// Close 内部检查 dirty：Flush 已上传则只清理 staging，否则上传
			if err := h.writer.Close(context.Background()); err != nil {
				log.Printf("Release %s: upload failed: %v", n.path, err)
				return syscall.EIO
			}
		}
		if h.reader != nil {
			h.reader.Close()
		}
	}
	return 0
}

// Setattr 处理文件属性变更（truncate 等）。
func (n *fileNode) Setattr(ctx context.Context, f gofuse.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	// 处理 truncate
	if in.Valid&(fuse.FATTR_SIZE) != 0 {
		newSize := int64(in.Size)
		// 如果有活跃写 handle → 截断 staging 文件
		if wh, ok := f.(*writeFileHandle); ok && wh != nil && wh.writer != nil {
			if err := wh.writer.Truncate(newSize); err != nil {
				return syscall.EIO
			}
			n.mu.Lock()
			n.entry.Size = newSize
			n.mu.Unlock()
		} else {
			// 无活跃写 handle → 只更新元数据大小（等下次 Open 写时生效）
			n.mu.Lock()
			n.entry.Size = newSize
			n.mu.Unlock()
		}
	}

	// 保留 mtime：rsync 等工具写入后会 utimensat，不记录会导致全量重传死循环
	if in.Valid&(fuse.FATTR_MTIME) != 0 {
		n.mu.Lock()
		if n.entry != nil {
			n.entry.MtimeSec = int64(in.Mtime)
		}
		n.mu.Unlock()
	}

	// 刷新当前属性
	n.mu.RLock()
	entry := n.entry
	n.mu.RUnlock()
	if entry != nil {
		out.Attr.Size = uint64(entry.Size)
		out.Attr.Mtime = uint64(entry.MtimeSec)
	}
	out.Attr.Mode = syscall.S_IFREG | 0644
	out.Attr.Nlink = 1
	return 0
}

// Write 写入数据到文件（通过 RemoteWriter → staging）。
func (h *writeFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n := h.node
	if err := h.writer.Write(ctx, off, data); err != nil {
		return 0, syscall.EIO
	}
	// 更新 entry.Size（Getattr 依赖此值，否则 cat 看到 size=0 不读取）
	newEnd := off + int64(len(data))
	n.mu.Lock()
	if newEnd > n.entry.Size {
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
	if err := w.Sync(context.Background()); err != nil {
		return syscall.EIO
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
	if err := w.Sync(context.Background()); err != nil {
		return syscall.EIO
	}
	return 0
}

// ============================================================
// inodeManager — 内存 inode 表
// ============================================================

type inodeManager struct {
	mu     sync.RWMutex
	byPath map[string]*inodeEntry
	byID   map[uint64]*inodeEntry
	nextID uint64
	ttl    time.Duration
	done   chan struct{} // 用于停止后台清理 goroutine
}

type inodeEntry struct {
	id       uint64
	path     string
	nodeID   uint64
	expireAt time.Time
}

func newInodeManager(ttl time.Duration) *inodeManager {
	m := &inodeManager{
		byPath: make(map[string]*inodeEntry),
		byID:   make(map[uint64]*inodeEntry),
		nextID: 2,
		ttl:    ttl,
		done:   make(chan struct{}),
	}
	go m.cleanupLoop()
	return m
}

func (m *inodeManager) getByPath(path string) (*inodeEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.byPath[path]
	if !ok || time.Now().After(e.expireAt) {
		return nil, false
	}
	return e, true
}

func (m *inodeManager) getOrCreate(path string, nodeID uint64) *inodeEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 惰性清理：每次写入时顺便清除过期条目
	now := time.Now()
	if len(m.byPath) > 100 { // 只在条目较多时清理，避免频繁扫描
		for k, e := range m.byPath {
			if now.After(e.expireAt) {
				delete(m.byPath, k)
				delete(m.byID, e.id)
			}
		}
	}

	if e, ok := m.byPath[path]; ok && now.Before(e.expireAt) {
		return e
	}
	id := m.nextID
	m.nextID++
	e := &inodeEntry{id: id, path: path, nodeID: nodeID, expireAt: now.Add(m.ttl)}
	m.byPath[path] = e
	m.byID[id] = e
	return e
}

// cleanupLoop 定时清理过期 inode 条目。
func (m *inodeManager) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.cleanup()
		case <-m.done:
			return
		}
	}
}

// cleanup 清理所有过期的 inode 条目。
func (m *inodeManager) cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	cleaned := 0
	for path, e := range m.byPath {
		if now.After(e.expireAt) {
			delete(m.byPath, path)
			delete(m.byID, e.id)
			cleaned++
		}
	}
	if cleaned > 0 {
		log.Printf("inodeManager cleanup: removed %d expired entries", cleaned)
	}
}

// Stop 停止后台清理 goroutine。
func (m *inodeManager) Stop() {
	close(m.done)
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

func (n *BaiduFS) createChild(ctx context.Context, path string, entry *mount.RemoteEntry, out *fuse.EntryOut) *gofuse.Inode {
	mode := uint32(syscall.S_IFREG)
	if entry.IsDir {
		mode = uint32(syscall.S_IFDIR)
	}
	stable := gofuse.StableAttr{Mode: mode}
	if entry.IsDir {
		child := n.NewInode(ctx, &dirNode{fs: n.fs, path: path}, stable)
		setEntryOutDir(out, child)
		return child
	}
	child := n.NewInode(ctx, &fileNode{fs: n.fs, path: path, entry: entry}, stable)
	setEntryOutFile(out, child, entry)
	return child
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

func setEntryOutDir(out *fuse.EntryOut, child *gofuse.Inode) {
	out.Attr.Ino = child.StableAttr().Ino
	out.Attr.Mode = uint32(syscall.S_IFDIR) | 0755
	out.Attr.Nlink = 2
}

func setEntryOutFile(out *fuse.EntryOut, child *gofuse.Inode, entry *mount.RemoteEntry) {
	out.Attr.Ino = child.StableAttr().Ino
	out.Attr.Mode = uint32(syscall.S_IFREG) | 0644
	out.Attr.Size = uint64(entry.Size)
	out.Attr.Mtime = uint64(entry.MtimeSec)
	out.Attr.Nlink = 1
}

// pathHash 将路径转为稳定的 uint64 用于 streak inode 标识。
func pathHash(p string) uint64 {
	var h uint64
	for i := 0; i < len(p); i++ {
		h = h*31 + uint64(p[i])
	}
	return h
}
