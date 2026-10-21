package fuse

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"baidupcs-fuse/adapter"
	"baidupcs-fuse/cache"
	"baidupcs-fuse/mount"
	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// handlerFS 是可注入错误、带文件内容与写会话的 CloudFS，用于验证 FUSE 层
// 的权限/类型/幂等/错误映射，而不是只验证 happy path。
type handlerFS struct {
	mu       sync.Mutex
	entries  map[string]*mount.RemoteEntry
	data     map[string][]byte
	nilPaths map[string]bool
	statErrs map[string]error
	dirErrs  map[string]error

	mkdirErr  error
	removeErr error
	renameErr error
	quotaErr  error
	openErr   error
	writerErr error

	statCalls  int
	renames    [][2]string
	removes    []string
	writerMake func(path string) (mount.RemoteWriter, error)
	quota      mount.FSStats
	dirHook    chan string
}

func newHandlerFS() *handlerFS {
	return &handlerFS{
		entries:  make(map[string]*mount.RemoteEntry),
		data:     make(map[string][]byte),
		nilPaths: make(map[string]bool),
		statErrs: make(map[string]error),
		dirErrs:  make(map[string]error),
		quota:    mount.FSStats{Total: 1000, Used: 400, Free: 600},
	}
}

func (m *handlerFS) addDir(path string) {
	m.entries[path] = &mount.RemoteEntry{FSID: int64(len(m.entries) + 10), Path: path, Name: baseName(path), IsDir: true, Mode: 0755}
}

func (m *handlerFS) addFile(path string, content []byte) {
	m.entries[path] = &mount.RemoteEntry{FSID: int64(len(m.entries) + 100), Path: path, Name: baseName(path), Size: int64(len(content)), Mode: 0644}
	m.data[path] = append([]byte(nil), content...)
}

func baseName(p string) string {
	i := strings.LastIndex(p, "/")
	return p[i+1:]
}

func parentDir(p string) string {
	i := strings.LastIndex(p, "/")
	if i <= 0 {
		return "/"
	}
	return p[:i]
}

func (m *handlerFS) Stat(_ context.Context, path string) (*mount.RemoteEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statCalls++
	if err, ok := m.statErrs[path]; ok {
		return nil, err
	}
	if m.nilPaths[path] {
		return nil, nil
	}
	if path == "/" {
		return &mount.RemoteEntry{FSID: 1, Path: "/", IsDir: true, Mode: 0755}, nil
	}
	e, ok := m.entries[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	cp := *e
	return &cp, nil
}

func (m *handlerFS) ReadDir(_ context.Context, path string) ([]*mount.RemoteEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dirHook != nil {
		select {
		case m.dirHook <- path:
		default:
		}
	}
	if err, ok := m.dirErrs[path]; ok {
		return nil, err
	}
	if path != "/" {
		e, ok := m.entries[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		if !e.IsDir {
			return nil, syscall.ENOTDIR
		}
	}
	var out []*mount.RemoteEntry
	for p, e := range m.entries {
		if parentDir(p) != path || p == path {
			continue
		}
		cp := *e
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *handlerFS) OpenRead(_ context.Context, path string) (mount.RemoteReader, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.openErr != nil {
		return nil, m.openErr
	}
	e, ok := m.entries[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	if e.IsDir {
		return nil, syscall.EISDIR
	}
	return &handlerReader{data: append([]byte(nil), m.data[path]...), fsid: e.FSID, mtime: e.MtimeSec}, nil
}

func (m *handlerFS) StatFS(context.Context) (*mount.FSStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.quotaErr != nil {
		return nil, m.quotaErr
	}
	cp := m.quota
	return &cp, nil
}

func (m *handlerFS) Mkdir(_ context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mkdirErr != nil {
		return m.mkdirErr
	}
	m.entries[path] = &mount.RemoteEntry{FSID: int64(len(m.entries) + 10), Path: path, Name: baseName(path), IsDir: true, Mode: 0755}
	return nil
}

func (m *handlerFS) Remove(_ context.Context, paths ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removeErr != nil {
		return m.removeErr
	}
	for _, p := range paths {
		delete(m.entries, p)
		delete(m.data, p)
		m.removes = append(m.removes, p)
	}
	return nil
}

func (m *handlerFS) Rename(_ context.Context, from, to string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.renameErr != nil {
		return m.renameErr
	}
	if e, ok := m.entries[from]; ok {
		e.Path = to
		e.Name = baseName(to)
		m.entries[to] = e
		delete(m.entries, from)
	}
	if d, ok := m.data[from]; ok {
		m.data[to] = d
		delete(m.data, from)
	}
	m.renames = append(m.renames, [2]string{from, to})
	return nil
}

func (m *handlerFS) CreateWriter(_ context.Context, path string) (mount.RemoteWriter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writerErr != nil {
		return nil, m.writerErr
	}
	if m.writerMake != nil {
		return m.writerMake(path)
	}
	return &memWriter{}, nil
}

// handlerReader 返回可预测的内容，并记录读取参数。
type handlerReader struct {
	data     []byte
	fsid     int64
	mtime    int64
	epoch    uint64
	readErr  error
	short    bool
	readAt   int
	closed   bool
	onReadAt func()
}

func (r *handlerReader) ReadAt(_ context.Context, off int64, size int) ([]byte, error) {
	r.readAt++
	if r.onReadAt != nil {
		r.onReadAt()
	}
	if r.readErr != nil {
		return nil, r.readErr
	}
	if off < 0 || off >= int64(len(r.data)) {
		return nil, nil
	}
	end := off + int64(size)
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	if r.short && end > off+1 {
		end = off + 1
	}
	return append([]byte(nil), r.data[off:end]...), nil
}
func (r *handlerReader) Size() int64  { return int64(len(r.data)) }
func (r *handlerReader) FSID() int64  { return r.fsid }
func (r *handlerReader) Mtime() int64 { return r.mtime }
func (r *handlerReader) Reload(context.Context) error {
	return nil
}
func (r *handlerReader) Close() error { r.closed = true; return nil }
func (r *handlerReader) CacheEpoch() uint64 {
	return r.epoch
}

// memWriter 是内存 RemoteWriter，记录 staging 内容、大小与生命周期错误。
type memWriter struct {
	data     []byte
	target   int64
	reader   mount.RemoteReader
	dirty    bool
	closed   bool
	syncErr  error
	closeErr error
	writeErr error
	truncErr error
	path     string
}

func (w *memWriter) Write(_ context.Context, off int64, data []byte) error {
	if w.writeErr != nil {
		return w.writeErr
	}
	if end := off + int64(len(data)); end > int64(len(w.data)) {
		w.data = append(w.data, make([]byte, end-int64(len(w.data)))...)
	}
	copy(w.data[off:], data)
	w.dirty = true
	return nil
}
func (w *memWriter) Close(context.Context) error {
	w.closed = true
	return w.closeErr
}
func (w *memWriter) Sync(context.Context) error {
	if w.syncErr != nil {
		return w.syncErr
	}
	w.dirty = false
	return nil
}
func (w *memWriter) Truncate(size int64) error {
	if w.truncErr != nil {
		return w.truncErr
	}
	if size < int64(len(w.data)) {
		w.data = w.data[:size]
	} else {
		w.data = append(w.data, make([]byte, size-int64(len(w.data)))...)
	}
	w.target = size
	w.dirty = true
	return nil
}
func (w *memWriter) TargetSize() int64        { return w.target }
func (w *memWriter) SetTargetSize(size int64) { w.target = size }
func (w *memWriter) MarkDirty()               { w.dirty = true }
func (w *memWriter) ReadFrom(_ context.Context, off int64, size int) ([]byte, error) {
	if off >= int64(len(w.data)) {
		return nil, nil
	}
	end := off + int64(size)
	if end > int64(len(w.data)) {
		end = int64(len(w.data))
	}
	return append([]byte(nil), w.data[off:end]...), nil
}
func (w *memWriter) Size() int64 { return int64(len(w.data)) }
func (w *memWriter) Reset() error {
	w.data = nil
	w.target = 0
	w.dirty = true
	return nil
}
func (w *memWriter) Dirty() bool                    { return w.dirty }
func (w *memWriter) SetReader(r mount.RemoteReader) { w.reader = r }

// newHandlerFSContext 建立带根 inode 的 FUSE 上下文，使 remotePath 能沿 inode 树解析。
func newHandlerFSContext(t *testing.T, cloud mount.CloudFS) (*BaiduFS, *baiduFSContext) {
	t.Helper()
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	opts.AccountKey = "acct"
	opts.EnableWrite = true
	opts.ReadBlockSize = 4
	opts.DiskCacheSize = 1 << 20
	ctx := newContext(opts, cloud)
	root := &BaiduFS{fs: ctx, rootPath: "/"}
	_ = gofuse.NewNodeFS(root, &gofuse.Options{})
	ctx.rootInode = root.EmbeddedInode()
	ctx.rootRemote = "/"
	return root, ctx
}

func attachDir(t *testing.T, ctx *baiduFSContext, parent *gofuse.Inode, name, path string) (*dirNode, *gofuse.Inode) {
	t.Helper()
	n := &dirNode{fs: ctx, path: path}
	inode := parent.NewInode(context.Background(), n, gofuse.StableAttr{Ino: pathHash(path), Mode: syscall.S_IFDIR})
	if !parent.AddChild(name, inode, false) {
		t.Fatalf("AddChild dir %s failed", name)
	}
	return n, inode
}

func attachFile(t *testing.T, ctx *baiduFSContext, parent *gofuse.Inode, name, path string, entry *mount.RemoteEntry) (*fileNode, *gofuse.Inode) {
	t.Helper()
	n := &fileNode{fs: ctx, path: path, entry: entry, entryAt: time.Now()}
	inode := parent.NewInode(context.Background(), n, stableAttr(entry))
	if !parent.AddChild(name, inode, false) {
		t.Fatalf("AddChild file %s failed", name)
	}
	return n, inode
}

func drainDir(t *testing.T, stream gofuse.DirStream) map[string]uint32 {
	t.Helper()
	if stream == nil {
		return nil
	}
	defer stream.Close()
	got := make(map[string]uint32)
	for stream.HasNext() {
		e, errno := stream.Next()
		if errno != 0 {
			t.Fatalf("DirStream.Next errno=%v", errno)
		}
		got[e.Name] = e.Mode
	}
	return got
}

// 目的：根/子目录的 Getattr、Readdir、Lookup 必须返回正确类型与大小，并走负缓存；
// 远端返回 nil entry 时必须报 EIO，而不是把空指针当作成功。
func TestHandlers_RootAndDirLookupReaddir(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addFile("/a.txt", []byte("hello"))
	cloud.addDir("/dir")
	cloud.addFile("/dir/b.txt", []byte("world"))
	cloud.nilPaths["/nil"] = true
	root, fsctx := newHandlerFSContext(t, cloud)

	var rootAttr fuse.AttrOut
	if errno := root.Getattr(ctx, nil, &rootAttr); errno != 0 {
		t.Fatalf("root Getattr errno=%v", errno)
	}
	if rootAttr.Mode != syscall.S_IFDIR|0755 || rootAttr.Size != 4096 {
		t.Fatalf("root attr=%+v", rootAttr.Attr)
	}

	stream, errno := root.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("root Readdir errno=%v", errno)
	}
	names := drainDir(t, stream)
	if len(names) != 2 || names["a.txt"] != syscall.S_IFREG || names["dir"] != syscall.S_IFDIR {
		t.Fatalf("root entries=%v", names)
	}

	var out fuse.EntryOut
	inode, errno := root.Lookup(ctx, "a.txt", &out)
	if errno != 0 || inode == nil {
		t.Fatalf("Lookup a.txt inode=%v errno=%v", inode, errno)
	}
	if out.Attr.Size != 5 || out.Attr.Mode != syscall.S_IFREG|0644 {
		t.Fatalf("a.txt attr=%+v", out.Attr)
	}
	before := cloud.statCalls
	if _, errno := root.Lookup(ctx, "a.txt", &out); errno != 0 || cloud.statCalls != before {
		t.Fatalf("cached lookup errno=%v statCalls=%d->%d", errno, before, cloud.statCalls)
	}
	if _, errno := root.Lookup(ctx, "missing", &out); errno != syscall.ENOENT {
		t.Fatalf("missing errno=%v", errno)
	}
	before = cloud.statCalls
	if _, errno := root.Lookup(ctx, "missing", &out); errno != syscall.ENOENT || cloud.statCalls != before {
		t.Fatalf("negative cache errno=%v statCalls=%d->%d", errno, before, cloud.statCalls)
	}
	if _, errno := root.Lookup(ctx, "nil", &out); errno != syscall.EIO {
		t.Fatalf("nil entry errno=%v, want EIO", errno)
	}

	dir, _ := attachDir(t, fsctx, root.EmbeddedInode(), "dir", "/dir")
	var dirAttr fuse.AttrOut
	if errno := dir.Getattr(ctx, nil, &dirAttr); errno != 0 || dirAttr.Mode != syscall.S_IFDIR|0755 {
		t.Fatalf("dir Getattr errno=%v attr=%+v", errno, dirAttr.Attr)
	}
	stream, errno = dir.Readdir(ctx)
	if errno != 0 || len(drainDir(t, stream)) != 1 {
		t.Fatalf("dir Readdir errno=%v", errno)
	}
	if _, errno := dir.Lookup(ctx, "b.txt", &out); errno != 0 || out.Attr.Size != 5 {
		t.Fatalf("dir Lookup errno=%v attr=%+v", errno, out.Attr)
	}

	// dirStream 必须跳过 nil 项，避免 Readdir 中空指针崩溃。
	s := dirStream([]*mount.RemoteEntry{nil, {Name: "f", Size: 1}, {Name: "d", IsDir: true}})
	got := drainDir(t, s)
	if len(got) != 2 || got["d"] != syscall.S_IFDIR || got["f"] != syscall.S_IFREG {
		t.Fatalf("dirStream=%v", got)
	}
	if attr := stableAttr(nil); attr.Mode != uint32(syscall.S_IFREG) || attr.Ino != 0 {
		t.Fatalf("stableAttr(nil)=%+v", attr)
	}
	if attr := stableAttr(&mount.RemoteEntry{FSID: 9, IsDir: true}); attr.Ino != 9 || attr.Mode != uint32(syscall.S_IFDIR) {
		t.Fatalf("stableAttr(dir)=%+v", attr)
	}
}

// 目的：mkdir 必须区分“已存在”“权限错误”“API 错误”，且子目录路径使用动态远端路径。
func TestHandlers_Mkdir(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.entries["/exists"] = &mount.RemoteEntry{FSID: 2, Path: "/exists", IsDir: true, Mode: 0755}
	cloud.statErrs["/denied"] = os.ErrPermission
	cloud.mkdirErr = nil
	root, fsctx := newHandlerFSContext(t, cloud)

	var out fuse.EntryOut
	inode, errno := root.Mkdir(ctx, "new", 0755, &out)
	if errno != 0 || inode == nil || out.Attr.Mode != syscall.S_IFDIR|0755 {
		t.Fatalf("Mkdir new inode=%v errno=%v attr=%+v", inode, errno, out.Attr)
	}
	if _, errno := root.Mkdir(ctx, "exists", 0755, &out); errno != syscall.EEXIST {
		t.Fatalf("mkdir existing errno=%v", errno)
	}
	if _, errno := root.Mkdir(ctx, "denied", 0755, &out); errno != syscall.EACCES {
		t.Fatalf("mkdir denied errno=%v", errno)
	}

	cloud.mkdirErr = os.ErrPermission
	if _, errno := root.Mkdir(ctx, "apierr", 0755, &out); errno != syscall.EACCES {
		t.Fatalf("mkdir api errno=%v", errno)
	}
	// Stat 返回非 ENOENT 错误时不得继续调用 mkdir。
	cloud.mkdirErr = nil
	cloud.statErrs["/other"] = syscall.EIO
	if _, errno := root.Mkdir(ctx, "other", 0755, &out); errno != syscall.EIO {
		t.Fatalf("mkdir stat errno=%v", errno)
	}

	dir, _ := attachDir(t, fsctx, root.EmbeddedInode(), "sub", "/sub")
	if _, errno := dir.Mkdir(ctx, "child", 0755, &out); errno != 0 {
		t.Fatalf("dir.Mkdir errno=%v", errno)
	}
	if _, ok := cloud.entries["/sub/child"]; !ok {
		t.Fatal("dir.Mkdir used wrong remote path")
	}
}

// 目的：Create/Mknod 写路径必须处理 EROFS、EEXIST、EISDIR、O_EXCL；
// 新文件提交空内容，写句柄的 Write/Read/Flush/Fsync/Release 生命周期正确。
func TestHandlers_CreateMknodAndWriteLifecycle(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addDir("/dir")
	cloud.addFile("/existing", []byte("old"))
	root, fsctx := newHandlerFSContext(t, cloud)

	var out fuse.EntryOut
	_, _, _, errno := root.Create(ctx, "existing", syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL, 0644, &out)
	if errno != syscall.EEXIST {
		t.Fatalf("O_EXCL existing errno=%v", errno)
	}
	if _, _, _, errno := root.Create(ctx, "dir", syscall.O_WRONLY|syscall.O_CREAT, 0644, &out); errno != syscall.EISDIR {
		t.Fatalf("create over dir errno=%v", errno)
	}

	node, handle, _, errno := root.Create(ctx, "new.txt", syscall.O_WRONLY|syscall.O_CREAT, 0644, &out)
	if errno != 0 || node == nil || handle == nil {
		t.Fatalf("create new node=%v handle=%v errno=%v", node, handle, errno)
	}
	wh, ok := handle.(*writeFileHandle)
	if !ok || wh == nil {
		t.Fatalf("handle type %T", handle)
	}
	if _, errno := wh.Write(ctx, []byte("abcdef"), 0); errno != 0 {
		t.Fatalf("write errno=%v", errno)
	}
	res, errno := wh.Read(ctx, make([]byte, 4), 1)
	if errno != 0 {
		t.Fatalf("write-handle read errno=%v", errno)
	}
	if b, _ := res.Bytes(nil); string(b) != "bcde" {
		t.Fatalf("write-handle read=%q", b)
	}
	fn := node.Operations().(*fileNode)
	if errno := fn.Flush(ctx, wh); errno != 0 {
		t.Fatalf("flush errno=%v", errno)
	}
	if errno := fn.Fsync(ctx, wh, 0); errno != 0 {
		t.Fatalf("fsync errno=%v", errno)
	}
	if errno := fn.Release(ctx, wh); errno != 0 {
		t.Fatalf("release errno=%v", errno)
	}
	if !wh.writer.(*memWriter).closed {
		t.Fatal("writer not closed on release")
	}

	// 已存在文件不带 O_EXCL：走 Open 复用逻辑，而不是重新创建。
	if _, h2, _, errno := root.Create(ctx, "existing", syscall.O_WRONLY, 0644, &out); errno != 0 || h2 == nil {
		t.Fatalf("create existing errno=%v handle=%v", errno, h2)
	}

	// 新文件空 close：MarkDirty 后 Close 必须落一个 0 字节对象。
	cloud.writerErr = nil
	if _, _, _, errno := root.Create(ctx, "empty", syscall.O_WRONLY|syscall.O_CREAT, 0644, &out); errno != 0 {
		t.Fatalf("create empty errno=%v", errno)
	}

	// 无写支持：Create/Mknod 必须 EROFS。
	roRoot, _ := newHandlerFSContext(t, newHandlerFS())
	roRoot.fs.opts.EnableWrite = false
	if _, _, _, errno := roRoot.Create(ctx, "x", syscall.O_WRONLY|syscall.O_CREAT, 0644, &out); errno != syscall.EROFS {
		t.Fatalf("readonly create errno=%v", errno)
	}
	if _, errno := roRoot.Mknod(ctx, "x", syscall.S_IFREG|0644, 0, &out); errno != syscall.EROFS {
		t.Fatalf("readonly mknod errno=%v", errno)
	}

	// Mknod 类型/存在性/API 错误。
	if _, errno := root.Mknod(ctx, "d", syscall.S_IFDIR|0755, 0, &out); errno != syscall.EINVAL {
		t.Fatalf("mknod dir mode errno=%v", errno)
	}
	if _, errno := root.Mknod(ctx, "existing", syscall.S_IFREG|0644, 0, &out); errno != syscall.EEXIST {
		t.Fatalf("mknod existing errno=%v", errno)
	}
	if _, errno := root.Mknod(ctx, "dir", syscall.S_IFREG|0644, 0, &out); errno != syscall.EISDIR {
		t.Fatalf("mknod over dir errno=%v", errno)
	}
	mnode, errno := root.Mknod(ctx, "mknod-ok", syscall.S_IFREG|0644, 0, &out)
	if errno != 0 || mnode == nil {
		t.Fatalf("mknod ok errno=%v node=%v", errno, mnode)
	}
	cloud.writerErr = os.ErrPermission
	if _, errno := root.Mknod(ctx, "mknod-denied", syscall.S_IFREG|0644, 0, &out); errno != syscall.EACCES {
		t.Fatalf("mknod writer errno=%v", errno)
	}
	cloud.writerErr = nil
	cloud.statErrs["/stat-err"] = syscall.EIO
	if _, errno := root.Mknod(ctx, "stat-err", syscall.S_IFREG|0644, 0, &out); errno != syscall.EIO {
		t.Fatalf("mknod stat errno=%v", errno)
	}

	// 子目录 Create/Mknod 使用 /dir 前缀。
	dir, _ := attachDir(t, fsctx, root.EmbeddedInode(), "dir", "/dir")
	if _, h, _, errno := dir.Create(ctx, "child", syscall.O_WRONLY|syscall.O_CREAT, 0644, &out); errno != 0 || h == nil {
		t.Fatalf("dir.Create errno=%v handle=%v", errno, h)
	}
	if _, errno := dir.Mknod(ctx, "child2", syscall.S_IFREG|0644, 0, &out); errno != 0 {
		t.Fatalf("dir.Mknod errno=%v", errno)
	}
	if _, ok := fsctx.meta.Get("/dir/child2"); !ok {
		t.Fatal("dir.Mknod used wrong path")
	}
}

// 目的：unlink/rmdir 必须校验类型与空目录、对 ENOENT 幂等并清缓存，
// 不能把百度 delete 的递归语义暴露给 POSIX 调用者。
func TestHandlers_RemoveSemantics(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addFile("/f", []byte("x"))
	cloud.addDir("/empty")
	cloud.addDir("/full")
	cloud.addFile("/full/child", []byte("y"))
	cloud.nilPaths["/nil"] = true
	cloud.statErrs["/denied"] = os.ErrPermission
	root, fsctx := newHandlerFSContext(t, cloud)

	if errno := root.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("unlink errno=%v", errno)
	}
	if _, ok := cloud.entries["/f"]; ok {
		t.Fatal("file not removed")
	}
	if errno := root.Unlink(ctx, "f"); errno != 0 {
		t.Fatalf("unlink missing should be idempotent: %v", errno)
	}
	if errno := root.Rmdir(ctx, "full"); errno != syscall.ENOTEMPTY {
		t.Fatalf("rmdir non-empty errno=%v", errno)
	}
	if errno := root.Rmdir(ctx, "empty"); errno != 0 {
		t.Fatalf("rmdir empty errno=%v", errno)
	}
	cloud.addDir("/empty2")
	if errno := root.Unlink(ctx, "empty2"); errno != syscall.EISDIR {
		t.Fatalf("unlink dir errno=%v", errno)
	}
	cloud.addFile("/file2", nil)
	if errno := root.Rmdir(ctx, "file2"); errno != syscall.ENOTDIR {
		t.Fatalf("rmdir file errno=%v", errno)
	}
	if errno := root.Unlink(ctx, "denied"); errno != syscall.EACCES {
		t.Fatalf("unlink denied errno=%v", errno)
	}
	if errno := root.Unlink(ctx, "nil"); errno != syscall.EIO {
		t.Fatalf("unlink nil errno=%v", errno)
	}

	// Remove API 错误映射。
	cloud.addFile("/apierr", nil)
	cloud.removeErr = os.ErrPermission
	if errno := root.Unlink(ctx, "apierr"); errno != syscall.EACCES {
		t.Fatalf("remove api errno=%v", errno)
	}
	cloud.removeErr = os.ErrNotExist
	if errno := root.Unlink(ctx, "apierr"); errno != 0 {
		t.Fatalf("remove ENOENT should be idempotent: %v", errno)
	}
	cloud.removeErr = nil
	cloud.addDir("/direrr")
	cloud.dirErrs["/direrr"] = os.ErrPermission
	if errno := root.Rmdir(ctx, "direrr"); errno != syscall.EACCES {
		t.Fatalf("rmdir readdir errno=%v", errno)
	}
	cloud.addDir("/dirgone")
	cloud.dirErrs["/dirgone"] = os.ErrNotExist
	if errno := root.Rmdir(ctx, "dirgone"); errno != 0 {
		t.Fatalf("rmdir vanished should be idempotent: %v", errno)
	}
	cloud.addDir("/removedir")
	cloud.removeErr = os.ErrExist
	if errno := root.Rmdir(ctx, "removedir"); errno != syscall.ENOTEMPTY {
		t.Fatalf("rmdir EEXIST errno=%v", errno)
	}

	// dirNode 的 Unlink/Rmdir 使用 /sub 前缀。
	cloud.removeErr = nil
	sub, _ := attachDir(t, fsctx, root.EmbeddedInode(), "sub", "/sub")
	cloud.addFile("/sub/inner", nil)
	if errno := sub.Unlink(ctx, "inner"); errno != 0 {
		t.Fatalf("dir.Unlink errno=%v", errno)
	}
	cloud.addDir("/sub/empty")
	if errno := sub.Rmdir(ctx, "empty"); errno != 0 {
		t.Fatalf("dir.Rmdir errno=%v", errno)
	}
}

// 目的：rename 的 flag 校验、跨目录路径计算与错误映射必须符合 POSIX；
// 原子交换（RENAME_EXCHANGE）不支持时明确返回 EINVAL。
func TestHandlers_RenameBasics(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addFile("/f", []byte("x"))
	cloud.addDir("/from")
	cloud.addFile("/from/old", []byte("y"))
	cloud.addDir("/to")
	root, fsctx := newHandlerFSContext(t, cloud)

	if errno := root.Rename(ctx, "f", root, "g", 0); errno != 0 {
		t.Fatalf("root rename errno=%v", errno)
	}
	if _, ok := cloud.entries["/g"]; !ok {
		t.Fatal("rename result missing /g")
	}
	from, _ := attachDir(t, fsctx, root.EmbeddedInode(), "from", "/from")
	to, _ := attachDir(t, fsctx, root.EmbeddedInode(), "to", "/to")
	if errno := from.Rename(ctx, "old", to, "new", 0); errno != 0 {
		t.Fatalf("dir rename errno=%v", errno)
	}
	if _, ok := cloud.entries["/to/new"]; !ok {
		t.Fatalf("dir rename path wrong: %v", cloud.renames)
	}

	if errno := root.Rename(ctx, "g", &fileNode{fs: fsctx}, "h", 0); errno != syscall.EINVAL {
		t.Fatalf("invalid parent errno=%v", errno)
	}
	if errno := root.Rename(ctx, "g", root, "g", 0); errno != 0 {
		t.Fatalf("same path errno=%v", errno)
	}
	if errno := root.Rename(ctx, "to", root, "to/child", 0); errno != syscall.EINVAL {
		t.Fatalf("subpath errno=%v", errno)
	}
	if errno := root.Rename(ctx, "g", root, "h", renameExchange); errno != syscall.EINVAL {
		t.Fatalf("exchange errno=%v", errno)
	}
	if errno := root.Rename(ctx, "g", root, "to", renameNoReplace); errno != syscall.EEXIST {
		t.Fatalf("noreplace existing errno=%v", errno)
	}
	cloud.statErrs["/denied"] = os.ErrPermission
	if errno := root.Rename(ctx, "g", root, "denied", renameNoReplace); errno != syscall.EACCES {
		t.Fatalf("noreplace stat errno=%v", errno)
	}
	if errno := root.Rename(ctx, "g", root, "missing", renameNoReplace); errno != 0 {
		t.Fatalf("noreplace missing errno=%v", errno)
	}

	cloud.renameErr = os.ErrPermission
	if errno := root.Rename(ctx, "to", root, "other", 0); errno != syscall.EACCES {
		t.Fatalf("rename api errno=%v", errno)
	}
}

// 目的：Statfs 必须缓存配额、正确换算 512 字节块，并在远端失败时返回 errno。
func TestHandlers_Statfs(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	root, _ := newHandlerFSContext(t, cloud)
	var out fuse.StatfsOut
	if errno := root.Statfs(ctx, &out); errno != 0 {
		t.Fatalf("statfs errno=%v", errno)
	}
	if out.Blocks != 1000/512 || out.Bfree != 600/512 || out.Bsize != 512 || out.NameLen != 255 {
		t.Fatalf("statfs=%+v", out)
	}
	cloud.quota = mount.FSStats{Total: 2048, Used: 0, Free: 2048}
	if errno := root.Statfs(ctx, &out); errno != 0 || out.Blocks != 1000/512 {
		t.Fatalf("cached statfs errno=%v out=%+v", errno, out)
	}
	cloud.quotaErr = os.ErrPermission
	root2, _ := newHandlerFSContext(t, cloud)
	if errno := root2.Statfs(ctx, &out); errno != syscall.EACCES {
		t.Fatalf("statfs error errno=%v", errno)
	}
}

// 目的：fileNode.Getattr 在 TTL 过期后必须回源并更新缓存；远端删除/返回空条目必须
// 分别映射为 ENOENT/EIO，不能让内核继续看到旧属性。
func TestHandlers_FileGetattr(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addFile("/f", []byte("hello"))
	root, fsctx := newHandlerFSContext(t, cloud)
	entry := &mount.RemoteEntry{FSID: 5, Path: "/f", Name: "f", Size: 99, Mode: 0644}
	node, _ := attachFile(t, fsctx, root.EmbeddedInode(), "f", "/f", entry)
	node.entryAt = time.Now().Add(-time.Hour) // 强制 TTL 过期

	cloud.addFile("/f", []byte("new"))
	cloud.entries["/f"].FSID = 5
	cloud.entries["/f"].MtimeSec = 123

	var out fuse.AttrOut
	if errno := node.Getattr(ctx, nil, &out); errno != 0 {
		t.Fatalf("getattr errno=%v", errno)
	}
	if out.Attr.Size != 3 || out.Attr.Mtime != 123 || out.Attr.Mode != syscall.S_IFREG|0644 {
		t.Fatalf("attr=%+v", out.Attr)
	}

	cloud.nilPaths["/nil"] = true
	nilNode, _ := attachFile(t, fsctx, root.EmbeddedInode(), "nil", "/nil", &mount.RemoteEntry{FSID: 6, Path: "/nil", Size: 1})
	nilNode.entryAt = time.Now().Add(-time.Hour)
	if errno := nilNode.Getattr(ctx, nil, &out); errno != syscall.EIO {
		t.Fatalf("nil stat errno=%v", errno)
	}

	cloud.addFile("/gone", nil)
	gone, _ := attachFile(t, fsctx, root.EmbeddedInode(), "gone", "/gone", &mount.RemoteEntry{FSID: 7, Path: "/gone", Size: 1})
	delete(cloud.entries, "/gone")
	gone.entryAt = time.Now().Add(-time.Hour)
	if errno := gone.Getattr(ctx, nil, &out); errno != syscall.ENOENT {
		t.Fatalf("deleted getattr errno=%v", errno)
	}

	// entry 为空且 Stat 无法提供新结果时返回 ENOENT。
	empty, _ := attachFile(t, fsctx, root.EmbeddedInode(), "empty", "/empty", nil)
	empty.entryAt = time.Now()
	if errno := empty.Getattr(ctx, nil, &out); errno != syscall.ENOENT {
		t.Fatalf("nil entry getattr errno=%v", errno)
	}
}

// 目的：readFileHandle 必须覆盖 EOF/越界/零长度/缓存/跨块/远端错误与 shutdown，
// 且只统计用户实际读到的字节。
func TestHandlers_ReadFileHandle(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addFile("/f", []byte("abcdefghij"))
	root, fsctx := newHandlerFSContext(t, cloud)
	var tracked int64
	var trackedPath string
	fsctx.onTrackDownload = func(p string, n int64) { tracked += n; trackedPath = p }
	node, _ := attachFile(t, fsctx, root.EmbeddedInode(), "f", "/f", cloud.entries["/f"])
	reader := &handlerReader{data: []byte("abcdefghij"), fsid: cloud.entries["/f"].FSID}
	h := &readFileHandle{node: node, reader: reader, streak: cache.NewStreak()}

	if _, errno := h.Read(ctx, nil, 0); errno != 0 {
		t.Fatalf("zero buffer errno=%v", errno)
	}
	if _, errno := h.Read(ctx, make([]byte, 4), -1); errno != syscall.EINVAL {
		t.Fatalf("negative off errno=%v", errno)
	}
	res, errno := h.Read(ctx, make([]byte, 4), 20)
	if errno != 0 {
		t.Fatalf("EOF errno=%v", errno)
	}
	if b, _ := res.Bytes(nil); len(b) != 0 {
		t.Fatalf("EOF data=%q", b)
	}
	res, errno = h.Read(ctx, make([]byte, 4), 2)
	if errno != 0 {
		t.Fatalf("read errno=%v", errno)
	}
	if b, _ := res.Bytes(nil); string(b) != "cdef" {
		t.Fatalf("read=%q", b)
	}
	// 跨块：[3,7) 跨 4 字节块边界，必须由 readRange 拼接。
	res, errno = h.Read(ctx, make([]byte, 4), 3)
	if errno != 0 {
		t.Fatalf("cross read errno=%v", errno)
	}
	if b, _ := res.Bytes(nil); string(b) != "defg" {
		t.Fatalf("cross read=%q", b)
	}
	if tracked != 8 || trackedPath != "/f" {
		t.Fatalf("tracked=%d path=%q", tracked, trackedPath)
	}

	reader.short = true
	cloud2 := newHandlerFS()
	cloud2.addFile("/s", []byte("abcdef"))
	root2, fsctx2 := newHandlerFSContext(t, cloud2)
	node2, _ := attachFile(t, fsctx2, root2.EmbeddedInode(), "s", "/s", cloud2.entries["/s"])
	h2 := &readFileHandle{node: node2, reader: &handlerReader{data: []byte("abcdef"), fsid: 3, short: true}, streak: cache.NewStreak()}
	if _, errno := h2.Read(ctx, make([]byte, 2), 0); errno != syscall.EIO {
		t.Fatalf("short read errno=%v", errno)
	}

	h2.reader.(*handlerReader).readErr = errors.New("boom")
	if _, errno := h2.Read(ctx, make([]byte, 1), 0); errno != syscall.EIO {
		t.Fatalf("remote error errno=%v", errno)
	}
	if _, errno := (&readFileHandle{node: node2}).Read(ctx, make([]byte, 1), 0); errno != syscall.EBADF {
		t.Fatalf("nil reader errno=%v", errno)
	}
	fsctx2.sched.Shutdown()
	if _, errno := h2.Read(ctx, make([]byte, 1), 0); errno != syscall.EIO {
		t.Fatalf("shutdown errno=%v", errno)
	}
}

// 目的：truncate 增大后的写句柄读取必须用零填充超出旧版本的部分，不能向远端
// 请求不存在的 Range；staging 不足时从远端回源。
func TestHandlers_WriteHandleRead(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addFile("/f", []byte("abcdef"))
	root, fsctx := newHandlerFSContext(t, cloud)
	entry := cloud.entries["/f"]
	node, _ := attachFile(t, fsctx, root.EmbeddedInode(), "f", "/f", entry)
	remote := &handlerReader{data: []byte("abcdef"), fsid: entry.FSID}
	w := &memWriter{data: []byte("XY"), target: 10, reader: remote}
	h := &writeFileHandle{node: node, writer: w, reader: remote}

	res, errno := h.Read(ctx, make([]byte, 8), 0)
	if errno != 0 {
		t.Fatalf("write read errno=%v", errno)
	}
	got, _ := res.Bytes(nil)
	if string(got) != "XYcdef\x00\x00" {
		t.Fatalf("write read=%q, want staging+remote+zeros", got)
	}
	// staging 已覆盖全部请求时只返回 staging，不访问远端。
	w.data = []byte("abcdefghij")
	before := remote.readAt
	res, errno = h.Read(ctx, make([]byte, 4), 0)
	if errno != 0 {
		t.Fatalf("staging read errno=%v", errno)
	}
	if b, _ := res.Bytes(nil); string(b) != "abcd" || remote.readAt != before {
		t.Fatalf("staging read=%q remoteReads=%d", b, remote.readAt-before)
	}
	// 无 reader 且请求超出 staging：EIO。
	h.reader = nil
	w.reader = nil
	w.data = []byte("ab")
	if _, errno := h.Read(ctx, make([]byte, 8), 0); errno != syscall.EIO {
		t.Fatalf("no reader errno=%v", errno)
	}
}

// 目的：块缓存必须按 reader 打开时的 epoch 判定有效性；commit 递增 epoch 后，
// 旧 reader 不得命中旧块，也不得把旧数据回填进新缓存。
func TestReadBlock_BlockEpochGuard(t *testing.T) {
	ctx := context.Background()
	cloud := newHandlerFS()
	cloud.addFile("/f", []byte("old!"))
	root, fsctx := newHandlerFSContext(t, cloud)
	node, _ := attachFile(t, fsctx, root.EmbeddedInode(), "f", "/f", cloud.entries["/f"])
	_ = node
	fsctx.bfs = newEpochFS()
	reader := &handlerReader{data: []byte("old!"), fsid: 77, epoch: 0}
	key := cache.BlockKeyFromPath(fsctx.opts.AccountKey, 77, 4, 0, 0)
	fsctx.blocks.Put(key, []byte("OLD!"))

	// epoch 仍是 0：允许命中缓存。
	data, err := fsctx.readBlock(ctx, reader, 0)
	if err != nil || string(data) != "OLD!" {
		t.Fatalf("cached read=%q err=%v", data, err)
	}

	// 模拟 commit：epoch 递增但缓存尚未清理（并发窗口）。
	fsctx.bfs.BumpBlockEpoch(77)
	reader.data = []byte("new!")
	data, err = fsctx.readBlock(ctx, reader, 0)
	if err != nil || string(data) != "new!" {
		t.Fatalf("stale reader read=%q err=%v", data, err)
	}
	freshKey := key
	freshKey.Epoch = fsctx.bfs.BlockEpoch(77)
	if _, ok := fsctx.blocks.Get(freshKey); ok {
		t.Fatal("stale reader must not backfill new-epoch cache")
	}

	// 新 reader（epoch 匹配）读取后可以正常缓存。
	fresh := &handlerReader{data: []byte("new!"), fsid: 77, epoch: fsctx.bfs.BlockEpoch(77)}
	data, err = fsctx.readBlock(ctx, fresh, 0)
	if err != nil || string(data) != "new!" {
		t.Fatalf("fresh read=%q err=%v", data, err)
	}
	if got, ok := fsctx.blocks.Get(freshKey); !ok || string(got) != "new!" {
		t.Fatalf("fresh reader did not cache: %q ok=%v", got, ok)
	}

	// invalidateBlocks 必须清缓存并递增 epoch。
	fsctx.invalidateBlocks(77)
	if fsctx.blocks.Has(key) {
		t.Fatal("invalidateBlocks did not clear memory cache")
	}
	if fsctx.bfs.BlockEpoch(77) != 2 {
		t.Fatalf("epoch=%d, want 2", fsctx.bfs.BlockEpoch(77))
	}
}

// newEpochFS 构造最小 adapter FS，仅用于 BlockEpoch 测试（不发起网络）。
func newEpochFS() *adapter.BaiduCloudFS {
	return adapter.NewBaiduCloudFS(nil, adapter.NewDlinkCache(time.Minute), "acct")
}
