# Code-Spec — CloudFS API & BaiduCloudFS 绑定

## Scenario: 实现 `mount/cloudfs.go`(接口) + `mount/baidu.go`(唯一触碰 baidupcs.* 的实现)

### 1. Scope / Trigger
- Trigger: **强制**——新增跨层 API 签名（FUSE/Cache/Scheduler ↔ Core），且是 Mount 架构脊柱(ADR-0001)。接口若漏掉 OpenRead/版本 pin，Phase2 会返工。

### 2. Signatures
```go
// mount/cloudfs.go —— FUSE/Cache/Scheduler 只依赖这个
type CloudFS interface {
    Stat(ctx context.Context, path string) (*RemoteEntry, error)      // → pcs.FilesDirectoriesMeta
    ReadDir(ctx context.Context, path string) ([]*RemoteEntry, error) // → pcs.FilesDirectoriesList(分页已内置)

    OpenRead(ctx context.Context, path string) (RemoteReader, error) // 内部 Stat + pin + 取 dlink，见 spec/read-path
    StatFS(ctx context.Context) (*FSStats, error)                       // → pcs.QuotaInfo / SpaceLeftInfo

    Mkdir(ctx context.Context, path string) error                       // → pcs.Mkdir
    Remove(ctx context.Context, paths ...string) error                  // → pcs.Remove(回收站); permanent 走 Recycle*
    Rename(ctx context.Context, from, to string) error                  // → pcs.Rename / Move（非递归）

    CreateWriter(ctx context.Context, path string) (RemoteWriter, error)// Phase2: staging + precreate/superfile2/create
}

type RemoteReader interface {   // 一次 open = 一个被 pin 的版本快照
    // ReadAt 返回 [off, off+size) 的数据。返回的 []byte 长度必须 == size，否则内部已返回 EIO。
    // go-fuse/v2 的 Read handler 直接将返回的 []byte 复制到 fuse.ReadResult。
    ReadAt(ctx context.Context, off int64, size int) ([]byte, error)

    Size() int64   // 【pinned size，open 时刻锁定】——不是实时值

    // Close 释放 pinned version 和 dlink 引用。v1 简单实现可为空（随进程销毁）。
    Close() error
}

type RemoteEntry struct {
    FSID     int64   // fs_id（稳定远程标识）← FileDirectory.FsID
    Path     string  // 规范化网盘路径 ← FileDirectory.Path
    Name     string  // server_filename ← FileDirectory.Filename
    IsDir    bool    // ← FileDirectory.Isdir
    Size     int64   // ← FileDirectory.Size（目录=0）
    MtimeSec int64   // mtime(秒) ← FileDirectory.Mtime —— 版本指纹一部分
    MD5      string  // 【可能空/不可靠】仅当 BlockList len==1 时可信；勿用于强校验
}

type FSStats struct{ Total, Used, Free int64 }
```

### 3. Contracts（每方法 → baidupcs 绑定 + 字段约束）
| CloudFS 方法 | 绑定的 Core 调用 | 关键约束 / 来源 |
|---|---|---|
| Stat | `pcs.FilesDirectoriesMeta(path)` (file_directory.go:110) | path 规范化后传入；返回 RemoteEntry，MD5 原样带(可能空) |
| ReadDir | `pcs.FilesDirectoriesList(path, DefaultOrderOptions)` (:208) | **分页已内置**(单页≤1000、fs_id 去重防死循环)，直接复用；勿自行拼单页逻辑 |
| OpenRead | `pcs.FilesDirectoriesMeta(path)` + `pcs.LocateDownload(path)` + **再 Stat 一次** | 内部 3 步消除竞态：①Stat pin → ②LocateDownload 取 dlink → ③再 Stat 比对 (FSID,Size,Mtime)：不一致则返回 ErrVersionConflict；调用者只传 path |
| StatFS | `pcs.QuotaInfo()` / `SpaceLeftInfo()` (quota.go:14/35) | Total=quota, Used=used, Free=quota-used；TTL 60s，失败退化为 `(MaxInt64, 0, MaxInt64)` + 日志(不阻塞挂载，勿返回 0 误判满盘) |
| Mkdir | `pcs.Mkdir(path)` (rm_mkdir.go:28) | Core 内部已 deleteCache(parent)，Mount 侧再 invalidate parent listing |
| Remove | `pcs.Remove(paths...)` (:9) = **回收站可恢复**；permanent → RecycleDelete/RecycleClear(recycle.go) | v1 unlink→Remove(可恢复)；仅 `--permanent` 走不可逆路径(OQ-3 待按账号类型验证) |
| Rename | `pcs.Rename(from,to)` / `Move(CpMvJSON...)`(cp_mv_rename.go) | **非递归**：大目录/含子树 → EOPNOTSUPP(见 v2 §6.4)，勿做无回滚批量移动 |

### 4. Validation & Error Matrix
- 任何方法返回的 `pcserror.Error` / HTTP 错误按此映射到 syscall errno（errors.go）：
  - NotFound(errno=-9) → **ENOENT**；Permission(-7)/Auth → **EACCES**
  - token/登录态失效(**errno -6 / 111**) → **先刷新账号凭据重试一次**(参照现有 rest_get/rest_post 行为)，再失败才上报 EIO/EACCES
  - IsDirectory→EISDIR · NotDirectory→ENOTDIR · AlreadyExists→EEXIST
  - NoSpace(配额满) → ENOSPC；ReadOnly 下写类操作 → **EROFS**
  - RateLimited / Network(timeout/reset/5xx)/dlink失效 → 重试后 **EIO**(不无限阻塞，见 spec/read-path §4)

### 5. Good / Base / Bad Cases
- **Good**: `OpenRead` 返回的 reader，其后远端同 path 被替换为不同 size；reader.Size() 仍返回 open 时的 pinned size → ReadAt 越界/变更按 spec/read-path §4 干净失败。
- **Base**: Stat→RemoteEntry 字段完整(fs_id/size/mtime/isdir)；ReadDir 对 10k+ 目录一次取全(分页生效)；OpenRead re-stat 比对一致→正常 pin。
- **旧接口风险 (已修复)**: 若 OpenRead 接受 `e *RemoteEntry`，Stat 与 OpenRead 之间文件被替换 → LocateDownload 拿到 B 版本的 dlink，但 pinned 是 A → **静默混流**。**当前设计已消除此风险**：OpenRead 接受 `path string`，内部 3 步（Stat→LocateDownload→再 Stat 比对）消除 Stat/LocateDownload 之间的竞态窗口。
- **Bad**: `reader.Size()` 返回"当前实时 size"(应返回 pinned)，导致 open 期间文件变短时 ReadAt 读到越界/混流。

### 6. Tests Required
- **Unit (MockCloudFS)**：`files map[string][]byte, dirs map[string][]string`；断言 Stat/ReadDir/OpenRead(版本 pin + 3步竞态消除)/StatFS 字段与错误码正确；越界 ReadAt→EIO；变更检测触发失效；ReadAt 返回 []byte 长度 == 请求 size。
- **Integration（真实测试账号）**：对固定路径验证 Stat/List/LocateDownload/QuotaInfo 返回符合 RemoteEntry 契约；Remove 后进回收站可恢复(OQ-3)。

### 7. Wrong vs Correct
#### Wrong
```go
// FUSE handler 里直接 import baidupcs、或读边界用实时 size：
pcs := getPCS(); fd, _ := pcs.FilesDirectoriesMeta(p)   // ← 泄漏 Core 到 FUSE 层，破坏隔离(ADR-0001)
if off+len > currentRemoteSize() { ... }                 // ← 应使用 r.Size()（pinned size）
data, _ := r.ReadAt(ctx, off)                            // ← 缺 size 参数，签名不匹配
```
#### Correct
```go
// OpenRead 接受 path，内部 3 步 Stat→LocateDownload→再 Stat 消除竞态：
r, _ := cloud.OpenRead(ctx, "/path/to/file")             // ①Stat ②LocateDownload ③再 Stat 比对 → pin
data, err := r.ReadAt(ctx, off, size)                     // 返回 []byte；长度≠size 时内部已返回 EIO
// filesystem.go: 复制 data 到 fuse.ReadResult，无需再校验长度
```
