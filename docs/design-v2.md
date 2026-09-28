# BaiduPCS-Go Mount 可执行设计方案 v2.0

**基线仓库**：`qjfoidnh/BaiduPCS-Go`（本地已 checkout，go.mod `go 1.23`）
**文档版本**：v2.0（合并并取代 `BaiduPCS-Go_Mount_VFS_完整改造设计文档_v1.0.md` 与 `BaiduPCS-Go_Mount_源码级改造方案_v1.0.md`，作为唯一事实源）
**日期**：2026-09-17

> v2 的核心变化：**所有能力都落到"复用哪个现有函数 / 新增什么"的粒度**；并依据真实代码修正了 v1 中会导致实现卡死或产生错误数据的若干假设（下载原语、版本指纹、并发限速、写路径语义）。

---

## 0. 为什么需要 v2：v1 的两个文档与真实代码对不上的地方

| # | v1 的假设/写法 | 真实代码事实 | 结论 / v2 决定 |
|---|---|---|---|
| A1 | "加一层 adapter 即可实现 `ReadRange`"（源码方案 §8） | 现有下载是**整文件落盘**。没有现成的"按 offset+length 流式读"导出 API，但存在可复用的取链入口：`baidupcs.(*BaiduPCS).LocateDownload(pcspath) (*URLInfo, pcserror.Error)`（`download.go:128`），返回 CDN `dlink`；并行下载器就是对 dlink 发带 `Range:` 的并发 GET | **可行，但要自己封装**：取链 + 自管 Range HTTP。见 §3.1 |
| A2 | Block Key / 完整性校验押在"可靠 revision/etag/sha256"上（VFS §44/§45） | `baidupcs.FileDirectory` 只有 `FsID int64 / Size int64 / Mtime,Ctime int64(秒) / MD5 string(多块时注释"可能不正确") / BlockList []string(per-block md5, DecryptMD5)`。**无 ETag、无 revision**（`file_directory.go:39-60`） | 版本指纹 = `fs_id + size + mtime`；删除"sha256 verify"承诺，降级为可选的 single-block md5。见 §4.1/§4.2 |
| A3 | 默认 `download_workers=8 / read_ahead=4`（两份 v1） | README L952：**普通(非 SVIP)用户并发极易触发限速，导致几小时至几天内全客户端近 0 速**；L184 建议线程 ≤12。现有 CLI 默认 `MaxParallel=1 / MaxDownloadLoad=1`（`pcsconfig/pcsconfig.go:234-237`） | **P0 风险**。非 SVIP 默认并发必须保守(≈1)，read-ahead 调小；SVIP 才允许放大。见 §5.1 |
| A4 | "打开的文件读取以 revision/fs_id/size 辅助校验"（VFS §29），但未定义失败后行为 | Block Key 含 size → open 期间远端被替换会**新旧字节混流** | **open 时 pin 版本快照；访问期检测到变更即干净失败(截断/EIO)，绝不混流**。见 §4.3 |
| A5 | inode `byID+byPath` 双表，失效只列本地操作（源码方案 §19） | 远端可被外部删除/改名 → `byPath` stale、parent 指向已删节点 | **访问时按需 re-stat + TTL；stale parent 处理规则显式定义**。见 §4.4 |
| B1 | close=同步上传（源码方案 §34）+ v1 未提"无断点续传"的致命后果 | README L622 / v4.0.0：**上传不再支持断点续传**；GB 级单次同步提交，任何网络抖动 = **整次写入失败、数据全丢**（recovery 只能重头再传） | staging + 确认前不删临时文件 + 明确"大文件写不可靠"风险；发布策略=只读优先。见 §6.1 |
| B5 | `unlink`/`rmdir` 语义含糊，"--trash"未落地 | `baidupcs.(*BaiduPCS).Remove(paths ...string)`（`rm_mkdir.go:9`）走 PCS remove → **回收站(可恢复)**；永久删需 `RecycleDelete/RecycleClear`（`recycle.go`）。批量按 path 删除 | v1：unlink→Remove(进回收站)；加 `--permanent` 才走 Recycle*。rmdir 必须空目录否则 ENOTEMPTY，不做递归 rm。见 §6.5 |

> 其余中低危项（FUSE session/timeout、allow_other+cache 安全、statfs TTL、多挂载共享 SQLite 锁、CGO/FUSE3 版本）在 §5.2（FUSE session/timeout、allow_other+cache 安全、多挂载 SQLite 锁、CGO/FUSE3）与 §5.5(statfs TTL) 等小节逐条给出决定，不再作为"未决问题"。

---

## 1. 复用现有代码的事实清单（ground truth）

Mount **只**通过下面这些已导出符号与 Core 交互；其余一律不动 CLI/百度协议实现。

### 元数据 / 目录
- `pcs.FilesDirectoriesMeta(path string) (*FileDirectory, pcserror.Error)` — 单对象 stat（`file_directory.go:110`）
- `pcs.FilesDirectoriesBatchMeta(paths ...string) (FileDirectoryList, pcserror.Error)` — **批量 meta**，readdir→stat 时用它减少往返（`:132`）
- `pcs.FilesDirectoriesList(path string, options *OrderOptions) (FileDirectoryList, pcserror.Error)` — 目录列表，**已分页**：单页上限 `maxListNum=1000`、按 `fs_id` 去重防死循环（v4.0.2 修复，`:208-253`）。大目录直接复用。
- `FileDirectory{FsID, AppID, Path, Filename, Ctime int64, Mtime int64, MD5 string, BlockList []string, Size int64, Isdir bool, Ifhassubdir}`（`:39-60`）

### 配额
- `pcs.QuotaInfo() (quota, used int64, pcserror.Error)` / `pcs.SpaceLeftInfo()`（`quota.go:14/35`）→ statfs

### 下载取链（读路径核心）
- `pcs.LocateDownload(pcspath string) (*URLInfo, pcserror.Error)` — 返回 `URLInfo.URLs []{url, encrypt}`，非加密 dlink 可直接 GET（`download.go:128`, URLStrings/SingleURL `:57/:69`）
- `InitRangeSize = 32KB`（`download.go:14`）— 现有下载器初次探测片段大小；Mount 可借鉴"先小 Range 探 size/校验 dlink"的做法

### namespace 变更
- `pcs.Mkdir(pcspath string)`（`rm_mkdir.go:28`，内部已 `deleteCache(parent)`）
- `pcs.Remove(paths ...string)` — **回收站删除**（`rm_mkdir.go:9`）
- `pcs.Rename(from, to string)` / `pcs.Move(cpmvJSON ...*CpMvJSON)` / `pcs.Copy(...)`（`cp_mv_rename.go`）— 非递归、按对象；大目录移动=逐对象，**无原子性/回滚**

### 上传
- `pcs.RapidUpload(...) / UploadTmpFile(uploadid, targetPath, partseq, partOffset, uploadFunc) / UploadCreateSuperFile(...)`（`upload.go:105+`）— 分片上传；**不支持断点续传**。块大小常量：Min/Middle/Max = 4MB/16MB/64MB，推荐 ≤32GB、上限 128GB。**Mount 当前默认分片 = 8MB（VIP 优化；保守档为 `MinUploadBlockSize`=4MB）**，对应 §6.1 `UploadSliceSize=8MB`（VIP 优化）。

### 账号 / HTTP
- `pcsconfig.Config.ActiveUserBaiduPCS() *baidupcs.BaiduPCS`（`internal/pcsconfig/export.go:23`）— 命令 Action 里拿当前账号客户端；多账号按名取对应 `.BaiduPCS()`（`baidu.go:50`，会带上 STOKEN/SBOXTKN/HTTPS/UA/UID/accesstoken）
- `requester.NewHTTPClient()` — `cookiejar.New(nil)` + transport(`MaxIdleConns=100, perHost 60`)（`http_client.go:20-48`）。**并发安全可复用**；CDN Range GET 用同一 *http.Client / Jar

### CLI 注册
- `main.go`: `app.Commands = []cli.Command{...}`，每个 `{Name, Flags, Action func(*cli.Context) error}`。参考 download（`main.go:1058+`）：Action → `pcscommand.RunDownload(args, opts)`。**新增 mount = 追加一个 cli.Command + `internal/pcscommand/mount.go`**

---

### 参考实现：baidupan-fuse（实测值来源，非代码底座）
本方案多处"实测参数/踩坑"来自同目录 `../baidupan-fuse`（Rust + fuser，**百度开放平台 xpan OAuth API**）。它已把读路径(dlink+Range)、写路径(precreate/superfile2/create 秒传)在真实环境跑通，故其数值可直接作为经验默认值；但协议不同(xpan vs BaiduPCS-Go Core cookie/BDUSS)，代码不可复用——所有实测结论须经 **Step 0 Spike** 用我们的 PCS/Core 栈复核后再固化。已吸收的参考点：dlink `Connection:close`+UA(§3.1)、parallel=1 通用默认(§5.1)、block≈16MB(§4.2)、三级顺序读(§4.2b/§5.4)、物化前缀虚段写模型 + md5 秒传去重(§6)。

## 2. 目标与范围（收敛后）

### Phase 1 必须交付（只读 MVP，先发布这个）
```
mount/unmount · readdir · getattr(stat) · open/read/release(opendir/readdir)
metadata cache · block cache(memory-only, FIFO) · range read · conservative read-ahead
read-only 模式 · statfs(quota) · URL 过期刷新 + 网络重试(保守并发)
MockCloudFS 单测/集成测试（不依赖真实账号）
```

### Phase 2（写，风险高，默认关闭 / opt-in `--enable-write`）
```
mkdir · create/write/close-commit · unlink(回收站) · rename(仅同父+空目录/单对象)
staging + upload recovery(整文件重传) · fsync/flush 语义落地
```

### Phase 3（工程化）：status/metrics、systemd、多账号挂载、带宽/QPS 限流、cache 管理
### Phase 4（探索，非承诺）：mmap read / CopyFileRange / xattr / macOS / WinFsp / WebDAV/S3 gateway

**明确不做（v1）**：random-write-as-blockdevice、上传断点续传、完整 POSIX ACL/强一致文件锁。

---

## 3. CloudFS 抽象 —— 绑定到真实 baidupcs 方法

新增 `mount/cloudfs.go`。**接口只描述 Mount 需要的语义**，实现类 `BaiduCloudFS`（`mount/baidu.go`）是唯一允许触碰 `baidupcs.*` 的地方。

```go
// mount/cloudfs.go
type CloudFS interface {
    Stat(ctx context.Context, path string) (*RemoteEntry, error)      // → pcs.FilesDirectoriesMeta
    ReadDir(ctx context.Context, path string) ([]*RemoteEntry, error) // → pcs.FilesDirectoriesList(分页已内置)

    OpenRead(ctx context.Context, path string) (RemoteReader, error)  // 内部自行 Stat + re-stat 比对 + pin + 取 dlink（见下）
    StatFS(ctx context.Context) (*FSStats, error)                     // → pcs.QuotaInfo / SpaceLeftInfo

    Mkdir(ctx context.Context, path string) error                     // → pcs.Mkdir
    Remove(ctx context.Context, paths ...string) error                // → pcs.Remove(回收站); permanent 用 Recycle*
    Rename(ctx context.Context, from, to string) error                // → pcs.Rename / Move（非递归）

    CreateWriter(ctx context.Context, path string) (RemoteWriter, error)// Phase2: staging + RapidUpload/分片
}

// RemoteEntry —— 不暴露百度原始 JSON，隔离协议变化
type RemoteEntry struct {
    FSID     int64   // fs_id（稳定远程标识）
    Path     string  // 规范化后的网盘路径
    Name     string  // server_filename
    IsDir    bool
    Size     int64
    MtimeSec int64   // mtime(秒) —— 版本指纹的一部分
    MD5      string  // 可能为空/不可靠（多块文件）；仅当 BlockList len==1 时可信
}

// RemoteReader：一次 open = 一个被 pin 的版本快照 + dlink(带 TTL)+ 并发受限的 Range GET
type RemoteReader interface {
    // ReadAt 返回 [off, off+size) 的完整数据。返回 []byte 长度必须 == size，否则内部已返回 EIO。
    // go-fuse/v2 的 Read handler 直接将返回的 []byte 复制到 fuse.ReadResult。
    ReadAt(ctx context.Context, off int64, size int) ([]byte, error) // 内部按 block 走 cache→range
    Size() int64   // pinned size（open 时锁定）
    Close() error  // 释放 pinned version；v1 简单实现可为空
}

// FSStats —— statfs
type FSStats struct{ Total, Used, Free int64 }
```

> **关键**：`RemoteReader.Size()` 返回的是 open 时刻锁定的大小，不是"实时"。这是 A2/A4 的正确性基础（§4.3）。

### BaiduCloudFS.OpenRead 的实现要点（A1）
> 接口签名 `OpenRead(ctx, path string) (RemoteReader, error)`：调用者只需传路径。内部 **3 步**消除 Stat/LocateDownload 之间的竞态窗口（AUDIT-002）。

1. **①Stat pin**：`pcs.FilesDirectoriesMeta(path)` 获取 `(FSID, Size, MtimeSec)`，记录为 pinned version。
2. **②LocateDownload**：`pcs.LocateDownload(path)` → 取非加密 dlink（`SingleURL(true)`，HTTPS 首个非加密链接）；**缓存 (dlink, expireAt)** per FSID，**TTL = 30min**。参考实现 baidupan-fuse 实测：官方有效 ~8h、顺序复用同一 dlink 无问题，保守取 30min（`src/settings.rs: dlink_ttl=1800`）。这直接解掉 v2 OQ-2。
3. **③再 Stat 比对**：`pcs.FilesDirectoriesMeta(path)` → 比对 (FSID, Size, MtimeSec)：不一致则返回 `ErrVersionConflict`（干净失败，不返回半成品 reader）。
4. **pin 版本**：三步通过后，记录 open 时的 `(FSID, Size, MtimeSec)`。之后所有 ReadAt 只服务这个快照。§4.3 的访问期 re-stat 检测变更。
4. Range GET：**每个请求强制 `Connection: close`**（baidupan-fuse 实测：CDN 会对"同一 keep-alive 连接上的第二个 Range 请求"返回 **403**，必须每片独立连接），并设 **UA=`pan.baidu.com`**（缺 UA 命中防盗链 errno=**31326**）。即 `Range: bytes=off-(off+len-1)` + `Connection: close`；对 dlink **单文件并发受 §5.1 per-file semaphore(默认容量 1) 限制**。Go 侧用共享 `*http.Client`(transport goroutine-safe)，但务必在请求头强制关 keep-alive，勿裸用连接池复用同一 dlink。
5. 刷新策略：收到 **HTTP 403**(CDN 限流/失效的典型表现)、`416/expired/dlink 失效` → **弃缓存重新 `LocateDownload`**（baidupan-fuse: `is_forbidden(403)`→drop dlink cache），最多重试 N=2~3 + 递增退避。
6. **绝不**把 dlink / BDUSS / STOKEN 泄漏到 FUSE 层或日志。

---

## 4. 缓存与一致性 —— 修正后的可落地模型

### 4.1 Metadata Cache（解决目录/stat 延迟）
- key = `account + path`；value = `RemoteEntry + ExpireAt`。**TTL 默认 30s**，负结果(negative) TTL **5s**。
- 批量场景用 `FilesDirectoriesBatchMeta`。
- **失效触发（本地操作后必须立即 invalidate）**：mkdir/create/close-commit/unlink/rename → 失效相关 path + 其 parent dir listing。

### 4.2 Block Cache —— key 只用"拿得到"的字段（A2）
```go
type BlockKey struct {
    AccountID string
    FSID      int64
    Size      int64   // open-pinned size
    MtimeSec  int64   // open-pinned mtime(秒) —— 版本指纹
    Offset    int64   // block 起始 = index*blockSize
}
// 磁盘文件名: sha256(account|fsid|size|mtime|offset).data
```
- **删除 v1 "sha256 verify"承诺**：没有权威的远端整文件哈希可比。改为：块内只存 `length/offset/fsid/pinned size`；**可选**当 `len(BlockList)==1` 时用该 block md5 做一次弱校验（失败→丢弃重下），多块文件跳过。
- **同尺寸+同时刻替换的碰撞风险**存在但概率低，且被 §4.3 pin-at-open + 访问期 re-stat 检测兜住：若发现远端 `(size,mtime)` 与 pinned 不符 → 判定"版本变了"→ 走变更处理（§4.3），不会静默混流。
- **顺序读块大小：保守档 ~16MB（实测甜点）/ VIP 优化档 32MB（可配，CLI `-read-block-size` 现默认 32MB）**——baidupan-fuse 实测甜点："越大吞吐越高，8MB≈4MB/s、16MB≈7MB/s、32MB持平；内核单次 read≤128KB，直连 API 只有 ~160KB/s"。随机读/小窗口不整块拉（见 §5.4 三级策略）。
- **MVP 缓存 = 纯内存 FIFO**（`VecDeque` 式入场序淘汰），设计值 **cache_mb=128**；当前实发默认 **512MB（VIP 优化）**。参考实现即如此：**无磁盘状态 ⇒ 零崩溃恢复负担、逻辑最简**。LRU+Pin/Unpin 作为可选增强；**disk cache + SQLite(`modernc.org/sqlite`) 降级为 Phase3/4 可选项（离线复用场景）**，不再是核心路径。
- **小文件阈值独立于块大小：`small_file ≤ ~8MB` → 整文件一次取回入 memory**（不再绑定 blockSize）；> 该值走 §5.4。

### 4.2b Read 三级策略：先精确窗口 → 再块缓存 → 确认流式才预读
> baidupan-fuse `src/fs.rs: read()` 实测模型，直接解决 mpv/mp4 **大量 seek**（探测头/跳章节）场景下"小读触发几十 MB 预读 = 延迟爆炸"的问题。
- **streak < 2**（首读 / 刚 seek）：只拉请求窗口 `[off, off+len)`，不整块、不预读 → 随机访问秒回。
- **streak ≥ 2**：进 §4.2 块缓存路径；但前 ~16 次读(≈2MB)仍**不预读**(防 seek 后小读放大)。
- **streak ≥ 16**（确认真流式）：**放开预读 2 块 + "服务已缓存块 / 取下一块"流水化** → 吞吐 ≈ 单块拉取速率(实测 ~4–7MB/s，视 CDN 节点)。
- **位置窗口判定**：`off ∈ [last_end - 64KB, last_end + 256KB]`（SEQ_TOLERANCE=256KB）。
- **单线程阻塞警告**（AUDIT-006）：SingleThreaded=true 下，16MB 块读 ≈ 3-4s 期间所有其他 FUSE 操作（ls/stat/cat）排队等待。v1 已知 tradeoff。
- 顺序判定：`last_read=(ino,end)`；本次 `off ∈ [end−64KB, end+SEQ_TOLERANCE]`(默认 **256KB**)视为连续 → streak++，否则归零。**per-inode** 维护(多文件互不干扰)。random read 立即回退精确窗口、停预读。

### 4.3 open 版本 pin + 变更检测（A4 —— 正确性关键）
```
open(path):                      // = OpenRead(ctx, path) 内部流程
    e = Stat(path)               // 锁定 (FSID, Size, MtimeSec)；一次 RPC 完成
    reader.pinSize = e.Size
read(off,len):
    if off+len > pinned.size: return EIO   // 越界（文件被截断/变短）
    blockKey uses pinned size/mtime         // → 命中"当时版本"的缓存
变更检测(访问期，best-effort, TTL 节流如每 N s / 每次 seek):
    cur = Stat(path)
    if (cur.Size != pinned.size || cur.MtimeSec != pinned.mtime):
        # 文件在 open 期间被外部替换/修改
        → 该 handle 进入"已失效"状态：后续 ReadAt 返回 EIO（或按配置截断到 min(pinned,cur)）
        → 绝不把新旧 block 混在一个句柄里返回
```
- **规则**：一个 open 句柄只服务它打开那一刻的版本；检测到变更就干净失败，由应用重开。这与 s3fs/rclone 的"open-on-open / consistency=refresh"一致，且能避免 §4.2 提到的碰撞混流。

### 4.4 inode/path 身份与外部 mutation（A5）
- **本地 node id**：FUSE/go-fuse/v2 用递增 `nodeid`；维护 `byNodeID map[uint64]*Inode` + `byPath map[string]*Inode`（规范化路径为 key，会话内）。inode 只在 mount session 有效，重启即重建（不持久化 inode）。~~byFSID 索引~~ v1 不需要：cache invalidation 按 path 触发，FSID 仅用于 dlink cache key 和 BlockKey，不需要反向查找 inode。**inode TTL = MetadataTTL（默认 30s）**，过期后 byPath 条目失效，下次 Lookup 触发 re-stat。
- **Lookup**：先查 `byPath(规范化)` → 命中且未过期返回；miss/过期则 re-stat。
- **stale parent / 已删节点**：访问某 path 时若其父 listing 里找不到、或 Stat 返回 NotFound → 视为"已被外部删除"，清理对应 inode 引用并向上 invalidate 到 root（best-effort）。**不追求强一致**；TTL + 按需 re-stat 是主机制。
- **PathResolver**：`local path ↔ remote path`，规范化 `/`、防 `..` 逃逸出 mountedRemoteRoot(本节 PathResolver)，UTF-8/中文/emoji 原样保留（禁止简单 `filepath.Base`）。

---

## 5. FUSE 层与并发预算 —— P0：限速优先于吞吐

### 5.1 并发 / read-ahead 默认值（A3，直接推翻 v1）
> README L952 原文："普通用户请将 max_parallel 和 max_download_load 都设置为1…极易触发限速,导致几小时至几天内账号在各客户端接近0速"。

**因此 Mount 的并发预算必须与现有 CLI 同源、且默认保守：**
```
# 通用默认（非 SVIP 与 SVIP 一律）——parallel=1
per_file_ranges         = 1      # per-file in-flight Range GET，对齐 MaxParallel=1；SVIP 也保持(实测更快)
max_concurrent_files    = 1      # 同时读的远端文件数，对齐 MaxDownloadLoad=1
read_ahead_blocks       = 2      # 仅 §5.4 streak≥16 确认真流式才启用
```
- **SVIP 也不放大**：baidupan-fuse `src/fs.rs` 实测注释——"SVIP '单长流'才是高速通道，并发波浪会被 CDN 限速(8 并发分块反而只有 ~2.3MB/s)"。→ **parallel=1 为保守安全默认（防限速）**。保留多连接机制(`read_range(parts)` aria2 式,见 baidupan-fuse `src/baidu.rs`)但**默认关闭、仅作实验参数**。**注意：当前实发 CLI 默认 MaxParallel=10（VIP 优化档）；非 VIP 用户请按本建议设 parallel≈1**。
- **实现**：`DownloadScheduler` 内建 (a) `perFileSem chan struct{}`（容量=1）限制单文件并发 Range；(b) `globalQPS tokenBucket`(默认 QPS≈5，可配) + 带宽上限。二者都接现有 `pcsconfig.Config.MaxParallel / MaxDownloadLoad`，用户调 CLI 配置即自动生效——**不另造一套并行参数**。
- **顺序检测**：见 §4.2b（三级策略）；随机访问立即回退精确窗口、停预读。

### 5.2 go-fuse/v2 FUSE3 选项（C6/C7/C12）
```go
mountOptions := &fuse.MountOptions{
    SingleThreaded:    true,              // v1 串行分发：与 baidupan-fuse 对齐；代价是大目录 readdir 阻塞其他操作（v1 接受）
    AllowOther:        opts.AllowOther,   // 需 /etc/fuse.conf user_allow_other；见安全警告
    Options:           []string{"fsname=baidupcs"}, 
                       // 不传 default_permissions（AUDIT-005）：启用会触发内核 DAC 检查，AllowOther=true 时其他用户写入被拒
                       // 权限由 Getattr 返回 0777 + mount uid/gid 控制（§5.3）
    MaxBackground:     128,               // C6: 限制内核排队请求，避免网络抖动挂死
    Timeout:           30 * time.Second,  // C6: kernel session timeout；配合每 op context deadline
}
```
- **v1 线程模型 = 单线程串行**（`SingleThreaded: true`）。理由：与 baidupan-fuse "单线程分发"对齐；避免 inode/handle/map 全部要加并发锁；百度 API 有频率限制，串行天然限流。**代价**：任何 FUSE 操作（含 Read）阻塞期间所有其他操作排队（16MB 块读 ≈ 3-4s，期间 ls/stat/cat 全部阻塞）。若 Phase 3 需要多线程，再加 sync.Map / RWMutex 并切换 `SingleThreaded=false`。
- **每个 FUSE handler**：`ctx = context.WithTimeout(r.Context(), perOpTimeout)`（默认 read 60s / metadata 15s），网络错误映射 EIO，不无限阻塞。
- **C7 allow_other 安全**：allow_other=true ⇒ cache 目录必须 `0700`(已是)且**建议同时启用 encrypted cache(Phase4)**；文档强警告"其他本地用户可读挂载内容=可读到云端明文缓存"。v1 默认 allow_other=false。
- **C9 多挂载共享（Phase3/4 only）**：v1 纯内存 cache，无持久化状态，不存在跨挂载共享问题。Phase3/4 启用磁盘 cache 时：`index.db` 用 WAL + `busy_timeout`，每个 mount 独立连接池；block 文件按 namespace(账号+mount)分目录避免跨挂载争用同一块。
- **C12 CGO/FUSE3**：`mount/fuse/linux.go //go:build linux`，其余 core/cache/scheduler 保持纯 Go（可交叉编译）。目标发行版需 libfuse3；systemd `NoNewPrivileges=true` 下 go-fuse/v2(纯 Go, cgo-free)可用。
- **O_DIRECT/O_SYNC 处理**（AUDIT-021）：百度网盘不支持 direct I/O。Open/Create handler 中过滤 O_DIRECT 和 O_SYNC 标志。O_SYNC 不触发每次 write 后上传（仍走 sync-on-close）。
- **百度网盘无 symlink/hardlink/special file**（AUDIT-015）：所有非目录条目一律映射为普通文件（mode 0644）。FUSE 的 Readlink/Symlink/Link/Mknod(special) 返回 ENOSYS。

### 5.3 权限模型
虚拟 uid/gid = mount uid/gid，file=0644 / dir=0755，再应用 umask（默认 1000/1000/022）。read-only 下 create/write/mkdir/unlink/rename → `EROFS`。

### 5.4 Read handler：三级顺序读策略（§4.2b）
go-fuse/v2 `Read(ino, fh, off, size)` 实现 §4.2b：streak<2→精确窗口；≥2→块缓存；≥16→+预读2块且"服务/取下一块"流水化。`last_read=(ino,end)`、per-inode streak，SEQ_TOLERANCE=256KB。

### 5.5 statfs
Total=quota, Used=used, Free=quota-used；**TTL 60s**。写满判断：create/close-commit 前用 `SpaceLeftInfo()`，不足返回 ENOSPC（§6）。statfs 值标注"近似"。**QuotaInfo 失败时的降级策略**：不返回 `0/0/0`（工具会误判"磁盘已满"拒绝写入），而是返回 `(math.MaxInt64, 0, math.MaxInt64)` 并在日志记录错误——这样 statfs 显示无限空间，不阻塞任何操作，用户通过日志或 `mount status` 知道配额查询失败。**写操作后 invalidate**：上传成功/删除/mkdir/rename 后立即 invalidate statfs 缓存（成本为零，避免 `df` 显示旧值长达 60s）。

---

## 6. 写路径 —— 明确语义 + 风险（Phase 2）

### 6.1 close=commit + 无断点续传 = 大文件不可靠（B1，必须显式告知）
**参考实现 baidupan-fuse 已跑通此路径**（`src/fs.rs: WriteSession/upload_session`, xpan precreate/superfile2/create），下面模型直接采纳其"物化前缀 + 虚段"设计：
```
stagingFile = CacheDir/uploads/<path-hash>/<ino>.tmp   (0700)
session = { path, file, tmp, mu sync.Mutex,
            len        // [0,len) 已在暂存文件物理物化
            target     // 逻辑大小(getattr 看到；可 > len，中间是虚段)
            fs_id      // 旧远端文件 id；0=远端还没有
            remote_size// 旧文件大小（回填边界）
            dirty }
write(off,data):
    session.mu.Lock(); defer session.mu.Unlock()
    if off>len: backfill(len→off)               # [len,off) 先物化：≤remote_size 拉旧数据，超出补 0
    file.seek(off); write(data)
    len=max(len,off+len); target=max(target,len); dirty=true
close()/flush/fsync(dirty): upload_session()
```
- **staging 文件锁**：v1 单线程 FUSE（§5.2 `SingleThreaded: true`）下所有 FUSE 操作串行，Write 和 Flush 不会并发。session.mu 仅在 Phase 3 多线程时需要（AUDIT-007）。
- **backfill**（虚段物化）：对 `[cur, min(to,remote_size))` 按 ~1MB 步进从远端拉旧数据写入暂存；`(remote_end,to)` 用 `set_len(to)` 稀疏扩成 0（不扩则上传 read_exact_at 越界报错）。**这就是 §6.3 gap-fill 的具体实现。** 极端场景（write(1GB, "x") 对空文件）= 1024 次 HTTP Range GET ≈ 数分钟（AUDIT-010），v1 可接受。Phase 2 可考虑用 ReadBlockSize（当前默认 32MB）做 backfill 加速。
- **upload_session = "分片算 md5 → precreate(可能秒传) → 只传缺片 → create"**：
  - 逐 `UploadSliceSize`（当前默认 8MB，VIP 优化；保守档 4MB）切片对暂存文件 `read_exact_at` + `md5`（未物化部分先 backfill）。
  - **precreate 返回 None = 秒传免传**(网盘已有同 md5)；否则按响应 `block_list`(缺片序号)**只上传变化的片** → 大文件重传大多便宜，显著缓解"无断点续传"风险。
  - **空文件处理**（AUDIT-004）：百度 API precreate 拒绝空 `block_list=[]`（errno=2）。必须填 `["d41d8cd98f00b204e9800998ecf8427e"]`（`md5([]byte{})`）作为唯一 block entry。Core 的 `mergeStringList()` 对空 blockList 产生 `[""]`（单个空串）——**需验证这等同于百度 API 要求的空串 md5**；若不等同，adapter 层必须显式填充。
- **上传成功后清理**（AUDIT-013）：删除 staging 文件 + 清理 uploads/ 子目录。失败后保留到 `CacheDir/failed-uploads/manifest.json`。
- **分片大小自适应**（AUDIT-012）：4MB 起步；官方限 ≤1024 片，>4GB 时按 `max(4MB, ceil(fileSize/1024))` 自动放大。Go 侧照搬此规则（BaiduPCS-Go `MinUploadBlockSize=4MB` / `MaxUploadBlockSize=64MB` 模型不同，优先用本规则）。参考 baidupan-fuse `upload_slice_size()`。
- **上传域名**（AUDIT-003）：superfile2 必须走 `d.pcs.baidu.com`（pan.baidu.com 上是 404）。Core 的 `UploadTmpFile` 内部已处理，adapter 层不得自行拼 URL。
- **覆盖写**：precreate/create `rtype=3` = 目标已存在则覆盖(对齐 FUSE write-overwrite)；映射到 BaiduPCS "policy" skip/overwrite/rsync。上传成功后 invalidate parent dir listing + dlink cache + 该 ino 读块缓存。
- **v1 不支持多 fd 并发写同一文件**：Open 时检测同 path 是否已有活跃写 session（byPath 查 staging map），有则返回 `EBUSY`。理由：多 fd 各自独立 staging，close 时"后传覆盖先传"但哪个"后"取决于网络延迟而非 close 顺序——无法保证正确性。单 fd 足够覆盖绝大多数用例。
- **上传失败恢复策略（close error 不可丢失）**：
  - FUSE Release 不把 error 传回应用进程（POSIX 语义）；sync-on-close 下上传失败 = **应用认为成功但数据未持久化**。
  - **恢复机制**：
    1. 进程内后台重试 3 次（指数退避 1s/4s/16s），每次重试刷新 dlink。
    2. 3 次全败 → 暂存文件保留，文件名含 `{path-hash}-{timestamp}.tmp`，写入 `CacheDir/failed-uploads/manifest.json`（含 path、大小、md5、失败时间、错误信息）。
    3. `mount status` 子命令列出 `failed-uploads/` 中所有条目。（AUDIT-016：`mount status` 是独立 CLI 调用，非挂载进程内部操作。）
    4. `mount recover` 子命令扫描 `failed-uploads/` 并逐个重传（需独立从 pcsconfig 获取登录态）。
    5. `--recover-uploads` 启动时自动执行恢复。
    6. manifest.json 写入用 `.tmp` + `os.Rename` 保证原子性（跨进程安全）。
  - **文档要求**：README/`--help` 必须声明"默认建议只读挂载；写入用 `--enable-write`；close 不保证成功，失败暂存见 `mount status`"。
- close 同步性：**默认 sync-on-close**(POSIX 认为 close 后已持久化)；提供 `--async-close`(更快但数据后台传，进程退出可能丢)。

### 6.2 fsync/flush 语义（B2）
```
Flush(): 每个 fd close 时调用（可多次）。若 sync-on-close 且该 handle dirty → 触发上传并等待完成；否则仅确保 staging 落盘(fsync)。非 dirty handle 调用 Flush 是 no-op。
Fsync(): 应用显式调用 fsync(2) 时触发。v1 语义等同 Flush（无远端 meta 可 fsync）。实现上可直接复用 Flush 逻辑。
```
- **v1 Fsync = Flush**（无区别），但接口必须保留 Fsync 以满足 go-fuse/v2 FileSystem 接口约束。
- **明确限制**：在 close/Flush 之前，同一 path 的其他 open 句柄可能读到旧版本（与 §4.3 pin-at-open 一致）——文档标注为"最终一致、非强一致"。

### 6.3 随机写 / seek / O_APPEND / 稀疏（B3 —— 定死规则）
- staging 是一个**本地镜像文件**，应用所有 `write(off)`/seek/O_APPEND 都作用其上 → **天然支持任意 offset**。
- **空洞处理**：close-commit 前对 `[0, maxOffsetWritten]` 内未被写入的 gap **补零**(zero-fill)，使上传 size == staging 实际长度。这样稀疏写不会产出"截断/错位"文件。（实现：flushStaging 时按已写区间合并，gap 用 `Seek+Write(zeros)`。）
- 语义即"暂存镜像在 close 时成为权威版本"；不做块设备式随机写优化（v1）。

### 6.4 rename / move（B4 —— v1 保守）
```
rename(from,to):
    if 同父目录:
        pcs.Rename → invalidate from metadata cache
                    → invalidate to metadata cache（若 to 已存在=覆盖写）
                    → invalidate to dlink cache + block cache（覆盖写时旧缓存无意义）
                    → invalidate parent dir listing
    else:  return EOPNOTSUPP + "跨目录移动请用 BaiduPCS-Go CLI mv（支持递归）"
```
v1 **所有跨目录 rename 一律返回 EOPNOTSUPP**，不管对象大小。理由：pcs.Move() 是非递归的（只处理单对象），跨目录场景语义复杂（同名冲突、权限、部分成功无回滚）；用 CLI mv 是更安全的选择。Phase 3 若需跨目录 rename，再加递归 Move 实现。

### 6.5 unlink / rmdir（B5）
```
unlink(path): pcs.Remove([path])            → 进【回收站,可恢复】；invalidate parent + path
rmdir(dir):   if 目录非空 → ENOTEMPTY; else pcs.Remove([dir])     # v1 不做递归 rm，防误删整树
--permanent:  unlink/rmdir 改走 RecycleDelete/RecycleClear(不可逆) —— 需显式开启
```

---

## 7. 模块与文件清单（修正两份 v1 的分歧）

**新增依赖**：`github.com/hanwen/go-fuse/v2`。其余复用现有。`modernc.org/sqlite` 为 Phase3/4 可选（磁盘缓存），v1 不引入。

```text
mount/                       # 【新】纯 Go，可交叉编译；唯一 import baidupcs.* 的是 baidu.go
├── cloudfs.go               CloudFS / RemoteEntry / RemoteReader / FSStats（§3）
├── baidu.go                 BaiduCloudFS：绑定 pcs.FilesDirectories*/LocateDownload/Mkdir/Remove/Rename/QuotaInfo
├── mount.go                 Mount(ctx, fs, opts) 生命周期；unmount/status/recover
├── options.go               MountOptions（见下，默认值=§5.1 保守档）
├── filesystem.go            go-fuse/v2 FileSystem：Lookup/Getattr/Open/Read/Release/Opendir/Readdir/Create/Write/Flush/Fsync/Mkdir/Rmdir/Unlink/Rename/Statfs
├── inode.go                 InodeManager(byNodeID/byPath) + PathResolver(规范化+防逃逸, §4.4)
├── handle.go                FileHandle：独立 offset(不在 Inode 里)、pinned size/version、dlink 引用计数
├── errors.go                pcserror.Error → syscall errno（§映射见下）
├── statfs.go                QuotaInfo→FSStats (TTL60s)
│
├── cache/                   metadata.go · block.go · memory.go(FIFO+可选LRU)  # v1 纯内存，disk/index 降级 Phase3/4
├── download/                scheduler.go(perFileSem+globalQPS, §5.1) · worker.go(Range GET dlink) · readahead.go(顺序检测)
└── upload/                  buffer.go(staging镜像+补零§6.3+mu锁) · worker.go(分片上传无续传) · recovery.go(CacheDir/failed-uploads/)

mount/fuse/linux.go          //go:build linux —— go-fuse/v2 mount/unmount（C12）
internal/pcscommand/mount.go # 【新】CLI action：解析 flags → pcsconfig.Config.ActiveUserBaiduPCS() → mount.Mount(...)
main.go                      # 追加 cli.Command{Name:"mount", Flags, Action}（参照 download @L1058+；不改其它命令）

tests: mockcloudfs_test · cache/scheduler/unit · cloudfs/integration(Mock) · fuse_e2e(真实挂载 ls/cat/cp/mkdir/touch/rm/mv)
```

**CacheDir 目录结构**（§6.1/§7 统一）：
```
~/.cache/baidupcs-fuse/
├── blocks/                  # block cache 文件（Phase3/4 磁盘缓存时启用；v1 纯内存，此目录空）
├── uploads/                 # staging 暂存文件（§6.1，按 <path-hash>/ 分子目录）
├── failed-uploads/          # 上传失败暂存 + manifest.json（§6.1 恢复机制）
└── index.db                 # metadata cache SQLite（Phase3/4；v1 纯内存，此文件不存在）
```

**MountOptions（当前实发默认 = VIP 优化档；保守档见各字段标注）**：`RemotePath, MountPoint, ReadOnly(true 建议), AllowOther(false), UID,GID,Umask; CacheDir=~/.cache/baidupcs-fuse（下设 blocks/ uploads/ failed-uploads/ index.db）, ReadBlockSize=32MB（读缓存块；VIP 优化档，保守档 16MB=§4.2 甜点）, UploadSliceSize=8MB（上传分片；VIP 优化档，保守档 4MB=§6.1）, CacheSizeMB=512MB（内存缓存；VIP 优化档，设计值 128MB=§4.2）, MetadataTTL=30s; DownloadWorkers(对齐 MaxParallel)=10（VIP 优化档，保守设计值 1）, ReadAheadBlocks=2, SVIP=false; MaxDownloadRate/MaxUploadRate/QPS`。

**错误映射（errors.go）**：NotFound→ENOENT · Permission/Auth→EACCES · AlreadyExists→EEXIST · NotDirectory→ENOTDIR · IsDirectory→EISDIR · 版本失效(§4.3)→EIO · NoSpace→ENOSPC · ReadOnly下写→EROFS · RateLimited/Network(timeout/reset/5xx)→重试后 EIO。

---

## 8. 可执行开发顺序（每步可运行、可测试、可回滚）

### Step 0 —— Spike：**用真实 BaiduPCS-Go Core(cookie)复核参考实现结论 + 定边界**
> **baidupan-fuse(xpan/Rust)已给出大量实测值**(dlink TTL≈8h、parallel=1 通用更快、block=16MB 甜点、三级顺序读)，但它走的是 xpan API；我们主干是 BaiduPCS-Go Core(cookie/BDUSS + PCS `LocateDownload`)。**必须用我们的协议栈复核这些结论是否同样成立**，再定 §5.1 默认值与上限。

- [ ] S0.1 **dlink Range 并发**(对应 baidupan-fuse parallel=1)：`pcs.LocateDownload` 取链 → 对同一文件发 N∈{1,2,4} 个**不同 Range、且每请求 `Connection: close` + UA=`pan.baidu.com`**，记录成功率/延迟/是否 403(keep-alive bug)/限流。确认单文件安全并发数（预期=1）。
- [ ] S0.2 **dlink 有效期**(复核 baidupan-fuse "8h")：取链后隔 T∈{5m,30m,2h} 再 GET，定 TTL(默认 30min)与刷新触发条件(HTTP 403/416)。
- [ ] S0.3 **Range 上限 + block size**：单请求最大 length、超大 offset；用 N∈{8MB,16MB,32MB} 单连接测吞吐，定默认块(预期 ~16MB)与预读块数。
- [ ] S0.4（可选）**上传秒传/分片**(若提前做写路径)：验证 BaiduPCS `precreate/superfile2` 的 md5 block_list 秒传、≤1024 片上限、空文件 workaround 在 Core 侧是否等价成立。
- **产出**：一份"实测参数表(基于 PCS/Core)"回填 §3.1/§4.2b/§5.1。**在 S0 完成前不要固化并发/block/TTL 数值**(当前默认值=参考实现经验值,待复核)。

### Step 1 —— CloudFS + Mock（无账号可测）
[ ] `mount/cloudfs.go`、`tests/mockcloudfs_test.go`(MockCloudFS: files map[string][]byte, dirs)。单测 Stat/ReadDir/OpenRead(版本pin)/StatFS。→ **验收**：mock 下 read offset+len 正确、越界 EIO、变更检测触发失效。

### Step 2 —— BaiduCloudFS adapter（绑定真实 baidupcs，仍可用 mock 覆盖逻辑）
[ ] `mount/baidu.go` 实现全部方法；dlink cache + refresh(S0.2 参数)。→ **验收**：对测试账号能 stat/list/取链成功。

### Step 3 —— FUSE read-only MVP（Phase1 核心，先发布这个）
[ ] inode/handle/filesystem(只读 handler)、metadata+block cache、range downloader(perFileSem=1)+保守read-ahead、go-fuse/v2 mount、errors/statfs。→ **验收**：`BaiduPCS-Go mount / /mnt/baidu --read-only` 后 `ls/cat/cp/mpv/ffmpeg` 正常；小 read 不产生大量 HTTP(看 request count)；断网读 cache hit 继续、miss EIO 且不挂死。

### Step 4 —— 稳定性
[ ] URL 刷新、网络重试(指数退避,仅可重试错)、cache 损坏自愈(scan+drop bad block+redownload)、SIGTERM flush→unmount 正常退出。故障注入(DNS/403/429/reset/partial/corrupt)。

### Step 5 —— Write（Phase2，opt-in）
[ ] staging(补零)+upload worker(无续传,保留失败临时文件)+recovery、mkdir/unlink/rmdir/rename(§6)、fsync/flush 语义。→ **验收**：`echo > f; cat f; mv; rm`(回收站可恢复)；kill -9 后 `--recover-uploads` 能重传；大文件弱网失败不污染远端目录。

### Step 6 —— 工程化（Phase3）
[ ] `mount status`、metrics(内部 counters→可选 Prometheus)、systemd(`baidupcs-mount@.service`, EnvironmentFile 放 remote/mountpoint/account/cache)、多账号/多挂载(cache namespace+SQLite WAL)、带宽/QPS 限流接 `pcsconfig.Config`。

### Step 7 —— 性能与压测
[ ] benchmark(顺序/随机读、metadata、大目录1k/10k/100k、并发读)；指标 MB/s·IOPS·p50/p95/p99·cache hit ratio。**只调优到"HTTP 有效吞吐的合理比例"**，不为 FUSE syscall 本身过度优化。

---

## 9. Git 提交拆分（便于回滚/审查）
```
feat: add cloudfs abstraction + mock          (Step1)
feat: add baidu cloudfs adapter               (Step2)
feat: add fuse read-only filesystem           (Step3a inode/handle/filesystem)
feat: add metadata cache                      (Step3b)
feat: add block cache(memory-only FIFO)       (Step3c)
perf: add range downloader + conservative read-ahead  (Step3d)
feat: add mount cli command                   (main.go + pcscommand/mount.go)
test: add mount integration + e2e             (Step4/5 测试)
fix: handle expired dlink / network retry     (Step4)
perf: optimize sequential read                (Step7)
```

---

## 10. 仍需实测确认的开放问题（很小，Spike 覆盖）
| ID | 问题 | 影响 | 如何定 |
|---|---|---|---|
| OQ-1 | dlink 单文件安全并发数 / 限速阈值(非 SVIP vs SVIP) | §5.1 默认值上限 | **参考实现经验值 parallel=1**；Step0 S0.1 用 PCS/Core 复核 |
| OQ-2 | dlink 有效期 TTL + 刷新触发码 | dlink cache/refresh(§3.1) | **参考实现已给(~8h有效/TTL30min,403刷新)**；Step0 S0.2 复核 |
| OQ-3 | `Remove` 是否恒进回收站、是否有"直接永久删"入口差异(账号类型) | §6.5 --permanent 实现 | Phase2 前对测试账号验证一次（与参考实现无关,纯我们栈） |
| ~~OQ-4~~ | ~~空文件 workaround 在 Core 侧是否等价成立~~ | ~~§6 upload_session~~ | **已解决**：Core `mergeStringList()` 自动处理（见 §6.1） |

> **OQ-1/OQ-2** 已有 baidupan-fuse 经验值，Step0 用我们的 PCS/Core 栈复核即可；**OQ-4 已解决**（空文件 workaround 由 Core `mergeStringList()` 自动处理，见 §6.1）；**真正只能靠我们协议栈实测的是 OQ-3(删除语义按账号类型)**。除这几点外，v2 的设计决定已定死并绑定到具体函数/文件，可直接开工。
