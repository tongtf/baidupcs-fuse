# Code-Spec — Read 路径（dlink / Range / Block Cache / 三级顺序读）

## Scenario: 实现 `mount/download/*`(scheduler/worker/range/readahead) + block-cache 读取分支，服务 FUSE `Read`。

### 1. Scope / Trigger
- Trigger: **强制**——跨层行为契约 + 多个非显然 CDN gotcha(ADR-0002/0003)。写错会静默失败或把账号限速封禁。**默认参数为参考实现(baidupan-fuse, xpan)经验值，Step0 S0.1/S0.2/S0.3 用 PCS/Core 栈复核后固化**（见 v2 §8 Step0）。

### 2. Signatures
```go
// RemoteReader.ReadAt — 返回 [off, off+size) 的完整数据（AUDIT-001）
// 返回的 []byte 长度必须 == size，否则内部已返回 EIO。
func (r *remoteReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error)

type BlockKey struct {           // spec/cloudfs-api §3；版本指纹只用"拿得到"的字段(无 revision/etag)
    AccountID string
    FSID      int64
    Size      int64   // open-pinned size
    MtimeSec  int64   // open-pinned mtime(秒)
    Offset    int64   // block 起始 = idx * blockSize
}

// download/worker.go —— 单段 Range GET（串行/并发共用）
func fetchPart(ctx context.Context, dlink string, off, length int64) ([]byte, error)
// download/range.go —— aria2 式多连接(默认 parts=1，见 ADR-0003；保留作实验参数)
func readRange(dlink string, off, length int64, parts int) ([]byte, error)

// cache/block.go
func (c *BlockCache) ensure(ctx context.Context, ino uint64, k BlockKey, fetch func() ([]byte, error)) error // FIFO 淘汰超 cap
```

### 3. Contracts（请求头 / 缓存 / 顺序检测参数）
- **每个 Range GET 必须**：`Range: bytes=off-(off+len-1)` + `Connection: close`(勿复用 keep-alive，CDN 会对同连接第二个 Range 返回 403) + `User-Agent: pan.baidu.com`(缺则防盗链 errno=31326)。共享 `*http.Client`，但每请求 header 关 keep-alive。
- **dlink 缓存**：per fs_id `(dlink, expireAt)`，TTL=30min（官方~8h）；失效/403→弃缓存重取(§4)。
- **块大小**默认 ~16MB（实测甜点：8≈4MB/s、16≈7MB/s、32持平；内核单次 read≤128KB，不整块则直连 API 仅~160KB/s）。小文件阈值独立 `small_file ≤ ~8MB`→整取入 memory。
- **缓存**：MVP=纯内存 FIFO(入场序淘汰)，默认 cache_mb=128；disk+SQLite 降级 Phase3/4（见 v2 §4.2 / ADR-0003）。
- **三级顺序读参数**(spec/read-path ↔ FUSE Read，v2 §5.4)：per-inode `last_read=(ino,end)`、`seq_streak`；判定连续=`off ∈ [end−64KB, end+SEQ_TOLERANCE]`(默认 256KB)。
  - streak<2 → **只拉请求窗口**，不整块/不预读（随机 seek 秒回）
  - streak≥2 → 进块缓存路径；前 ~16 次(≈2MB)仍**不预读**(防 seek 后小读放大)
  - streak≥16 → **放开预读 +2 块 + "服务已缓存/取下一块"流水化**（吞吐≈单块速率）

### 4. Validation & Error Matrix
- HTTP **403**(CDN 限流/dlink 失效典型表现) / **416** / expired → **弃该 fs_id dlink 缓存 + 重新 `LocateDownload`**，重试 ≤2~3 + 递增退避(300ms·n)。
- **短读防御（关键）**：拉回字节数 ≠ 请求 length → **返回 EIO，绝不能当 EOF/截断文件**(否则内核把短读当文件尾，用户看到"文件被截断")。baidupan-fuse `ensure_block`："要 {len} 得到 {} → bail"。
- **版本变更**：访问期 re-stat(节流)发现远端 `(size,mtime)` ≠ pinned → 该 handle 置失效；后续 ReadAt→EIO(或按配置截断到 min(pinned,cur))，**绝不新旧 block 混流**(v2 §4.3)。
- Network timeout/connect-reset/5xx/rate-limit → 指数退避重试后 **EIO**，不无限阻塞(FUSE per-op ctx deadline，见 v2 §5.2 C6)。
- `off ≥ pinned.size` → 返回空(EOF)；越界读→EIO。

### 5. Good / Base / Bad Cases
- **Good（顺序流式）**：mpv/ffmpeg 连续读同一文件 → streak 爬到≥16，+2 块预读且流水化，吞吐≈单连接 CDN(~4–7MB/s)。
- **Base（随机 seek）**：探测 mp4 moov / 跳章节，多次小 read 间隔大 → 每次只拉请求窗口、不触发整块/预读，延迟低。
- **Bad（open 期间被替换）**：句柄打开后远端同 path 换成更大文件；若用实时 size 做边界会读到越界旧缓存+新下载混流。**正确**=OpenRead 内部 Stat pin 了 open 时的 size；访问期 re-stat 检测变更→handle 失效→干净失败(§4)。

### 6. Tests Required
- **Unit**：顺序判定(streak/SEQ_TOLERANCE)状态机、BlockKey 版本指纹、`off+len>size` 越界、短读判 EIO 不判 EOF 的分支；readRange parts=1 与窗口切分数学。
- **Integration（注入故障）**：mock dlink，第一次 GET 返回 403 → 断言"弃缓存+重取+成功重试"；返回短数据→断言 ReadAt=EIO(非 EOF)；re-stat 变更→断言 handle 失效且后续 EIO。
- **E2E（真实挂载）**：`mpv/ffmpeg -i /mnt/baidu/x.mp4`(顺序)、随机 seek、大文件 cp；**关键断言**：网络抖动/短读时 `cat`/`cp` 产物大小=远端 size(不被当截断)；小 read 不产生大量 HTTP(看 request count)。

### 7. Wrong vs Correct
#### Wrong
```go
// (1) 复用 keep-alive 连发 Range → CDN 403 bug；(2) 读边界用实时 size
req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))        // ← 缺 Connection:close / UA=pan.baidu.com
if off+len > currentRemoteSize() { return io.EOF }                    // ← 短读/变更被当 EOF，文件"看起来截断"；且用实时 size 会混流
```
#### Correct
```go
req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))
req.Header.Set("Connection", "close")          // ADR-0002：每请求独立连接，规避 keep-alive 403
req.Header.Set("User-Agent", "pan.baidu.com")   // 否则防盗链 errno=31326
data := fetchPart(ctx, dlink, off, length)
if int64(len(data)) != length { return errEIO } // ← 短读=EIO，绝不 EOF/截断(v2 §4.2b / spec/read-path §4)
// 边界用 open-pinned Size()；访问期 re-stat 变更→handle 失效(§4)，不混流
```
