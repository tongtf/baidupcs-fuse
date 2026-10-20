# 开发指南（Development Guide）

本文件是 `baidupcs-fuse` 的**工程约定与实现事实源**，面向修改代码的人。产品用法见
[`USAGE.md`](../USAGE.md)，设计背景见 [`design-v2.md`](design-v2.md) 与 [`adr/`](adr/)。

> 目标：高效、稳定、可诊断。任何改动优先保证**不丢数据、不挂死、错误可见**，其次才是吞吐。

---

## 1. 快速开始

```bash
# 依赖（国内网络）
export GOPROXY=https://goproxy.cn,direct

go build ./...
go test ./... -count=1
go test ./... -count=1 -race
gofmt -l .            # 必须为空
go vet ./...
```

真实账号的挂载验证不属于单测：单测一律使用 `mount.MockCloudFS` / `adapter.MockPCSClient`，
不得在 `go test ./...` 中访问网络。

### 目录职责

| 目录 | 职责 | 禁止 |
|---|---|---|
| `cmd/bdfs` | CLI（mount/status/recover/refresh）、进程生命周期 | 直接 import `baidupcs.*` |
| `fuse` | go-fuse/v2 节点与句柄、errno 映射、读路径编排 | 直接 import `adapter`（历史遗留，逐步移除） |
| `mount` | `CloudFS` / `RemoteReader` / `RemoteWriter` 契约与错误类型 | 依赖具体云厂商 |
| `adapter` | 百度 pan-api / PCS 实现、staging、上传、dlink | 让 `baidupcs.*` 类型越过 `pcscore` 边界 |
| `adapter/pcscore` | 唯一允许 import `BaiduPCS-Go` 的包 | 向外暴露上游类型 |
| `cache` | 内存块缓存、磁盘块缓存、元数据缓存、顺序读状态机 | 发起网络请求 |
| `download` | Range 下载调度、限流、curl 子进程管理 | 依赖 FUSE |
| `config` | 参数结构与默认值 | 读取全局变量 |

依赖方向：`cmd → fuse → {mount, cache, download, adapter}`，`adapter → mount`。
`fuse` 对 `adapter` 的具体类型断言只允许出现在 `Mount()` 的依赖注入处。

---

## 2. 核心不变量（改代码前必须读）

### 2.1 读路径

1. **一个 open 句柄只服务打开瞬间的版本**：`RemoteReader` 在 `OpenRead` 内 pin
   `(FSID, Size, MtimeSec)`；块 key 必须带这三元组 + offset。
2. **短读不是 EOF**：`ReadAt` 返回长度必须等于请求长度；不足即 `ErrShortRead` → EIO。
3. **块缓存项不可变**：`BlockCache.Get` 返回只读切片，调用方只能读；写路径不得原地修改。
4. **同一块并发 miss 只下载一次**：读块统一走 `readBlock`（single-flight）。
5. **预读有界**：每个句柄最多 1 个在途预读；shutdown/Release 后不得再发请求。
6. **403/416 必须在第一现场失效 dlink**：`DlinkCache.Invalidate(fsID)` 后重新取链，
   不允许外层循环复用同一个坏链接。刷新优先 `locatedownload`，返回的
   `(fsID,size,mtime)` 必须与 pinned 版本一致；回退 `method=download` 后还要再次
   `Stat` 校验版本。
7. **HTTP 200 对非零 Range 不是 206**：只有响应体完整覆盖 `[off,off+length)`
   才允许切片接受；仅返回 `length` 字节视为短读，绝不能按请求区间读错数据。
8. **写入成功后不能只靠 mtime 换 key**：百度可能保持 mtime 不变，同尺寸覆盖会命中
   旧块；commit/O_TRUNC/truncate 成功后必须按旧 `fsID` 显式清理内存与磁盘块缓存。

### 2.2 写路径（ADR-0004）

状态机：

```
opening ─► open ─► committing ─► closed
             │          │
             │          └─ 失败：保留 staging + 写入 failed 清单
             └─ O_TRUNC：Reset(target=0)
```

1. `targetSize` 是**逻辑文件大小**，初始为远端旧大小；`Write` 只会增大它，
   `Truncate` 显式设置它。上传大小必须是 `targetSize`，不是 staging 长度。
2. `staging` 只物化 `[0, stagingLen)`；`[stagingLen, min(targetSize, oldSize))` 在上传前从旧 reader
   回填。回填失败必须让 commit 失败，**绝不能补零**；只有超过 `oldSize` 的部分才补零。
3. `truncate` 到更小/更大尺寸都要保留 `[0, min(newSize, oldSize))` 的旧前缀；
   只有 `O_TRUNC` 才允许丢弃全部旧内容。**同尺寸 truncate 是 no-op**，不能重建
   staging 或重传；零长度 `Write` 也不得制造写洞。
4. **close = commit**：`Flush/Release` 默认同步等待上传完成；失败返回 EIO 并保留 staging。
   `--async-close` 是显式 opt-in，且 `fsync` 仍同步。`fsync` 失败不得关闭 writer，
   后续 close 仍可重试。
5. commit 期间冻结该句柄的写入（返回 EBUSY），避免上传半个快照。
6. 卸载先停止新写入，再 drain 在途 commit（有上限），最后才取消 ctx / 退出。
7. 失败清单记录的是逻辑目标大小；`recover` 必须校验 staging 实际长度，长度不符时禁止上传。
   `recover` 必须先拿账户 `cacheDir` 的实例锁，禁止与运行中的挂载并发改 manifest/staging。
8. 写路径的上传接口必须使用**覆盖**语义（`rtype=overwrite/2`）；`skip` 可能在目标
   已存在时直接返回成功却保留旧内容。分片上传要校验服务端返回的 MD5，multipart
   boundary 必须随机生成。

### 2.3 路径解析与 rename

- 节点创建时的 `node.path` 只是初始路径；目录被 rename 后，子节点必须通过
  `gofuse.Inode.Path(rootInode)` 动态解析，再拼回远端根路径。禁止把创建时路径当作永久事实。
- `createFile` 打开**已存在**文件时节点尚未 `NewInode`，必须调用 `openAt(explicitPath)`；
  此时调用 `remotePath()` 会因 go-fuse bridge 为空而 panic。
- `rename` 到已存在的目标：先 `Stat` 源/目标。类型不匹配 → `EISDIR/ENOTDIR`；
  非空目录 → `ENOTEMPTY`；只允许覆盖空目录。
- 禁止“先删目标再 rename”：源 rename 失败会永久丢目标。实现上先把目标改名到同目录
  隐藏备份，源 rename 成功后再删备份；源失败则把备份回滚。回滚失败要保留备份并报 EIO，
  不能静默丢路径。
- `unlink` 前必须 `Stat` 并拒绝目录（`EISDIR`），`rmdir` 拒绝文件（`ENOTDIR`）；
  百度 delete 可能递归，不能把类型判断交给远端。目标已不存在时幂等返回成功，但仍要失效
  本地元数据/负缓存。

### 2.4 错误模型

- `adapter` 保留 `*mount.BaiduError`（含 `Errno/Method/Msg`），不要提前转字符串。
- FUSE 层只调用 `mount.BaiduErrorToErrno(err)`；禁止到处 `return syscall.EIO`。
- 幂等：`Mkdir` 已存在 → 成功；`Remove` 不存在 → 成功；`Rmdir` 非空 → ENOTEMPTY；
  配额不足 → ENOSPC；鉴权失败 → EACCES。
- `context.Canceled` → EINTR（读被取消），`DeadlineExceeded` → ETIMEDOUT；
  shutdown → EIO。

### 2.5 缓存

| 层 | key | 一致性 | 淘汰 |
|---|---|---|---|
| BlockCache | account+fsID+size+mtime+offset | 版本化，不可变；commit 后按 fsID 失效 | FIFO |
| DiskCache | 同上（完整哈希分片） | 原子写 + 长度校验；按 fsID 失效 | 近似 LRU（采样更新） |
| MetadataCache | account+path | TTL + 显式失效 | LRU |
| dlink | account+fsID+size+mtime | TTL + 第一现场失效 | TTL/容量 |
| 负缓存 | account+path | 5~10s | TTL + 上限（10000） |

- 元数据缓存**返回副本**，node 持有自己的副本；写路径只改 node 的副本并显式回写。
- 目录 rename/删除后按前缀失效子树；目录列表刷新后同步更新子项元数据。
- `accountKey` 默认由 BDUSS 派生；缓存目录按 `cacheDir/<accountKey>/` 隔离。
- 同一 `cacheDir` 只允许一个挂载实例（flock），socket 固定放在该账户目录内；
  因此不需要也不允许两个实例共享同一个 `cacheDir`。
- 磁盘 manifest 缺失/损坏时清理无法索引的孤儿块；坏块或长度不符必须丢弃，不能当缓存命中。

---

## 3. 并发与生命周期

- FUSE 使用 `SingleThreaded: false`；共享状态必须用锁或不可变数据。
- 锁顺序固定：`fileNode.mu → writersMu`，禁止反向；网络调用不得持有 `writersMu`。
- `RemoteReader.Close` 必须幂等（`sync.Once` 或 context cancel）。
- 所有常驻后台循环必须有退出条件（`done` channel / ctx），并在 `shutdownFn` 中等待。
  短连接 handler 必须设置 socket deadline，且不得在 shutdown 后继续发起网络请求。
- 异步 close 在启动 goroutine **之前**注册 drain 计数；`WaitCommits` 用计数 + idle channel，
  不能对可能在 `Release` 中 `Add` 的 `WaitGroup` 调用 `Wait`。
- 信号处理：`SIGINT/SIGTERM` → 停止新写入 → drain → 刷盘 → 退出；第二次信号强制退出并
  保留 failed manifest/staging。

### 请求预算（目标）

- 顺序读 1GB（32MB 块、128KB 读）：网络请求 ≈ 块数，不再出现“每块 2 次 curl + 2 次 HTTP”。
- 单文件 `open`：metadata 命中时 ≤1 次远端元数据请求；dlink 与 verify 并行。
- 失败重试：单层、最多 3 次、指数退避 + jitter；429/503 尊重 `Retry-After`；
  403/416 只触发一次 dlink 刷新。

---

## 4. 测试规范

| 类型 | 位置 | 要求 |
|---|---|---|
| 单元 | 同包 `*_test.go` | 不访问网络；用 mock |
| 并发 | `-race` | 至少覆盖 Readdir+Setattr、并发读同块、取消读 |
| 故障注入 | mock | 403、短读、磁盘满、ctx 取消、上传失败 |
| 端到端 | `scripts/e2e-test.sh` | 真实账号，手动执行，不进 CI |

新增行为必须同时给**失败路径**用例。`t.Log` 不能代替断言；把 panic 当预期行为的测试必须删除。

### CI 门槛

```bash
gofmt -l .                       # 空
go vet ./...
go build ./...
go test ./... -count=1
go test ./... -count=1 -race
```

CI 必须包含 `./adapter/...`（写路径最危险的模块）。

---

## 5. 发布检查清单

- [ ] P0/P1 任务全部关闭，且有回归用例
- [ ] 故障注入通过：取消读 / 403 / 短读 / 磁盘满 / kill -9 / 卸载中途 / 双实例
- [ ] `gofmt`、`go vet`、`-race` 全绿
- [ ] README/USAGE/ADR 与实现逐条对照
- [ ] `ACCEPTANCE-REPORT.md` 的结论限定在已验证范围
- [ ] 不提交凭据、缓存、编译产物

---

## 6. 文档维护

- 行为变化：同步更新 ADR 与 `USAGE.md`；接口变化：更新 `specs/`。
- 任务拆分与进度：`docs/TASKS-2026-10.md`。
- 审计发现与验收证据：`AUDIT-2026-10-20.md`。
- 不再维护“设计与实现相反”的描述；发现漂移先改文档或改代码，不能两边都留。
