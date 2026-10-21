# 测试覆盖率提升专项计划（2026-10）

> 目标：**每个模块语句覆盖率 ≥ 80%**，全量 `-race` 通过；过程中发现的功能缺陷一并修复。
>
> 基线版本：`HEAD = 4ab4ff5`（v1.1.0）
> 计划创建：2026-10-21
> 状态图例：`TODO` / `DOING` / `DONE` / `BLOCKED`

---

## 1. 基线实测

命令：

```bash
go test ./<pkg> -coverprofile=/tmp/<pkg>.out
go tool cover -func=/tmp/<pkg>.out | tail -1
```

| 包 | 已覆盖/总语句 | 当前覆盖率 | 达到 80% 需新增覆盖语句 |
|---|---:|---:|---:|
| `adapter` | 647/2076 | 31.2% | 1014 |
| `adapter/pcscore` | 0/90 | 0.0% | 72 |
| `cache` | 353/557 | 63.4% | 93 |
| `cmd/bdfs` | 31/531 | 5.8% | 394 |
| `cmd/parallelprobe` | 0/338 | 0.0% | 271 |
| `config` | 0/2 | 0.0% | 2 |
| `download` | 169/222 | 76.1% | 9 |
| `fuse` | 358/1134 | 31.6% | 550 |
| `mount` | 0/42 | 0.0% | 34 |

### 基线质量状态

| 检查 | 结果 |
|---|---|
| `go build ./...` | 通过 |
| `gofmt -l .` | 空 |
| `go vet ./...` | 通过 |
| `go test ./... -race` | 通过 |
| Test 函数总数 | 145（2 个默认跳过：真实账号活测、性能基准） |

### 基线主要盲区

1. `adapter/panapi.go`、`pcsclient.go`、`pcscore.go`、`speed.go` 全部 0%——CI 只验证了 mock 层。
2. `remoteReader.ReadAt/Reload/Close` 0%——真实读路径未执行。
3. FUSE handler 层 50/82 函数 0%——flags、ABI、返回 errno 未验证。
4. `cmd/bdfs` 仅测了凭据解析；`status`/`recover`/`refresh`/挂载主流程未测。
5. `mount` 的 7 个测试全部在测 mock 自身，生产代码 0%。
6. 已确认缺陷：`BumpBlockEpoch` 无调用方、`CacheEpoch` 未被 `readBlock` 校验（死代码）。

---

## 2. 任务拆分

### C01 `mount`：错误映射与类型（目标 ≥80%）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C01-1 | `BaiduErrorToErrno` 表驱动：-9/-11/-8/-10/-6/ENOSPC/EEXIST/ENOTEMPTY/ETIMEDOUT/ECANCELED/EIO | 每个 errno 分支命中 | TODO |
| C01-2 | `BaiduError.Error()`、`Unwrap`、`os.ErrNotExist`/`Permission`/`DeadlineExceeded`/`Canceled` 映射 | 覆盖率 ≥80% | TODO |
| C01-3 | `baiduErrnoToSyscall` 全部 case（含未知 errno 回退 EIO） | 覆盖率 ≥80% | TODO |

新增：`mount/errors_test.go`

### C02 `config`：默认值（目标 100%）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C02-1 | `DefaultOptions` 全字段断言（路径、TTL、并发、容量） | 2/2 语句覆盖 | TODO |

新增：`config/options_test.go`

### C03 `adapter/pcscore`：协议编解码（目标 ≥80%）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C03-1 | 请求编码字段/顺序/URL encode | 编码分支覆盖 | TODO |
| C03-2 | 响应解析：成功、errno、空 body、非法 JSON | 正常+错误分支 | TODO |
| C03-3 | 分片/合并/秒传参数构造 | 参数断言 | TODO |

新增：`adapter/pcscore/pcscore_test.go`

### C04 `cache`：缓存边界（目标 ≥80%）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C04-1 | `BlockCache`：FIFO 淘汰、覆盖写扩容、`InvalidateFSID`、只读切片 | 全分支 | TODO |
| C04-2 | `DiskCache`：坏 manifest、缺块、孤儿清理、Put 失败、容量 0、并发淘汰 | 全分支 | TODO |
| C04-3 | `MetadataCache`：LRU 淘汰、负缓存上限、`InvalidatePrefix/Parent/Dir`、副本隔离 | 全分支 | TODO |
| C04-4 | `streak`：时间窗边界、跨文件重置 | 全分支 | TODO |

扩展：`cache/block_test.go`、`cache/disk_test.go`、`cache/metadata_test.go`、`cache/streak_test.go`

### C05 `download`：HTTP 边界（目标 ≥80%）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C05-1 | HTTP 200 + 非零 Range：body 覆盖 `off+length` 才成功，否则 `ErrShortRead` | 两个分支 | TODO |
| C05-2 | 缺失 `BDFS_META`、非法状态码、`size_download` 不符 | 各自报错 | TODO |
| C05-3 | `Shutdown` 杀活跃 curl、`IsShutdown`、`track/untrack` | 分支覆盖 | TODO |
| C05-4 | 重试耗尽、ctx 取消、`length<=0` | 分支覆盖 | TODO |

扩展：`download/scheduler_test.go`

### C06 `adapter`：真实网络层与写路径（目标 ≥80%，最大项）

#### C06-1 Pan API HTTP（`panapi.go`，35 函数）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C06-1a | `ReadDir` 分页：2500 条、跨页去重、稳定排序 | 2500 条全返回 | TODO |
| C06-1b | `Stat`：filemetas 命中、失败回退 list、ENOENT | 三条路径 | TODO |
| C06-1c | `LocateDownload`/`GetDlinkDownload` 签名/URL 解析 | 字段断言 | TODO |
| C06-1d | `Mkdir`/`Remove`/`Rename` 幂等与错误映射 | 幂等用例 | TODO |
| C06-1e | `RapidUpload`/`PrecreateUpload`/`UploadSlice`/`CreateSuperFile` 请求体与 multipart boundary | httptest 捕获请求 | TODO |
| C06-1f | UID 异步获取、失败负缓存、重试；GET/POST 429/5xx 有限重试 | 调用次数断言 | TODO |
| C06-1g | `QuotaInfo`、`StatFS`、`UpdateStoken` | 分支覆盖 | TODO |

新增：`adapter/panapi_http_test.go`

#### C06-2 Core-PCS 传输（`pcsclient.go`）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C06-2a | HTTP 构造、参数、错误映射 | httptest | TODO |
| C06-2b | 分片上传/合并/秒传 | 请求断言 | TODO |

新增：`adapter/pcsclient_http_test.go`

#### C06-3 真实读路径（`reader.go`）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C06-3a | `ReadAt` 正常/短读/越界 | 数据断言 | TODO |
| C06-3b | 403 → `Reload` 一次 → 重试成功；416 同上；二次失败不再循环 | 调用次数断言 | TODO |
| C06-3c | `Reload` 版本变化、dlink 缓存失效 | 版本断言 | TODO |
| C06-3d | `Close` 幂等、`RefreshDlink` | 双 Close 不 panic | TODO |

新增：`adapter/reader_read_test.go`

#### C06-4 统计与 STOKEN（`speed.go`、`stoken.go`）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C06-4a | `TransferStats` 累加/快照/进度日志启停 | 并发安全 | TODO |
| C06-4b | `StokenRefresher.Start/Stop/RefreshNow`、非 200 不更新、Stop 幂等 | 分支覆盖 | TODO |

扩展：`adapter/stoken_test.go`，新增 `adapter/speed_test.go`

#### C06-5 写路径（`baidu.go`）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C06-5a | `Truncate` 扩大/缩小/负值/关闭后 | 数据断言 | TODO |
| C06-5b | 尾部回填：旧文件 100KB，只写前 1KB，远端仍 100KB | 尾部内容不变 | TODO |
| C06-5c | `Mkdir`/`Remove`/`Rename` 委托与错误 | mock 断言 | TODO |
| C06-5d | `BumpBlockEpoch`/`BlockEpoch` 生效（修复死代码后） | 旧 reader 不回填 | TODO |
| C06-5e | `CreateWriter` 关闭中拒绝、`BeginShutdown`/`WaitCommits` | 分支覆盖 | TODO |

扩展：`adapter/baidu_test.go`

#### C06-6 上传恢复（`upload.go`）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C06-6a | `LoadFailedUploads` 损坏 JSON/空文件/去重 | 分支覆盖 | TODO |
| C06-6b | `RemoveFailedUpload`、`ListProgress`、`ListStuckUploads` | 分支覆盖 | TODO |
| C06-6c | 分片 MD5 校验、短读、覆盖策略 | mock 断言 | TODO |

扩展：`adapter/upload_retry_test.go`

### C07 `fuse`：FUSE 语义与 handler（目标 ≥80%）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C07-1 | `lookupChild`/`readDirEntries`/`mkdirChild`/`mknodFile` | 正负缓存、错误分支 | TODO |
| C07-2 | `createFile`：创建、已存在、O_EXCL、O_TRUNC、失败回滚 | flags 分支 | TODO |
| C07-3 | `renamePath` + `checkRenameFlags`：NOREPLACE/EXCHANGE/同路径 | errno 断言 | TODO |
| C07-4 | `removePath`：文件/目录/不存在/非空/类型不匹配 | errno 断言 | TODO |
| C07-5 | `readBlock`/`readRange`/`readCrossBlock`：缓存命中、single-flight、短读 | 调用次数/数据 | TODO |
| C07-6 | 写句柄 `Write/Flush/Release/Setattr` 与 commit | 数据+errno | TODO |
| C07-7 | `Statfs`/`Getattr`/`Readdir`/`Open` 超时与取消 | 分支覆盖 | TODO |
| C07-8 | `instance_unix`：同目录双实例失败、释放后可再次获取 | 锁竞争 | TODO |

新增：`fuse/handlers_test.go`、`fuse/instance_test.go`；扩展 `fuse/write_lifecycle_test.go`

### C08 `cmd/bdfs`：CLI 可测性重构与子命令（目标 ≥80%）

> 现状 `cmdMount` 单体包含 flag 解析、凭据解析、组件构造、真实挂载，无法直接测试。需做**行为保持**的重构。

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C08-1 | 抽出 `parseMountArgs([]string) (mountConfig, error)`（flag+凭据优先级+空 cache-dir 兜底） | 优先级/缺参/文件凭据 | TODO |
| C08-2 | 抽出 `buildMountComponents(opts) (*components, error)`（client/staging/refresher/logger），挂载注入 `mountFn` | 组件构造断言 | TODO |
| C08-3 | `cmdStatus`：无 socket、健康/异常响应、上传进度 JSON | 临时 socket server | TODO |
| C08-4 | `cmdRecover`：实例锁占用、空清单、本地缺失、成功移除 | 临时目录 | TODO |
| C08-5 | `cmdRefresh`：socket 请求/超时/错误 | 临时 socket | TODO |
| C08-6 | `updateEnvFile`、`deriveAccountKey`、`resolveAccountDir`、`writeCurrentAccount`、`formatSize/Speed`、`parseSize` | 全分支 | TODO |

新增：`cmd/bdfs/mount_args_test.go`、`cmd/bdfs/commands_test.go`

### C09 `cmd/parallelprobe`：开发工具（目标 ≥80%）

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C09-1 | `parseCreds`、`readFull`、`min`、`isRapidNotFound` | 全分支 | TODO |
| C09-2 | `ensureDir`/`uploadFile`/`slowUpload`：fake PCS client | 请求断言 | TODO |
| C09-3 | `diag`：httptest CDN 正常/异常 | 分支覆盖 | TODO |
| C09-4 | `runProbe`：fake client + 注入 fetcher | 成功/失败路径 | TODO |

新增：`cmd/parallelprobe/main_test.go`

### C10 缺陷修复

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C10-1 | `blockEpoch` 死代码：在 commit/O_TRUNC/truncate 后 `BumpBlockEpoch`，`readBlock` 校验 `CacheEpoch`，旧 reader 不回填 | 并发旧 reader 测试 | TODO |
| C10-2 | 执行中发现的其它真实缺陷 | 回归用例 | TODO |
| C10-3 | 修正文档中测试数量、T18/T42/T57 状态过度声明 | 文档与实测一致 | TODO |

### C11 CI 与文档收口

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C11-1 | CI 增加按包覆盖率门槛（≥80%，允许显式豁免清单） | CI 可执行 | DONE |
| C11-2 | 更新本文件进度表、`CHANGELOG.md`、`docs/TASKS-2026-10.md` | 数据为最终实测 | DONE |
| C11-3 | 全量 `gofmt`/`vet`/`race`/`-count=3` 回归 | 全绿 | DONE |

### C12 测试质量复审

| 子项 | 内容 | 验收 | 状态 |
|---|---|---|---|
| C12-1 | 按 `docs/TESTING.md` 复审本轮新增测试：补“目的”注释、删除 trivia/重复/仅覆盖用例 | 每个 Test 有目的；无用例仅为点亮语句 | DONE |
| C12-2 | 对错误路径断言具体语义（errno/错误类型/状态回滚），而非“有错即可” | 抽查全部新增测试 | DONE |
| C12-3 | 对并发/超时用例去除裸 `time.Sleep` 同步 | `-race -count=3` 稳定 | DONE |

---

## 3. 执行顺序

1. **低风险快赢**：C02 → C01 → C03 → C05 → C04
2. **网络与读路径**：C06-1 → C06-2 → C06-3 → C06-4
3. **写路径与上传**：C06-5 → C06-6
4. **FUSE**：C07
5. **CLI 与开发工具**：C08 → C09
6. **缺陷与收口**：C10 → C11

每完成一个模块：运行 `go test ./<pkg> -count=1 -race -cover`，回填本文件状态与实测覆盖率。

---

## 4. 验收标准

> 质量门槛见 `docs/TESTING.md`：**每个测试必须有目的、断言副作用/错误语义；禁止无意义测试和纯覆盖率测试**。

| 项 | 标准 |
|---|---|
| 测试质量 | 符合 `docs/TESTING.md` 全部条款；审查中发现的覆盖率注水用例必须删除或改造 |
| 模块覆盖率 | 上表 9 个包均 ≥80.0%（`go test ./<pkg> -cover`） |
| 竞态 | `go test ./... -race -count=1` 全绿 |
| 稳定性 | `go test ./... -count=3` 全绿；`-shuffle=on` 全绿 |
| 静态检查 | `gofmt -l .` 为空；`go vet ./...` 通过 |
| 测试真实性 | 不允许 `t.Skip` 掩盖失败；仅保留真实账号活测与显式性能基准跳过 |
| 逻辑变更 | 仅限缺陷修复与行为保持的可测性重构，且必须有回归用例 |
| 文档 | 本文件进度表、CHANGELOG、TASKS 状态与实测一致 |

---

## 5. 风险与应对

| 风险 | 应对 |
|---|---|
| FUSE 需要真实内核挂载 | 使用 `gofuse.NewNodeFS` 在进程内构造 inode 树，只测 handler，不调用 `fusermount` |
| `cmdMount` 单体不可测 | 行为保持地抽出解析/组件构造，真实挂载通过 `mountFn` 注入 |
| `parallelprobe` 依赖真实账号 | 使用 fake `PCSClient` + `httptest` CDN，不访问外网 |
| 测试触发真实网络 | 所有 HTTP 客户端注入 `SetBaseURL` 或使用本地 server；禁止访问 pan.baidu.com |
| 覆盖率虚高（只测 mock） | 覆盖率按生产包统计；新增测试必须断言请求内容/状态转换，而非只跑通 |
| 异步/定时用例不稳定 | 统一使用轮询+超时，禁止裸 `time.Sleep` 作为唯一同步手段 |

---

## 6. 进度回填

| 任务 | 状态 | 实测覆盖率 | 备注 |
|---|---|---:|---|
| C01 mount | DONE | 100.0% | `mount/errors_test.go` |
| C02 config | DONE | 100.0% | `config/options_test.go` |
| C03 adapter/pcscore | DONE | 90.0% | 窄接口注入；RapidUpload/FakeRapidUpload 依赖包内未导出类型，保留 10% |
| C04 cache | DONE | 90.5% | `cache/coverage_test.go`、块缓存 epoch 失效 |
| C05 download | DONE | 87.4% | HTTP 200 Range/201/500、Shutdown、headers、per-file cap |
| C06 adapter | DONE | 91.7% | `panapi.go` 98.0%、`reader.go` 95.9%、`upload.go` 83.3%；上传/提交/失败清单流程 |
| C07 fuse | DONE | 81.7% | handler/后台/socket/实例锁/Flush-Fsync/epoch；Mount 成功路径需真实内核环境 |
| C08 cmd/bdfs | DONE | 84.0% | 行为保持重构 `prepareMount`/`parseRecoverOptions`/`runStatus`/`runRefresh`/`recoverEntries` |
| C09 cmd/parallelprobe | DONE | 93.0% | fake PCS + `httptest` + 可注入 `runProbeWith`；不访问百度网络 |
| C10 缺陷修复 | DONE | - | 块缓存 epoch 接线、后台 stop 幂等、reader jitter 竞态 |
| C11 CI/文档 | DONE | - | CI 按包覆盖率门槛；本文件/CHANGELOG/TASKS 回填 |
| C12 测试质量复审 | DONE | - | 新测试均带 `// 目的`；删除 trivia/死代码；错误路径断言 errno/语义 |

### 最终实测（2026-10-21）

```bash
go test ./... -count=1 -coverprofile=/tmp/coverage.out
go tool cover -func=/tmp/coverage.out | tail -1
```

| 包 | 覆盖率 | 结论 |
|---|---:|---|
| `adapter` | 91.7% | ≥80% |
| `adapter/pcscore` | 90.0% | ≥80% |
| `cache` | 90.5% | ≥80% |
| `cmd/bdfs` | 84.0% | ≥80% |
| `cmd/parallelprobe` | 93.0% | ≥80% |
| `config` | 100.0% | ≥80% |
| `download` | 87.4% | ≥80% |
| `fuse` | 81.7% | ≥80% |
| `mount` | 100.0% | ≥80% |
| **合计** | **88.4%** | - |

> `fuse.Mount` 的成功分支依赖真实 FUSE 内核/挂载权限，CI 仅覆盖参数校验、curl 缺失、实例锁占用与失败清理；成功挂载后的后台组件已由 `startDiskFlush`/`startRefreshSocket`/`startBackgroundRefresh` 等拆分函数覆盖。
