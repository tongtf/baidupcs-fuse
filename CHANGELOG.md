# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased] - 2026-10-20

2026-10 全量审计整改（详见 `AUDIT-2026-10-20.md` 与 `docs/TASKS-2026-10.md`）。

### Added
- `--async-close`（默认关闭，close 同步等待上传）、`--max-upload-parallel`、`--commit-timeout`、`-bduss-file`。
- 账户级缓存隔离：默认由 BDUSS 派生 `account-key`，数据落在 `<cache-dir>/<account-key>/`；同一缓存目录用 flock 保证单实例。
- 磁盘缓存 manifest 对账/孤儿清理、坏块与截断检测；元数据负缓存、LRU 淘汰、前缀失效。
- 失败上传清单改为 JSON 原子写，支持路径含 `|` 与并发写入；`bdfs recover` 逐条重写。

### Changed
- 目录列表 TTL 默认 24h → 5m；上传并发与下载 `--max-parallel` 解耦。
- 读路径统一 `readBlock` + single-flight，块缓存返回只读切片；每句柄至多 1 个在途预读。
- 写路径引入 `targetSize`：部分写不再截断远端尾部；commit 前回填旧尾部/补零；默认 sync-on-close，失败保留 staging。
- FUSE 错误统一映射 errno；Mkdir/Remove/Rename 幂等；根目录支持 rename；`Getattr` 超 TTL 回源。
- 磁盘缓存块路径改为完整 SHA-256；写入 tmp+fsync+rename，权限 0600/0700。

### Fixed
- 修复下载信号量在取消路径泄漏、`requestID` 假幂等、curl Range 响应校验不足。
- 修复 dlink 403/416 不失效、并发重复刷新；同 reader 刷新合并为一次。
- 修复 `TestWritePath_LargeFile_SliceScaling` 与 USAGE 默认值不一致的陈旧断言。
- 修复 Core-PCS 模式 STOKEN 刷新回调为空实现。
- 修复 `parentPath` 多级路径计算错误（写入后失效了根目录而非父目录，并影响上传 `target_path`）。
- 修复 rename 覆盖检查：仅允许覆盖空目录，文件/目录类型不匹配返回 EISDIR/ENOTDIR，避免百度递归删除目标目录内容。
- 修复 O_RDWR 句柄首次读取：staging 为空时 `io.EOF` 被误判为 EIO，无法回源远端。
- 修复 truncate 增大/缩小后读写语义：扩展区返回零、缩小后再写大洞不会回填已截掉的旧数据。
- 修复下载调度器每次 Range GET 泄漏一个 context watcher goroutine。
- 修复失败上传清单损坏时被静默覆盖、`remove` 响应解析失败被当成功、`recover` 读取清单失败却退出 0。
- 修复目录列表过期后后台刷新永远不触发；目录刷新与负缓存保持一致。
- 修复 rename 覆盖目标时“先删后改”导致源改名失败会永久丢失目标：改为目标移入隐藏备份，成功后清理、失败回滚。
- 修复 `unlink` 可能落到目录（百度 delete 可能递归）：删除前 `Stat` 校验类型；幂等删除也清理元数据/负缓存。
- 修复同尺寸 truncate 仍重建 staging、零长度 Write 制造写洞；truncate 现在是真正的 no-op 路径。
- 修复写入后块缓存可能命中旧版本：commit/O_TRUNC/truncate 成功后按旧 `fsID` 清理内存与磁盘块缓存。
- 修复 `OpenRead` 在 dlink 刷新/`method=download` 回退时未校验 `(fsID,size,mtime)`，可能把旧版本当成新 reader。
- 修复秒传使用 `skip` 策略在目标已存在时保留旧内容；写路径统一覆盖语义，并校验分片服务端 MD5。
- 修复 multipart boundary 固定、STOKEN 解析误匹配 `XSTOKEN=` 及非 200 响应解析。
- 修复 `recover` 与运行中挂载共享缓存目录、`-cache-dir ""` 在 Parse 后才生效、`current` 临时文件并发覆盖。
- 修复元数据负缓存无界增长、磁盘 manifest 损坏时孤儿块无法回收。
- 修复块缓存版本失效死代码：`BlockKey.Epoch` 纳入 key，commit/O_TRUNC/truncate 后 bump epoch，旧 reader 不再命中或回填新版本缓存。
- 修复后台刷新 stop 非幂等（重复清理会 `close of closed channel`）；socket 与磁盘刷盘清理改为 `sync.Once`。
- 修复 `OpenRead` 在启动后台刷新前读取全局 jitter 的竞态。

### Docs
- 新增 `docs/DEVELOPMENT.md`、`docs/TASKS-2026-10.md`；USAGE/README/INSTALL 对齐新参数、并发模型与权限说明。

### Testing
- 新增 `docs/TESTING.md` 与 `docs/TEST-COVERAGE-PLAN-2026-10.md`：测试目的/状态/错误/并发/缓存/安全覆盖门槛，禁止纯覆盖率测试。
- 9 个 Go 包语句覆盖率全部 ≥80%（合计 88.4%）：`adapter` 91.7%、`pcscore` 90.0%、`cache` 90.5%、`cmd/bdfs` 84.0%、`cmd/parallelprobe` 93.0%、`config` 100%、`download` 87.4%、`fuse` 81.7%、`mount` 100%。
- CLI 为可测性做行为保持拆分：`prepareMount`、`parseRecoverOptions`、`recoverEntries`、`runStatus`、`runRefresh`；`parallelprobe` 注入 PCS/探测函数，测试全部走 fake/`httptest`，不访问百度网络。
- CI 增加按包覆盖率门槛（每个包 ≥80%），避免总量平均掩盖低覆盖包。
- 修复 `cmd/bdfs` Unix socket 测试服务端未读请求就响应/关闭导致的 Linux CI 偶发失败：测试服务端统一先解码请求再响应，并在测试规范中禁止该写法。

## [1.0.0] - 2026-XX-XX

首个正式稳定版,`baidupcs-fuse` 作为独立 Go FUSE 客户端发布。

### Added
- `mount` 命令:将百度网盘挂载为本地文件系统,支持读(并发 Range GET)、写(close=commit staging)与元数据管理。
- 三级读缓存:`block`(内存块缓存)+ `disk`(磁盘 LRU 缓存)+ `streak`(顺序预读)。
- 并发下载调度器(`download/`),默认并发数、分块大小可通过 flags/config 调节。
- Linux systemd(`systemd/bdfs.service`)与 macOS launchd(`launchd/`)服务模板。
- CLI 诊断工具:`cmd/parallelprobe`(压力测试/吞吐探测)。
- 文档体系:`README.md`、`USAGE.md`(全子命令用法)、`INSTALL-macos.md`、`THIRD-PARTY-LICENSES.md`、`CONTRIBUTING.md`、`CODE_OF_CONDUCT.md`。

### Changed
- 依赖方式:以**网络依赖**(go.mod `require`)引入 BaiduPCS-Go,可直接作为独立模块使用,无需本地 workspace 绑定。
- 上传分片默认 **8MB**,读缓存块默认 **32MB**,与配置项对齐。

### Fixed
- 读路径严格遵循 CloudFS 协议:`Connection: close` + `User-Agent: pan.baidu.com`;版本冲突(`fs_id`/大小/mtime)时返回 `ErrVersionConflict`,杜绝新旧 block 混流。
- 空文件预创建使用空串 md5 作为唯一 block entry;>4GB 文件按 API 上限自动放大分片。
- 移除开发期遗留的真实凭据与本地绝对路径泄露(`systemd/bdfs.env`、`parallelprobe` 默认路径、service ExecStart)。

### Removed
- 删除无用的 `fuse/spike.go` 桩代码及悬空文档引用。

## [0.x] - 早期

早期迭代开发版本,包含架构设计与参考实现验证(详见 Git 历史)。
