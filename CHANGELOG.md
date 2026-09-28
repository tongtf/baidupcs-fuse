# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

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
