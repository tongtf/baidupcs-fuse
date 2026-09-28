# 0001 — CloudFS 抽象层绑定到 BaiduPCS-Go Core（而非 CLI / 直接 HTTP）

**Status**: Accepted (2026-09-17)

## Context
Mount(FUSE/VFS/Cache/Scheduler) 需要一个稳定的后端接口访问百度网盘。候选做法：(a) FUSE → `exec` BaiduPCS-Go CLI；(b) Mount 层直接拼百度 HTTP 请求；(c) 定义 CloudFS 抽象，由唯一实现类 `BaiduCloudFS` 绑定到现有 `baidupcs.*` Core。

## Decision
采用 (c)。规则：
- **FUSE/Cache/Scheduler 一律只依赖 `mount/cloudfs.go` 的接口**（Stat / ReadDir / OpenRead→RemoteReader / StatFS / Mkdir / Remove / Rename / CreateWriter）。
- **唯一允许 import `baidupcs.*` 的文件是 `mount/baidu.go`**。
- CLI、未来 WebDAV/S3 都复用同一 CloudFS；BaiduPCS-Go 只是第一个 backend。

## Why（含证据）
1. v1 文档的架构原则即"Mount 不调用 CLI / 不直接 download 整文件"，(a)(b) 均违反且已被否决。
2. Core 已具备全部所需能力且有**明确导出方法可绑定**：`FilesDirectoriesMeta/List/BatchMeta`(file_directory.go)、`LocateDownload`(download.go:128, dlink)、`Mkdir/Remove/Rename/Move`(rm_mkdir.go / cp_mv_rename.go)、`QuotaInfo/SpaceLeftInfo`(quota.go)。→ 抽象成本极低。
3. FUSE 层绝不能见 BDUSS/STOKEN/dlink（安全隔离，v2 §74）；只有 (c) 能强制这条边界。

## Alternatives considered
- **(a) exec CLI**：进程开销、无流式读、无法控制 Range/并发 → 否决。
- **(b) Mount 直接 HTTP**：把百度协议泄漏进 FUSE，破坏隔离与多 backend 扩展 → 否决。
- **重构 Core 成"面向 FUSE"**：改动面大、影响现有 CLI；用 adapter(baidu.go) 即可，不动 Core → 不选（除非 S0 Spike 证明 `LocateDownload` 不足以支撑流式读，才回退到给 Core 加导出方法）。

## Consequences
- + 隔离清晰、可 mock（MockCloudFS）单测无需真实账号。
- - CloudFS 接口必须一开始就覆盖 OpenRead/RemoteReader(版本 pin)，否则 Phase2 会返工——见 spec/cloudfs-api.md。
- - `baidu.go` 是协议变化唯一落点；百度 API 变更只改这一处。
