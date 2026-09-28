# baidupcs-fuse

[![CI](https://github.com/tongtf/baidupcs-fuse/actions/workflows/ci.yml/badge.svg)](https://github.com/tongtf/baidupcs-fuse/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.23%2B-blue)](https://go.dev/dl/)

百度网盘 **FUSE 挂载**客户端(Go)。把百度云盘以文件系统的方式挂到本地,Linux/macOS 上直接 `cat`/`cp`/`vim` 云文件,支持可选的上传(写)路径。

本项目是 [BaiduPCS-Go](https://github.com/qjfoidnh/BaiduPCS-Go) 之上的独立 FUSE 层:通过 BaiduPCS-Go 提供的 PCS / pan-api 接口访问你的百度网盘,FUSE 层负责缓存、并发读写与挂载语义,二者经一个明确的 `CloudFS` 接口解耦(详见 [`docs/adr/0001`](docs/adr/0001-cloudfs-abstraction-bound-to-baidupcs-core.md))。

> ⚠️ **免责声明**:本项目仅供学习与个人数据备份使用。请遵守百度网盘服务条款与相关法律法规,作者不对使用本工具产生的任何后果负责。滥用(尤其是高并发抓取)可能触发账号限速甚至封号。

---

## 功能

- 📖 **路径式访问**:像本地文件一样浏览、读取百度云盘目录
- ⚡ **三级读缓存**:内存块缓存 + 顺序读预读 + 磁盘缓存,大幅减少重复下载
- 🔀 **并发传输**:可配置的全局并发与单文件并发(默认串行,避免触发限速)
- ✍️ **可选写路径**(`--enable-write`):本地暂存、关闭即提交上传;支持秒传去重
- 🔄 **STOKEN 自动刷新**与后台目录缓存
- 🩺 **运行时可观测**:`status` 查看挂载状态/传输统计,`recover` 恢复失败的上传
- 🧱 **接口隔离**:唯一允许触碰 BaiduPCS-Go 内部的适配层以下面方式限定(ADR-0001)

```
FUSE / Cache / Scheduler  ──不直接依赖──▶  mount.CloudFS 接口  ──实现──▶  adapter.BaiduCloudFS  ──唯一 import──▶  BaiduPCS-Go/baidupcs
```

## 目录结构

```
baidupcs-fuse/
├── cmd/bdfs/            # CLI 入口(mount / status / recover / refresh)
├── mount/               # CloudFS 接口实现、inode 管理(与具体云厂商解耦)
├── adapter/             # Baidu 适配层(唯一 import BaiduPCS-Go 的地方) + 上传/staging
├── cache/               # 三级缓存:内存块 / 磁盘元数据 / 顺序读 streak
├── download/            # 并发下载调度器 + 测速
├── fuse/                # go-fuse/v2 filesystem 实现(对内核暴露的 VFS)
├── config/              # 挂载参数与默认值
├── specs/               # 接口契约规范(cloudfs-api / read-path / concurrent-rw)
├── docs/                # 设计文档:design-v2 + ADR + 术语表
└── systemd/ launchd/    # Linux / macOS 开机挂载服务模板
```

## 依赖说明

`adapter/pcscore/pcscore.go` 需要 `github.com/qjfoidnh/BaiduPCS-Go/baidupcs` 提供的内部函数与错误码(ADR-0001)。本项目通过 **go.mod 网络依赖**解析 BaiduPCS-Go,无需在本地放置源码副本——go 工具链会自动从模块代理下载:

```go
// baidupcs-fuse/go.mod
require github.com/qjfoidnh/BaiduPCS-Go v0.0.0-20260909034501-1b9131817aaf
```

> 该伪版本锁定上游 `1b91318` 提交,可复现构建。若你的网络无法访问公共代理,设置国内镜像:
>
> ```bash
> export GOPROXY=https://goproxy.cn,direct
> ```
>
> ⚠️ BaiduPCS-Go 采用裸模块路径(`github.com/qjfoidnh/BaiduPCS-Go`,无 `/v4` 后缀)却发布 v4.x tag,部分 Go 版本/代理会拒绝按 `v4.0.2` 拉取。本项目锁定其最新提交的伪版本以规避该问题。

## 编译

需要 Go 1.23+。首次构建会自动下载依赖(含 BaiduPCS-Go)。

```bash
cd baidupcs-fuse
go build -o bdfs ./cmd/bdfs/
```

> 提示:编译产物 `bdfs` 已加入 `.gitignore`,勿提交。

### macOS

见 [`INSTALL-macos.md`](INSTALL-macos.md)(安装 macFUSE + Go,编译与开机挂载)。

## 使用

```bash
# 获取 BDUSS:浏览器登录 pan.baidu.com → F12 → Application → Cookies → BDUSS
./bdfs mount \
  -mount /Volumes/bdpan \
  -bduss 'YOUR_BDUSS' \
  -stoken 'YOUR_STOKEN' \
  -remote / \
  --enable-write \
  --max-parallel 10 \
  --read-block-size 33554432
```

挂载后可直接操作:

```bash
ls /Volumes/bdpan/
cat /Volumes/bdpan/某个文件.txt      # 触发按需下载 + 缓存
cp remote-file ./local-copy
./bdfs status                        # 查看传输统计与缓存状态
./bdfs recover                       # 列出并重传失败的上传
./bdfs refresh -path /文档           # 刷新指定目录元数据缓存
```

> 📖 **完整命令/参数手册见 [`USAGE.md`](USAGE.md)**(含所有子命令、全量参数与默认值、典型场景、常见问题)。

### 关键参数

> 以下为常用参数;`mount` 的全部参数(`-account-key`、各类 TTL、超时等)详见 [`USAGE.md`](USAGE.md)。参数默认值以实际构建为准,运行 `bdfs mount --help` 可查当前版本确切值。

| 参数 | 默认 | 说明 |
|---|---|---|
| `-mount` | (必填) | 挂载点 |
| `-bduss` | (必填) | BDUSS cookie |
| `-stoken` | - | STOKEN;缺省时走 pan-api 兼容模式 |
| `-remote` | `/` | 映射的云端根路径 |
| `--enable-write` | false | 启用上传(写)路径,**非 VIP 用户谨慎** |
| `--max-parallel` | 10 | 全局并发;保守值可设 `1`,避免触发限速 |
| `--per-file-sem` | 1 | 单文件并发块数(`>1` 启用单文件并行) |
| `--read-block-size` | - | 读缓存块大小(字节,VIP 优化约 32MB) |
| `--block-cache-size` | 512MB | 读缓存总容量 |
| `--disk-cache-size` | 10GB | 磁盘缓存容量(`0`=不限) |
| `--allow-other` | false | 允许其他用户访问挂载点(慎用) |
| `-use-core-pcs` | false | 使用 Core PCS API(需 bduss+stoken) |
| `-cache-dir` | `~/.cache/baidupcs-fuse` | 缓存目录 |

其他子命令:`status -cache-dir <dir>`、`recover [-bduss/-stoken/-env-file/-use-core-pcs/-cache-dir]`、`refresh [-path <云端路径>/-cache-dir]`(path 空=刷新全部)。凭据优先级:`-bduss/-stoken` > `-env-file` 文件 > `$BDFS_BDUSS`/`$BDFS_STOKEN` 环境变量。

## 测试

```bash
cd baidupcs-fuse
go test ./... -count=1
# 带竞态检查(推荐)
go test ./cache/... ./fuse/... ./mount/... -count=1 -race
```

单元测试以 `MockCloudFS` 为主,不依赖真实账号;集成测试需要真实账号与固定测试路径。

## 架构与设计文档

| 文档 | 内容 |
|---|---|
| [`docs/design-v2.md`](docs/design-v2.md) | 可执行设计方案(主参考) |
| [`docs/adr/`](docs/adr/) | 架构决策记录(接口隔离、读路径、并发、写路径) |
| [`specs/cloudfs-api.md`](specs/cloudfs-api.md) | `CloudFS` 接口契约 |
| [`specs/read-path.md`](specs/read-path.md) | 读路径实现规范 |
| [`specs/2026-09-19-concurrent-rw-spec.md`](specs/2026-09-19-concurrent-rw-spec.md) | 并发读写规格 |
| [`docs/context-glossary.md`](docs/context-glossary.md) | 领域术语表 |

## 安全须知

- **切勿提交凭据**:`BDUSS`/`STOKEN` 是你的百度网盘登录凭证。本仓库 `.gitignore` 已忽略 `*.creds`、`creds.txt` 等;也请勿把密钥写进代码或 commit。
- **限速风险**:高并发下载易触发百度网盘限流,非 VIP 用户建议 `--max-parallel 1`。
- 上传走 `superfile2`,大文件(>4GB)会自动放大分片、空文件需特殊 block_list 处理——详见 ADR-0004 与 design-v2。

## 关于「独立性」

当前已通过 **go.mod 网络依赖**解析 BaiduPCS-Go:发布仓库单仓库、无本地源码副本,`require` 锁定上游某一提交以保证可复现。这是「零外部源码」与「随上游更新」之间的折中——若需要完全脱离 `qjfoidnh/BaiduPCS-Go`(连模块代理都不依赖),可将 `adapter/pcscore` 改写为自包含 HTTP 实现(直接调 PCS/pan-api,脱离对该模块的 import),属后续可做的重构。

## 许可证

本仓库采用 [Apache-2.0](LICENSE)。核心依赖 [BaiduPCS-Go](https://github.com/qjfoidnh/BaiduPCS-Go)
同为 Apache-2.0;其余第三方库归属与协议详见 [THIRD-PARTY-LICENSES.md](THIRD-PARTY-LICENSES.md)。
