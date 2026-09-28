# bdfs 使用手册

本手册覆盖 `bdfs` 全部子命令与参数。参数默认值以实际构建为准,运行 `bdfs <command> --help` 可查看当前版本的确切值。

## 目录

- [凭据准备](#凭据准备)
- [mount — 挂载网盘](#mount--挂载网盘)
- [status — 查看状态](#status--查看状态)
- [recover — 恢复上传](#recover--恢复上传)
- [refresh — 刷新缓存](#refresh--刷新缓存)
- [参数速查表](#参数速查表)
- [典型场景](#典型场景)
- [开机自启](#开机自启)
- [常见问题](#常见问题)
- [安全须知](#安全须知)

---

## 凭据准备

`bdfs` 通过 BDUSS(必要时 STOKEN)访问百度网盘。获取方式:浏览器登录 `pan.baidu.com` → F12 → Application → Cookies → 复制 `BDUSS`。

凭据优先级(高→低):

1. 命令行参数 `-bduss` / `-stoken`
2. `-env-file <文件>`(每行形如 `BDUSS="..."`、`STOKEN="..."`)
3. 环境变量 `$BDFS_BDUSS` / `$BDFS_STOKEN`

> 建议自启场景用 `-env-file` 或环境变量,**避免凭据出现在命令行**(否则会被 `ps`/历史命令看到)。仓库提供 [`scripts/install-macos.sh`](scripts/install-macos.sh) 与 systemd/launchd 模板。

---

## mount — 挂载网盘

```bash
bdfs mount -mount <挂载点> -bduss '<BDUSS>' [-stoken '<STOKEN>'] [选项...]
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-mount` | **必填** | 本地挂载点路径(不存在会自动创建) |
| `-bduss` | 必填 | BDUSS cookie |
| `-stoken` | - | STOKEN;缺省时走 pan-api 兼容模式 |
| `-remote` | `/` | 映射的云端根路径 |
| `--enable-write` | false | 启用上传(写)路径,本地暂存、关闭即提交 |
| `-use-core-pcs` | false | 使用 Core PCS API(需 `-bduss` + `-stoken`;默认走 pan-api) |
| `-account-key` | `default` | 账户标识(cache key 前缀,多账号用) |
| `--max-parallel` | 10 | 全局最大并发下载/上传数(**保守值设 1**,避免限速) |
| `-per-file-sem` | 1 | 单文件最大并发块数(`>1` 启用单文件并行,需配合 max-parallel) |
| `-read-block-size` | 33554432 (32MB) | 读缓存块大小(字节) |
| `-block-cache-size` | 536870912 (512MB) | 内存读缓存总容量(字节) |
| `-disk-cache-size` | 10737418240 (10GB) | 磁盘缓存容量(字节,`0`=不限) |
| `-cache-dir` | `~/.cache/baidupcs-fuse` | 缓存目录 |
| `-dlink-ttl` | 30m | dlink(下载直链)缓存 TTL |
| `-metadata-ttl` | 30s | 文件 stat 缓存 TTL |
| `-dir-ttl` | 24h | 目录列表缓存 TTL |
| `-dir-refresh-interval` | 24h | 后台目录刷新间隔(`0`=禁用) |
| `-stoken-refresh` | 1h | STOKEN 自动刷新间隔(`0`=禁用) |
| `-env-file` | - | 环境变量文件路径(刷新 STOKEN 时自动回写) |
| `-curl-timeout` | 60s | curl 单次下载超时 |
| `-http-timeout` | 120s | HTTP 客户端超时 |
| `-upload-slice-size` | 8388608 (8MB) | 上传分片大小(字节) |
| `-allow-other` | false | 允许其他用户访问挂载点(慎用,需内核支持) |

示例:

```bash
# 只读挂载(默认,最快最省流量)
bdfs mount -mount /mnt/pan -bduss 'xxx'

# 读写挂载 + 保守并发(非 VIP 推荐)
bdfs mount -mount /mnt/pan -bduss 'xxx' -stoken 'yyy' \
  --enable-write --max-parallel 1 --per-file-sem 1

# 从凭据文件读取(自启推荐)
bdfs mount -mount /mnt/pan -env-file ~/.bdfs.env -remote '/我的文档'
```

挂载后可像本地文件一样操作:

```bash
ls /mnt/pan/
cat /mnt/pan/视频.mp4            # 按需下载 + 缓存
cp /mnt/pan/照片.jpg ./local     # 下载副本
vim /mnt/pan/笔记.txt            # 需 --enable-write,保存即上传
```

> ⚠️ FUSE 为单线程串行模型(任一读/写操作阻塞时,其他操作排队)。大文件读取期间 `ls`/`stat` 会短暂等待。

---

## status — 查看状态

查看当前缓存目录的挂载与传输统计(活跃 fetch、疑似卡死的上传等):

```bash
bdfs status [-cache-dir <dir>]
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-cache-dir` | `~/.cache/baidupcs-fuse` | 要查询的缓存目录 |

---

## recover — 恢复上传

列出并重传失败的上传(写路径失败时保留在 staging,可用本命令重传):

```bash
bdfs recover [-bduss 'xxx'] [-stoken 'yyy'] [-env-file <f>] [-use-core-pcs] [-cache-dir <dir>]
```

| 参数 | 说明 |
|---|---|
| `-bduss` | BDUSS(必填) |
| `-stoken` | STOKEN(可选) |
| `-env-file` | 凭据文件路径(读取 BDUSS/STOKEN) |
| `-use-core-pcs` | 使用 Core PCS API |
| `-cache-dir` | 缓存目录 |

---

## refresh — 刷新缓存

清除指定或全部目录的元数据缓存,强制重新从云端拉取:

```bash
bdfs refresh [-path <云端路径>] [-cache-dir <dir>]
#   -path 为空 → 刷新全部缓存目录;否则只刷该路径
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-path` | (空=全部) | 要刷新的云端路径,如 `/docs` |
| `-cache-dir` | `~/.cache/baidupcs-fuse` | 缓存目录 |

示例:`bdfs refresh -path /文档` 立即使该目录下远端变更可见。

---

## 参数速查表(仅 mount)

> 完整列表见上方 `mount` 章节;此处按类别速览。

**凭据**: `-bduss`(必填)、`-stoken`、`-env-file`、`-use-core-pcs`、`-account-key`
**挂载**: `-mount`(必填)、`-remote`、`--enable-write`、`-allow-other`
**并发/性能**: `--max-parallel`、`-per-file-sem`、`-read-block-size`
**缓存**: `-block-cache-size`、`-disk-cache-size`、`-cache-dir`、`-dlink-ttl`、`-metadata-ttl`、`-dir-ttl`、`-dir-refresh-interval`
**网络/超时**: `-curl-timeout`、`-http-timeout`、`-stoken-refresh`
**上传**: `-upload-slice-size`

---

## 典型场景

**只读挂载,最快最省流量(推荐日常使用)**
```bash
bdfs mount -mount /mnt/pan -bduss 'xxx' --max-parallel 1
```

**非 VIP 用户的保守读写**
```bash
bdfs mount -mount /mnt/pan -bduss 'xxx' -stoken 'yyy' \
  --enable-write --max-parallel 1 --per-file-sem 1
```

**只映射子目录(如「我的文档」)**
```bash
bdfs mount -mount /mnt/bd -bduss 'xxx' -remote '/我的文档'
```

**多账号切换**:用 `-account-key` 区分 cache key 前缀,避免不同账户缓存混用。

---

## 开机自启

- Linux:见 [`systemd/bdfs.service`](systemd/bdfs.service) + [`scripts/install-macos.sh`](scripts/install-macos.sh)。
- macOS:见 [`INSTALL-macos.md`](INSTALL-macos.md) 与 [`launchd/com.baidupcs-fuse.plist`](launchd/com.baidupcs-fuse.plist)。

自启强烈建议使用 `-env-file` 存放凭据,**不要内嵌 BDUSS/STOKEN**。

---

## 常见问题

**Q:挂载后读取很慢或报错?**
A:优先降低 `--max-parallel`(非 VIP 设 1);确认走了正确的鉴权模式(`pan-api` 默认,或 `-use-core-pcs`)。Range GET 必须带 `Connection: close` 与特定 User-Agent,库已内置,无需手动处理。

**Q:写入后文件没上传?**
A:写路径为「关闭即提交」模型——确保文件已 `close`;失败件用 `bdfs recover` 重传,或用 `bdfs status` 查看。

**Q:提示 dlink/签名相关错误(errno)?**
A:dlink 每 30min(TTL)刷新;403/416/expired 会弃缓存重新取链。频繁出现请检查账号状态与网络。

**Q:如何彻底卸载挂载点?**
A:`fusermount -u <挂载点>`(Linux)或 `umount <挂载点>`(macOS);勿用 `pkill -f bdfs`(可能误杀其他会话)。

---

## 安全须知

- **勿提交凭据**:`BDUSS`/`STOKEN` 是登录凭证,仓库已忽略 `*.creds`、`creds.txt`。
- **限速风险**:高并发易触发百度网盘限流,非 VIP 建议 `--max-parallel 1`。
- **上传域名**:上传走 `d.pcs.baidu.com`(superfile2),库内部已处理;空文件需特殊 block_list(库已处理)。
- 本工具仅供学习与个人数据备份,请遵守服务条款与相关法律法规。
