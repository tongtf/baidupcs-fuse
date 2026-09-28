# 0002 — dlink Range 读取：每请求独立连接 + 固定 UA + TTL/刷新策略

**Status**: Accepted (2026-09-17)，默认参数待 Step0 S0.1/S0.2 用 PCS/Core 栈复核后固化。

## Context
Mount 读文件 = `pcs.LocateDownload(path)` 取 dlink(CDN URL) → 对 dlink 发带 `Range:` 的 HTTP GET（现有并行下载器即如此，`requester/downloader/worker.go`）。但 CDN 有两个**非显然行为**会让"看起来正确"的实现静默失败。

## Decision
1. **每个 Range GET 请求头强制 `Connection: close`**；不要对同一 dlink 复用 keep-alive 连接发多个顺序 Range。Go 侧用共享 `*http.Client`(transport goroutine-safe)即可，但在每请求 header 关 keep-alive（勿裸用连接池复用）。
2. **每次取链/读取设 UA = `pan.baidu.com`**。
3. dlink **(dlink, expireAt)** per fs_id 缓存，**TTL=30min**。
4. 收到 **HTTP 403 / 416 / expired / dlink 失效** → **弃该 fs_id 的 dlink 缓存并重新 `LocateDownload`**（自愈），最多重试 2~3 + 递增退避。

## Why（含证据）
- baidupan-fuse `src/baidu.rs: fetch_part` 实测注释：**"CDN 会 403 掉同一 keep-alive 连接上的第二个 Range 请求"** → 必须每片独立连接(`Connection: close`)；代价仅每片一次 TLS 握手，可接受。
- **缺 UA 命中防盗链 errno=31326**（baidupan-fuse `DL_UA="pan.baidu.com"`）。
- dlink "官方有效 ~8h、顺序复用同一 dlink 无问题"（`src/settings.rs: dlink_ttl=1800`, fs.rs 注释）→ TTL 取保守 30min。
- **403 = CDN 限流/失效的典型表现**，重新取链即可自愈（baidupan-fuse `is_forbidden(403)` → drop cache）。

## Alternatives considered
- **连接池复用同一 dlink 连发 Range**：省 TLS 握手但触发 keep-alive 403 bug → 否决。
- **每块都重新取 dlink（不缓存）**：filemetas/locate 便宜但仍浪费、且加剧请求量与限流风险 → 不选，用 TTL 缓存 + 失效刷新。

## Consequences
- + 规避两类静默失败；读路径可预测。
- - 每片一次 TLS 握手（吞吐略降，被 ~16MB 整块拉取摊薄）。
- **待复核**：以上 UA/Connection/TTL 均为 xpan(参考实现)结论；我们走 PCS `LocateDownload`，Step0 S0.1/S0.2 需在真实账号确认同行为后固化默认值。
