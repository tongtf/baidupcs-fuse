# 0003 — 读并发默认 parallel=1（含 SVIP），限速优先于吞吐

**Status**: Accepted (2026-09-17)，数值待 Step0 S0.1/S0.3 复核。

## Context
v1 文档默认 `download_workers=8 / read_ahead=4`，追求吞吐。但百度对并发有强约束：普通用户高并发会触发**全客户端限速（几小时至几天近 0 速）**；且"多连接并行拉同一文件"未必比单流快。

## Decision
- **per-file in-flight Range GET = 1（通用默认，SVIP 也保持）**；`max_concurrent_files=1`。
- read-ahead 仅在三級顺序读确认"真流式"(streak≥16)后启用，块数=2（见 spec/read-path）。
- 保留多连接机制(aria2 式 `read_range(parts)`)**但默认关闭、仅作实验参数**；全局另有 QPS token-bucket + 带宽上限。
- 并发预算接现有 `pcsconfig.Config.MaxParallel / MaxDownloadLoad`，不另造一套并行参数。

## Why（含证据）
1. BaiduPCS-Go README L952：**"普通用户请将 max_parallel 和 max_download_load 都设置为1…极易触发限速,导致几小时至几天内账号在各客户端接近0速"**；L184 "建议下载线程数最大不超过12"。现有 CLI 默认即 `MaxParallel=1 / MaxDownloadLoad=1`(pcsconfig.go:234-237)。→ 高并发对普通用户是**灾难级风险(P0)**，不是性能优化项。
2. baidupan-fuse `src/fs.rs` 实测注释：**"SVIP '单长流'才是高速通道,并发波浪会被 CDN 限速(8 并发分块反而只有 ~2.3MB/s)"**。→ **连 SVIP 都保持 parallel=1**；这把 v1 "SVIP 可放大到 ≤8"推翻。
3. 实测吞吐基线：单连接 ~4–7MB/s（视 CDN 节点），内核单次 read≤128KB，直连 API 仅 ~160KB/s → **性能承诺应按此下调**（v1 status 示例 "84 MB/s" 严重虚高）。

## Alternatives considered
- **默认 workers=8 / SVIP ≤8**：短期吞吐略升但触发限速封号风险，且实测未必更快 → 否决。
- **完全无并发、连预读都没有**：顺序大文件吞吐掉到单块往返水平，mpv/ffmpeg 体验差 → 不选；用"确认流式才放开小量预读"(streak≥16, +2 块)折中。

## Consequences
- + 账号安全(避免限速封禁)、行为可预测、与现有 CLI 配置同源。
- - 顺序吞吐上限≈单连接 CDN 速率(~4–7MB/s)，需在文档/`--help` 明确告知，勿承诺过高。
- **待复核**：S0.1/S0.3 用 PCS/Core 栈确认并发=1 最优、block size ~16MB 甜点后再固化默认值（当前值为参考实现经验值）。
