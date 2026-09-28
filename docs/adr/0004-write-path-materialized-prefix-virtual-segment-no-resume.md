# 0004 — 写路径：本地暂存(物化前缀+虚段) + close-commit，无断点续传靠 md5 分片去重兜底

**Status**: Accepted (2026-09-17)，Phase2 opt-in(`--enable-write`)；上传等价性待 Step0 S0.4 / Phase2 复核。

## Context
FUSE write/close 必须映射到百度"无随机写、无断点续传"的上传模型（BaiduPCS v4.0.0+ 明确取消上传断点续传，README L622）。v1 已禁止"FUSE write → 每次调用一次 upload"。需要一种既能支持 POSIX `write(off)/seek/O_APPEND/truncate`、又能在 close 时正确提交的方案，并尽量降低弱网失败代价。

## Decision
- **close=commit**：改动全落本地暂存文件；`flush/fsync/close(最后引用)`时才上传（默认 sync-on-close）。
- 暂存用 **"物化前缀 + 虚段"**模型（见 CONTEXT）：`[0,len)` 已物理写入；`[len,target)` 是"旧远端数据/全0"的虚段，按需回填。随机写先 backfill gap；truncate-short=剪短、truncate-long=稀疏扩零。
- **上传 = "逐片算 md5 → precreate(可能秒传) → 只传缺片 → create"**：利用百度 per-slice md5 去重——未变分片**免传**(秒传)、partial change 只传变化片，显著缓解"无断点续传、弱网全量失败"的风险。
- **release 上传失败保留暂存文件供手动抢救**（应用看不到 release 的错误）；`--recover-uploads` 重头再传。

## Why（含证据）
1. "每次 write 调一次 upload"被 v1 明确否决：百度无随机写、请求量爆炸 → 必须 close-commit + 本地暂存镜像。
2. baidupan-fuse `src/fs.rs: WriteSession/upload_session` **已跑通**此模型（xpan precreate/superfile2/create）：物化前缀+虚段、backfill、per-slice md5 → precreate 秒传/只传缺片。→ 采纳其设计，非新造轮子。
3. **无断点续传是硬约束**(README L622)；唯一能降低弱网失败代价的是"分片级去重让多数重传变便宜(秒传)"——这是把风险从"全量重传"降到"只补变化片/多为秒传"的关键。
4. 空文件边界：`block_list=[]` 被 precreate 拒(errno=2)，需**单个空串 md5 当唯一片**绕过（baidupan-fuse 实测，覆盖 `touch`/O_CREAT）。

## Alternatives considered
- **直接映射 POSIX random write 到百度分片写**：无此 API、语义不符 → 否决。
- **write-back + 断点续传**：v4.0.0+ 已取消上传续传，不可依赖 → 不选；改用 md5 去重兜底。
- **异步 close(不等上传)**：更快但进程退出丢数据、违背 POSIX"close=持久化"直觉 → 默认 sync-on-close，`--async-close` 作可选并明确警告。

## Consequences
- + 支持完整 POSIX 写语义；多数重传靠秒传变便宜；失败可抢救。
- - **大文件弱网仍可能失败**（无续传的残余风险），须在 README/`--help` 声明"默认建议只读挂载，写入需 `--enable-write`"。
- - close 前同 path 其他句柄读到旧版本(最终一致、非强一致)，与 read-path pin-at-open 一致；文档标注。
- **待复核**：S0.4 / Phase2 用 BaiduPCS Core(precreate/superfile2)确认 md5 block_list 秒传、≤1024 片上限、空文件 workaround 在 Core 侧等价成立（参考实现是 xpan）。
