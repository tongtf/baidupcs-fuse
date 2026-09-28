# CONTEXT — BaiduPCS-Go Mount 领域术语表

纯术语定义，不含实现细节（实现在 `spec/*.md`）。本表用于消除本项目里最易混淆的几组同名/近义概念。

## 远程对象标识
- **fs_id**：百度网盘给每个文件/目录分配的稳定整数 ID；是"同一远端对象跨请求不变"的唯一可靠标识。**本地 FUSE inode id ≠ fs_id**（inode 仅会话内有效，重启重建）。
- **BlockList / per-block md5**：百度对一个文件的分片级 md5 数组。`len==1`(小文件)时该值可信；`len>1`(大文件)时整文件 md5 "可能不正确"。**不是**内容 revision/etag——本项目无可靠版本指纹，只能用 `fs_id + size + mtime(秒)` 近似（见 ADR-0004 / spec/read-path）。

## 下载直链
- **dlink (下载直链)**：百度 CDN 上临时有效的文件 HTTP 端点 URL。官方有效 ~8h；本项目保守缓存 TTL=30min。**必须**带 UA=`pan.baidu.com`（否则防盗链 errno=31326），且每次 Range GET 需独立连接（见 ADR-0002）。

## "块 / 分片" —— 两个不同概念，禁止混用
| 术语 | 含义 | 典型大小 | 出现位置 |
|---|---|---|---|
| **上传分片 (slice)** | 三段式上传(precreate/superfile2/create)切片的粒度；官方限 ≤1024 片，>4GB 自动放大保持边界对齐 | 4MB 起步 | spec/write-path、ADR-0004 |
| **读缓存块 (block-cache block)** | Range 读取时"整块拉取+缓存"的粒度，决定顺序吞吐；内核单次 read≤128KB，不整块则直连 API ~160KB/s | ~16MB（实测甜点） | spec/read-path、ADR-0003 |

中文口语里的"分片/块"必须落到上表二者之一再讨论。

## 上传
- **秒传 (instant upload)**：precreate 时提交 per-slice md5，若网盘已有同内容则直接完成、免数据回传；partial change 也只传变化片。**本项目上传无断点续传**（BaiduPCS v4.0.0+ / baidupan-fuse 一致），秒传去重是缓解弱网失败的主要手段。

## 写路径
- **物化前缀 (materialized prefix)**：暂存文件中 `[0, len)` 已真实落盘的区间。
- **虚段 (virtual segment)**：`[len, target)`，逻辑上属于"旧远端数据或全 0"但尚未写入暂存的区间；上传时才按需回填(backfill)并稀疏扩零。**随机写/seek/O_APPEND/truncate** 都通过维护这对边界实现（ADR-0004）。

## 账号 / API
- **SVIP (超级会员)**：百度付费最高档。本项目结论：**读并发对 SVIP 也保持 parallel=1**——实测"单长流 > 并发波浪"，且普通用户高并发会触发全客户端限速（ADR-0003）。
- **xpan API vs PCS API**：两套不同的百度网盘接口/鉴权。**baidupan-fuse**(参考实现)走 xpan(开放平台 OAuth, app_key/secret)；本项目主干复用 **BaiduPCS-Go Core**(cookie/BDUSS + PCS `LocateDownload`)。二者协议不同，参考实现的实测值须经 Step0 用我们栈复核（见 spec/read-path §Scope）。
