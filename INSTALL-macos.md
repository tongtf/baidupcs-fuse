# bdfs macOS 安装指南

## 前置依赖

```bash
# 1. 安装 macFUSE（内核扩展，需重启）
brew install --cask macfuse

# 2. 安装 Go
brew install go

# 3. 确认 curl 可用（macOS 自带）
curl --version
```

## 编译

```bash
# 克隆两个仓库（bdfs 依赖 BaiduPCS-Go）
git clone https://github.com/your-repo/BaiduPCS-Go.git
git clone https://github.com/your-repo/bdfuse.git
cd bdfuse

# 用 go.work 编译（自动解析本地依赖）
cd baidupcs-fuse
go build -o bdfs ./cmd/bdfs/
```

## 运行

```bash
# 挂载到 /Volumes/bdpan
./bdfs mount \
  -mount /Volumes/bdpan \
  -bduss 'YOUR_BDUSS' \
  -stoken 'YOUR_STOKEN' \
  -remote / \
  -enable-write \
  -max-parallel 10 \
  -read-block-size 33554432 \
  -block-cache-size 536870912 \
  -disk-cache-size 10737418240 \
  -curl-timeout 60s \
  -stoken-refresh 1h

# 访问
ls /Volumes/bdpan/
```

### 完整 CLI 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-mount` | (必填) | 挂载点路径 |
| `-bduss` | (必填) | 百度 BDUSS cookie |
| `-stoken` | (必填) | 百度 STOKEN（可选，启用 PCS API 兼容模式） |
| `-use-core-pcs` | false | 使用 Core PCS API（需 -bduss + -stoken；默认走 pan-api） |
| `-remote` | `/` | 远端根路径 |
| `-enable-write` | false | 启用写操作 |
| `-allow-other` | false | 允许其他用户访问 |
| `-max-parallel` | 10 | 全局最大并发下载/上传数（默认 10） |
| `-per-file-sem` | 1 | 单文件最大并发下载块数（默认 1=串行；>1=启用单文件并行，需配合 max-parallel） |
| `-read-block-size` | 33554432 (32MB) | 读缓存块大小 |
| `-upload-slice-size` | 8388608 (8MB) | 上传分片大小 |
| `-block-cache-size` | 536870912 (512MB) | 内存缓存上限 |
| `-disk-cache-size` | 10737418240 (10GB) | 磁盘缓存上限（0=不限） |
| `-curl-timeout` | 60s | curl 单次下载超时（默认 60s） |
| `-http-timeout` | 120s | HTTP 客户端超时（默认 120s） |
| `-stoken-refresh` | 1h | STOKEN 自动刷新间隔（0=禁用） |
| `-dir-refresh-interval` | 24h | 后台目录刷新间隔（0=禁用） |
| `-dir-ttl` | 24h | 目录列表缓存 TTL（默认 24h） |
| `-metadata-ttl` | 30s | 文件 stat 缓存 TTL |
| `-dlink-ttl` | 30m | dlink 缓存 TTL |
| `-account-key` | default | 账户标识（cache key 前缀） |
| `-env-file` | (空) | **启动时读取 BDUSS/STOKEN 的文件路径**（KEY=VALUE 格式，容错注释/首尾空格；文件缺失仅记日志不打断）。也可用环境变量 `$BDFS_BDUSS`/`$BDFS_STOKEN`。优先级：命令行 flag > env-file > 环境变量 |

## 开机启动（launchd）

```bash
# 1. 安装二进制
sudo cp bdfs /usr/local/bin/bdfs

# 2. 创建 env 文件
cat > ~/.bdfs.env << 'EOF'
BDUSS=YOUR_BDUSS
STOKEN=YOUR_STOKEN
EOF
chmod 600 ~/.bdfs.env

# 3. 创建挂载点
sudo mkdir -p /Volumes/bdpan

# 4. 安装 launchd 服务（凭据只放在 ~/.bdfs.env，plist 里不要填 BDUSS/STOKEN）
cp launchd/com.baidupcs-fuse.plist ~/Library/LaunchAgents/
# 编辑 plist：仅把 /Users/YOU 替换成你的用户名即可（无需再填 BDUSS/STOKEN）
nano ~/Library/LaunchAgents/com.baidupcs-fuse.plist

# 5. 加载服务
launchctl load ~/Library/LaunchAgents/com.baidupcs-fuse.plist

# 6. 查看状态
launchctl list | grep baidupcs
tail -f ~/Library/Logs/bdfs.log
```

## 管理命令

```bash
# 停止
launchctl unload ~/Library/LaunchAgents/com.baidupcs-fuse.plist

# 重启
launchctl unload ~/Library/LaunchAgents/com.baidupcs-fuse.plist
launchctl load ~/Library/LaunchAgents/com.baidupcs-fuse.plist

# 卸载挂载点
umount /Volumes/bdpan
# 或
diskutil unmount force /Volumes/bdpan
```

## 注意事项

- **macFUSE 版本**：需 4.x+，支持 FUSE 协议 2.9
- **STOKEN 过期**：bdfs 内置 `--stoken-refresh 1h` 自动续期，只要进程不重启即可
- **BDUSS 过期**：约 30 天，过期后需更新 `~/.bdfs.env`
- **macOS 限制**：不支持 `NOTIFY`（内核主动推送），不影响正常读写
- **性能**：macOS 的 macFUSE 性能略低于 Linux，但足够日常使用
