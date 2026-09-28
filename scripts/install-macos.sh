#!/bin/bash
# bdfs macOS 一键安装脚本
# 用法: ./install-macos.sh -b BDUSS -s STOKEN

set -e

BDUSS=""
STOKEN=""
MOUNT_POINT="/Volumes/bdpan"
INSTALL_DIR="/usr/local/bin"

usage() {
    echo "用法: $0 -b <BDUSS> -s <STOKEN> [-m <挂载点>] [-i <安装目录>]"
    echo ""
    echo "选项:"
    echo "  -b    BDUSS cookie（必填）"
    echo "  -s    STOKEN（必填）"
    echo "  -m    挂载点路径（默认 /Volumes/bdpan）"
    echo "  -i    二进制安装目录（默认 /usr/local/bin）"
    exit 1
}

while getopts "b:s:m:i:" opt; do
    case $opt in
        b) BDUSS="$OPTARG" ;;
        s) STOKEN="$OPTARG" ;;
        m) MOUNT_POINT="$OPTARG" ;;
        i) INSTALL_DIR="$OPTARG" ;;
        *) usage ;;
    esac
done

[ -z "$BDUSS" ] || [ -z "$STOKEN" ] && usage

echo "=== bdfs macOS 安装 ==="
echo "挂载点: $MOUNT_POINT"
echo "安装目录: $INSTALL_DIR"
echo ""

# 1. 检查 macFUSE
if ! kextstat | grep -q macfuse; then
    echo "⚠️  macFUSE 未安装"
    echo "   请先运行: brew install --cask macfuse"
    echo "   然后重启电脑"
    exit 1
fi
echo "✅ macFUSE 已安装"

# 2. 编译 bdfs
echo "🔨 编译 bdfs..."
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"
go build -o /tmp/bdfs ./cmd/bdfs/
echo "✅ 编译完成"

# 3. 安装二进制
echo "📦 安装到 $INSTALL_DIR/bdfs..."
sudo cp /tmp/bdfs "$INSTALL_DIR/bdfs"
sudo chmod +x "$INSTALL_DIR/bdfs"
echo "✅ 安装完成"

# 4. 创建挂载点
echo "📁 创建挂载点 $MOUNT_POINT..."
sudo mkdir -p "$MOUNT_POINT"
echo "✅ 挂载点就绪"

# 5. 创建 env 文件
ENV_FILE="$HOME/.bdfs.env"
echo "🔑 创建 env 文件 $ENV_FILE..."
cat > "$ENV_FILE" << EOF
BDUSS=$BDUSS
STOKEN=$STOKEN
EOF
chmod 600 "$ENV_FILE"
echo "✅ env 文件就绪"

# 6. 创建日志目录
mkdir -p "$HOME/Library/Logs"

# 7. 生成 launchd plist
PLIST_FILE="$HOME/Library/LaunchAgents/com.baidupcs-fuse.plist"
CACHE_DIR="$HOME/.cache/baidupcs-fuse"
mkdir -p "$CACHE_DIR"

cat > "$PLIST_FILE" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.baidupcs-fuse</string>
    <key>ProgramArguments</key>
    <array>
        <string>$INSTALL_DIR/bdfs</string>
        <string>mount</string>
        <string>-mount</string>
        <string>$MOUNT_POINT</string>
        <string>-bduss</string>
        <string>$BDUSS</string>
        <string>-stoken</string>
        <string>$STOKEN</string>
        <string>-remote</string>
        <string>/</string>
        <string>-cache-dir</string>
        <string>$CACHE_DIR</string>
        <string>-enable-write</string>
        <string>-max-parallel</string>
        <string>10</string>
        <string>-per-file-sem</string>
        <string>3</string>
        <string>-read-block-size</string>
        <string>33554432</string>
        <string>-upload-slice-size</string>
        <string>8388608</string>
        <string>-block-cache-size</string>
        <string>536870912</string>
        <string>-curl-timeout</string>
        <string>60s</string>
        <string>-stoken-refresh</string>
        <string>1h</string>
        <string>-env-file</string>
        <string>$ENV_FILE</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>$HOME/Library/Logs/bdfs.log</string>
    <key>StandardErrorPath</key>
    <string>$HOME/Library/Logs/bdfs.log</string>
    <key>WorkingDirectory</key>
    <string>$HOME</string>
</dict>
</plist>
EOF
echo "✅ launchd plist 就绪"

# 8. 加载服务
echo "🚀 启动服务..."
launchctl unload "$PLIST_FILE" 2>/dev/null || true
launchctl load "$PLIST_FILE"
echo "✅ 服务已启动"

# 9. 等待挂载
sleep 3
if [ -d "$MOUNT_POINT" ] && ls "$MOUNT_POINT" >/dev/null 2>&1; then
    echo ""
    echo "🎉 安装成功！"
    echo "   挂载点: $MOUNT_POINT"
    echo "   日志:   $HOME/Library/Logs/bdfs.log"
    echo "   管理:   launchctl [stop|start] com.baidupcs-fuse"
else
    echo ""
    echo "⚠️  服务已启动，但挂载点可能还未就绪"
    echo "   请检查日志: tail -f $HOME/Library/Logs/bdfs.log"
fi
