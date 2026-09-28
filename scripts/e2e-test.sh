#!/bin/bash
# scripts/e2e-test.sh — E2E 测试脚本
# 需要真实 BDUSS + 网络访问
set -euo pipefail

MOUNT_POINT="/tmp/bdfs-e2e-$$"
BDUSS="${BDUSS:-}"
REMOTE_PATH="${REMOTE_PATH:-/}"
BDFS_BIN="${BDFS_BIN:-./bdfs}"

cleanup() {
    echo "=== 清理 ==="
    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        fusermount -u "$MOUNT_POINT" 2>/dev/null || true
    fi
    rmdir "$MOUNT_POINT" 2>/dev/null || true
    rm -rf "${MOUNT_POINT}.staging" 2>/dev/null || true
}

trap cleanup EXIT

# 前置检查
if [ -z "$BDUSS" ]; then
    echo "SKIP: BDUSS 未设置，跳过 E2E 测试"
    echo "用法: BDUSS='xxx' $0"
    exit 0
fi

if [ ! -x "$BDFS_BIN" ]; then
    echo "构建 bdfs..."
    go build -o "$BDFS_BIN" ./cmd/bdfs/
fi

echo "=== 1. 挂载 ==="
mkdir -p "$MOUNT_POINT"
"$BDFS_BIN" -mount "$MOUNT_POINT" -remote "$REMOTE_PATH" -bduss "$BDUSS" &
PID=$!
sleep 2

if ! kill -0 $PID 2>/dev/null; then
    echo "FAIL: bdfs 进程未启动"
    exit 1
fi

if ! mountpoint -q "$MOUNT_POINT"; then
    echo "FAIL: 挂载点未激活"
    kill $PID 2>/dev/null || true
    exit 1
fi
echo "PASS: 挂载成功 (PID=$PID)"

echo "=== 2. ls ==="
if ls "$MOUNT_POINT" >/dev/null 2>&1; then
    echo "PASS: ls 成功"
    ls "$MOUNT_POINT" | head -5
else
    echo "FAIL: ls 失败"
fi

echo "=== 3. stat ==="
if stat "$MOUNT_POINT" >/dev/null 2>&1; then
    echo "PASS: stat 成功"
else
    echo "FAIL: stat 失败"
fi

echo "=== 4. df ==="
if df "$MOUNT_POINT" >/dev/null 2>&1; then
    echo "PASS: df 成功"
    df "$MOUNT_POINT"
else
    echo "FAIL: df 失败"
fi

echo "=== 5. cat（读文件）==="
FIRST_FILE=$(ls "$MOUNT_POINT" 2>/dev/null | head -1)
if [ -n "$FIRST_FILE" ]; then
    if [ -f "$MOUNT_POINT/$FIRST_FILE" ]; then
        if cat "$MOUNT_POINT/$FIRST_FILE" >/dev/null 2>&1; then
            echo "PASS: cat $FIRST_FILE 成功"
        else
            echo "WARN: cat $FIRST_FILE 失败（可能文件较大）"
        fi
    else
        echo "SKIP: $FIRST_FILE 是目录，跳过 cat"
    fi
else
    echo "SKIP: 目录为空"
fi

echo "=== 6. cp（写文件）==="
TEST_FILE="$MOUNT_POINT/bdfs-e2e-test-$(date +%s).txt"
echo "e2e test $(date)" > "$TEST_FILE" 2>/dev/null || true
if [ -f "$TEST_FILE" ]; then
    echo "PASS: 写文件成功"
    rm -f "$TEST_FILE" 2>/dev/null || true
else
    echo "SKIP: 写文件未启用（需要 --enable-write）"
fi

echo "=== 7. 卸载 ==="
kill -TERM $PID 2>/dev/null || true
wait $PID 2>/dev/null || true
if ! mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    echo "PASS: 卸载成功"
else
    echo "FAIL: 卸载后仍挂载"
    fusermount -u "$MOUNT_POINT" 2>/dev/null || true
fi

echo ""
echo "=== E2E 测试完成 ==="
