package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"baidupcs-fuse/adapter"
)

// 目的：mount 参数必须按“命令行 > env-file > 环境变量”解析，账户目录隔离且
// 缺少必填参数/目录不可建时在挂载前失败。
func TestPrepareMount_ParsingAndValidation(t *testing.T) {
	root := t.TempDir()
	mnt := t.TempDir()

	t.Run("missing mount", func(t *testing.T) {
		_, err := prepareMount([]string{"-bduss", "b", "-cache-dir", root, "-stoken-refresh", "0"}, func(string) string { return "" })
		if err == nil || !strings.Contains(err.Error(), "-mount") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("missing bduss", func(t *testing.T) {
		_, err := prepareMount([]string{"-mount", mnt, "-cache-dir", root, "-stoken-refresh", "0"}, func(string) string { return "" })
		if err == nil || !strings.Contains(err.Error(), "-bduss") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("env fallback and write support", func(t *testing.T) {
		env := func(k string) string {
			if k == "BDFS_BDUSS" {
				return "env-bduss"
			}
			return ""
		}
		setup, err := prepareMount([]string{
			"-mount", mnt, "-cache-dir", root, "-enable-write",
			"-account-key", "acct", "-remote", "/docs/", "-stoken-refresh", "0",
		}, env)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(setup.stop)
		if setup.opts.BDUSS != "env-bduss" || setup.opts.AccountKey != "acct" || !setup.opts.EnableWrite {
			t.Fatalf("opts=%+v", setup.opts)
		}
		if setup.opts.CacheDir != filepath.Join(root, "acct") {
			t.Fatalf("cache dir=%s", setup.opts.CacheDir)
		}
		if setup.opts.RemotePath != "/docs/" {
			t.Fatalf("remote path=%s", setup.opts.RemotePath)
		}
		cur, err := os.ReadFile(filepath.Join(root, "current"))
		if err != nil || strings.TrimSpace(string(cur)) != "acct" {
			t.Fatalf("current=%q err=%v", cur, err)
		}
	})

	t.Run("env file and bduss file precedence", func(t *testing.T) {
		envFile := filepath.Join(t.TempDir(), "baidu.env")
		if err := os.WriteFile(envFile, []byte("BDUSS=file-bduss\nSTOKEN=file-stoken\n"), 0600); err != nil {
			t.Fatal(err)
		}
		setup, err := prepareMount([]string{
			"-mount", mnt, "-cache-dir", root, "-env-file", envFile,
			"-stoken-refresh", "0", "-use-core-pcs",
		}, func(string) string { return "" })
		if err != nil {
			t.Fatal(err)
		}
		setup.stop()
		if setup.opts.BDUSS != "file-bduss" {
			t.Fatalf("bduss=%s", setup.opts.BDUSS)
		}

		bdussFile := filepath.Join(t.TempDir(), "bduss")
		if err := os.WriteFile(bdussFile, []byte("  file-only  \n"), 0600); err != nil {
			t.Fatal(err)
		}
		setup, err = prepareMount([]string{
			"-mount", mnt, "-cache-dir", root, "-bduss-file", bdussFile,
			"-stoken-refresh", "0",
		}, func(string) string { return "" })
		if err != nil {
			t.Fatal(err)
		}
		setup.stop()
		if setup.opts.BDUSS != "file-only" {
			t.Fatalf("bduss-file=%q", setup.opts.BDUSS)
		}
	})

	t.Run("command line bduss wins over env", func(t *testing.T) {
		setup, err := prepareMount([]string{
			"-mount", mnt, "-cache-dir", root, "-bduss", "cli",
			"-stoken-refresh", "0",
		}, func(string) string { return "env" })
		if err != nil {
			t.Fatal(err)
		}
		setup.stop()
		if setup.opts.BDUSS != "cli" {
			t.Fatalf("bduss=%q", setup.opts.BDUSS)
		}
	})

	t.Run("traversal account key is hashed", func(t *testing.T) {
		setup, err := prepareMount([]string{
			"-mount", mnt, "-cache-dir", root, "-bduss", "b",
			"-account-key", "../../evil", "-stoken-refresh", "0",
		}, func(string) string { return "" })
		if err != nil {
			t.Fatal(err)
		}
		setup.stop()
		if strings.ContainsAny(setup.opts.AccountKey, `/\`) || strings.Contains(setup.opts.CacheDir, "..") {
			t.Fatalf("unsanitized key=%q dir=%q", setup.opts.AccountKey, setup.opts.CacheDir)
		}
		if filepath.Dir(setup.opts.CacheDir) != root {
			t.Fatalf("cache dir escaped root: %s", setup.opts.CacheDir)
		}
	})

	t.Run("cache dir is file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := prepareMount([]string{
			"-mount", mnt, "-cache-dir", file, "-bduss", "b",
			"-stoken-refresh", "0",
		}, func(string) string { return "" })
		if err == nil || !strings.Contains(err.Error(), "创建缓存目录") {
			t.Fatalf("err=%v", err)
		}
	})
}

// 目的：status 必须展示进度/卡死/失败清单，并区分 socket 可用与不可用；
// 损坏的 manifest 要报告错误而不是崩溃。
func TestRunStatus_OutputAndHealth(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "bdfs-status-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	accountDir := filepath.Join(root, "acct")
	if err := os.MkdirAll(accountDir, 0700); err != nil {
		t.Fatal(err)
	}

	now := time.Now().Format(time.RFC3339)
	old := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
	if err := adapter.WriteProgress(accountDir, &adapter.UploadProgress{
		Path: "/remote/a", TotalSize: 4 << 20, SliceSize: 1 << 20,
		TotalSlices: 4, UploadedSlices: 2, UploadedBytes: 2 << 20,
		LastActivityAt: now, StartedAt: now, SpeedBps: 1.5 * float64(1<<20),
	}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.WriteProgress(accountDir, &adapter.UploadProgress{
		Path: "/remote/stuck", TotalSlices: 0, LastActivityAt: old, StartedAt: old,
	}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.SaveFailedUpload(accountDir, adapter.FailedUploadEntry{
		RemotePath: "/remote/bad", LocalPath: "/tmp/bad", Size: 2 << 20, Error: "boom",
	}); err != nil {
		t.Fatal(err)
	}
	if err := adapter.SaveFailedUpload(accountDir, adapter.FailedUploadEntry{
		RemotePath: "/remote/nosize", LocalPath: "/tmp/nosize",
	}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	runStatus(root, "acct", &out)
	text := out.String()
	for _, want := range []string{
		"活跃上传: 2 个", "50.0%", "█", "░", "1.5 MB/s", "疑似卡死] 1 个",
		"失败上传:", "(2.0 MB)", "[boom]", "无法连接挂载进程",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("status missing %q:\n%s", want, text)
		}
	}

	// socket 可用：健康状态与卡死详情必须输出。
	sock := filepath.Join(accountDir, "refresh.sock")
	serveUnixOnce(t, sock, func(conn net.Conn, _ refreshRequest) {
		_ = json.NewEncoder(conn).Encode(refreshResponse{
			OK: true, ActiveFetches: 3, StuckCount: 2,
			StuckDetail: []string{"block 1", "block 2"},
		})
	})
	out.Reset()
	runStatus(root, "acct", &out)
	text = out.String()
	if !strings.Contains(text, "3 个活跃 Range GET") || !strings.Contains(text, "2 疑似卡死") || !strings.Contains(text, "⏳ block 1") {
		t.Fatalf("health output:\n%s", text)
	}

	// 空账户与损坏清单。
	empty := filepath.Join(root, "empty")
	if err := os.MkdirAll(empty, 0700); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	runStatus(root, "empty", &out)
	if !strings.Contains(out.String(), "活跃上传: 无") || !strings.Contains(out.String(), "失败上传: 无") {
		t.Fatalf("empty status:\n%s", out.String())
	}

	corrupt := filepath.Join(root, "corrupt", "failed-uploads")
	if err := os.MkdirAll(corrupt, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corrupt, "manifest.json"), []byte("{bad"), 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	runStatus(root, "corrupt", &out)
	if !strings.Contains(out.String(), "读取失败上传清单失败") {
		t.Fatalf("corrupt manifest output:\n%s", out.String())
	}
}

// 目的：recover 必须分别处理本地缺失、尺寸不完整、上传失败、成功四类记录，
// 并保证 manifest 与 staging 的清理/保留符合可重试语义。
func TestRecoverEntries_AllOutcomes(t *testing.T) {
	cacheDir := t.TempDir()
	mk := func(dir, name, content string) string {
		d := filepath.Join(cacheDir, dir)
		if err := os.MkdirAll(d, 0700); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(d, name)
		if err := os.WriteFile(p, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	okPath := mk("up-ok", "data", "ok")
	badSize := mk("up-bad", "data", "ab")
	errPath := mk("up-err", "data", "e")

	entries := []adapter.FailedUploadEntry{
		{RemotePath: "/missing", LocalPath: filepath.Join(cacheDir, "gone", "data"), Size: 1},
		{RemotePath: "/bad-size", LocalPath: badSize, Size: 5},
		{RemotePath: "/err", LocalPath: errPath, Size: 1},
		{RemotePath: "/ok", LocalPath: okPath, Size: 2},
	}
	for _, e := range entries {
		if err := adapter.SaveFailedUpload(cacheDir, e); err != nil {
			t.Fatal(err)
		}
	}

	upload := func(_ context.Context, _, remote string, _ int64) (*adapter.UploadResult, error) {
		if remote == "/err" {
			return nil, os.ErrPermission
		}
		return &adapter.UploadResult{Path: remote}, nil
	}
	var out bytes.Buffer
	recovered, failed := recoverEntries(cacheDir, entries, upload, &out)
	if recovered != 1 || failed != 2 {
		t.Fatalf("recovered=%d failed=%d\n%s", recovered, failed, out.String())
	}
	for _, want := range []string{"本地暂存已不存在", "本地暂存不完整", "失败（permission denied）", "成功", "1 成功, 2 失败"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q:\n%s", want, out.String())
		}
	}

	left, err := adapter.LoadFailedUploads(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	byRemote := make(map[string]adapter.FailedUploadEntry)
	for _, e := range left {
		byRemote[e.RemotePath] = e
	}
	if _, ok := byRemote["/missing"]; ok {
		t.Fatal("missing-local record should be removed")
	}
	if _, ok := byRemote["/ok"]; ok {
		t.Fatal("successful record should be removed")
	}
	if e, ok := byRemote["/err"]; !ok || !strings.Contains(e.Error, "permission") {
		t.Fatalf("failed record not updated: %+v", e)
	}
	if _, ok := byRemote["/bad-size"]; !ok {
		t.Fatal("incomplete record must be kept")
	}
	if _, err := os.Stat(okPath); !os.IsNotExist(err) {
		t.Fatalf("successful staging not removed: %v", err)
	}

	out.Reset()
	recovered, failed = recoverEntries(cacheDir, nil, upload, &out)
	if recovered != 0 || failed != 0 || !strings.Contains(out.String(), "恢复完成: 0 成功, 0 失败") {
		t.Fatalf("empty recover recovered=%d failed=%d out=%s", recovered, failed, out.String())
	}
}

// 目的：refresh 必须发送 path/type、处理成功、服务端失败、坏响应、无 socket，
// 而不是静默成功。
func TestRunRefresh_RequestFlows(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "bdfs-refresh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	accountDir := filepath.Join(root, "acct")
	if err := os.MkdirAll(accountDir, 0700); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(accountDir, "refresh.sock")

	serveUnixOnce(t, sock, func(conn net.Conn, req refreshRequest) {
		resp := refreshResponse{OK: true, Message: "已刷新 " + req.Type + " " + req.Path}
		_ = json.NewEncoder(conn).Encode(resp)
	})
	var out bytes.Buffer
	if err := runRefresh(root, "acct", "/docs", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "已刷新 refresh /docs") {
		t.Fatalf("out=%q", out.String())
	}

	serveUnixOnce(t, sock, func(conn net.Conn, _ refreshRequest) {
		_ = json.NewEncoder(conn).Encode(refreshResponse{OK: false, Message: "boom"})
	})
	if err := runRefresh(root, "acct", "", &out); err == nil || !strings.Contains(err.Error(), "刷新失败: boom") {
		t.Fatalf("err=%v", err)
	}

	serveUnixOnce(t, sock, func(conn net.Conn, _ refreshRequest) {
		conn.Close()
	})
	if err := runRefresh(root, "acct", "", &out); err == nil || !strings.Contains(err.Error(), "读取响应失败") {
		t.Fatalf("closed err=%v", err)
	}

	serveUnixOnce(t, sock, func(conn net.Conn, _ refreshRequest) {
		_, _ = conn.Write([]byte("not-json"))
	})
	if err := runRefresh(root, "acct", "", &out); err == nil || !strings.Contains(err.Error(), "读取响应失败") {
		t.Fatalf("bad json err=%v", err)
	}

	os.Remove(sock)
	if err := runRefresh(root, "acct", "", &out); err == nil || !strings.Contains(err.Error(), "无法连接挂载进程") {
		t.Fatalf("no socket err=%v", err)
	}
}

// 目的：recover 参数必须支持 env-file/环境变量回退并派生账户目录，
// 缺少凭据时在加锁前失败。
func TestParseRecoverOptions(t *testing.T) {
	root := t.TempDir()
	if _, err := parseRecoverOptions([]string{"-cache-dir", root}, func(string) string { return "" }); err == nil {
		t.Fatal("missing bduss should fail")
	}
	env := func(k string) string {
		switch k {
		case "BDFS_BDUSS":
			return "env-b"
		case "BDFS_STOKEN":
			return "env-s"
		}
		return ""
	}
	o, err := parseRecoverOptions([]string{"-cache-dir", root, "-use-core-pcs"}, env)
	if err != nil {
		t.Fatal(err)
	}
	if o.bduss != "env-b" || o.stoken != "env-s" || !o.useCorePCS {
		t.Fatalf("opts=%+v", o)
	}
	if !strings.HasPrefix(o.cacheDir, root+string(os.PathSeparator)) {
		t.Fatalf("cache dir=%s", o.cacheDir)
	}

	envFile := filepath.Join(t.TempDir(), "env")
	if err := os.WriteFile(envFile, []byte("BDUSS=file-b\nSTOKEN=file-s\n"), 0600); err != nil {
		t.Fatal(err)
	}
	o, err = parseRecoverOptions([]string{"-cache-dir", root, "-env-file", envFile}, func(string) string { return "" })
	if err != nil || o.bduss != "file-b" || o.stoken != "file-s" {
		t.Fatalf("env file opts=%+v err=%v", o, err)
	}

	bdussFile := filepath.Join(t.TempDir(), "bduss")
	if err := os.WriteFile(bdussFile, []byte("file-only\n"), 0600); err != nil {
		t.Fatal(err)
	}
	o, err = parseRecoverOptions([]string{"-cache-dir", root, "-bduss-file", bdussFile}, func(string) string { return "" })
	if err != nil || o.bduss != "file-only" {
		t.Fatalf("bduss file opts=%+v err=%v", o, err)
	}
}

// 目的：账户派生/current 文件/路径穿越处理必须稳定且不把原始 BDUSS 写入路径。
func TestAccountHelpers(t *testing.T) {
	if deriveAccountKey("") != "default" {
		t.Fatal("empty bduss must map to default")
	}
	k1 := deriveAccountKey("secret")
	if len(k1) != 12 || k1 != deriveAccountKey("secret") || strings.Contains(k1, "secret") {
		t.Fatalf("derived key=%q", k1)
	}

	root := t.TempDir()
	key, dir := resolveAccountDir(root, "explicit", "")
	if key != "explicit" || dir != filepath.Join(root, "explicit") {
		t.Fatalf("explicit=%q/%q", key, dir)
	}
	key, dir = resolveAccountDir(root, "", "bduss")
	if key != deriveAccountKey("bduss") || dir != filepath.Join(root, key) {
		t.Fatalf("derived=%q/%q", key, dir)
	}
	key, dir = resolveAccountDir(root, "../evil", "")
	if strings.ContainsAny(key, `/\`) || strings.Contains(dir, "..") {
		t.Fatalf("traversal=%q/%q", key, dir)
	}
	key, _ = resolveAccountDir(root, "", "")
	if key != "default" {
		t.Fatalf("fallback key=%q", key)
	}

	writeCurrentAccount(root, "acct")
	data, err := os.ReadFile(filepath.Join(root, "current"))
	if err != nil || strings.TrimSpace(string(data)) != "acct" {
		t.Fatalf("current=%q err=%v", data, err)
	}
	key, dir = resolveAccountDir(root, "", "")
	if key != "acct" || dir != filepath.Join(root, "acct") {
		t.Fatalf("current resolve=%q/%q", key, dir)
	}
	// 写 current 的父目录不可建时安全返回，不 panic。
	blocked := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocked, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	writeCurrentAccount(blocked, "x")
}

// 目的：env 文件回写必须原子、0600，并在目标不可写时返回错误。
func TestUpdateEnvFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "baidu.env")
	if err := updateEnvFile(path, "b", "s"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "BDUSS=b\nSTOKEN=s\n" {
		t.Fatalf("data=%q err=%v", data, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("mode=%v err=%v", info.Mode(), err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind: %v", err)
	}
	if err := updateEnvFile(filepath.Join(dir, "missing", "x"), "b", "s"); err == nil {
		t.Fatal("write into missing dir should fail")
	}
}

// 目的：大小/速度/行解析与默认缓存目录输出格式必须覆盖各量级边界。
func TestFormatHelpers(t *testing.T) {
	for _, tc := range []struct {
		in   int64
		want string
	}{
		{0, "0 B"}, {1023, "1023 B"}, {1024, "1.0 KB"},
		{2 << 20, "2.0 MB"}, {3 << 30, "3.0 GB"},
	} {
		if got := formatSize(tc.in); got != tc.want {
			t.Fatalf("formatSize(%d)=%q want %q", tc.in, got, tc.want)
		}
	}
	for _, tc := range []struct {
		in   float64
		want string
	}{
		{0, "0 B/s"}, {1024, "1.0 KB/s"}, {2 << 20, "2.0 MB/s"}, {3 << 30, "3.0 GB/s"},
	} {
		if got := formatSpeed(tc.in); got != tc.want {
			t.Fatalf("formatSpeed(%f)=%q want %q", tc.in, got, tc.want)
		}
	}
	if parseSize("123") != 123 || parseSize("bad") != 0 {
		t.Fatal("parseSize mismatch")
	}
	if got := splitLines("a\nb\n"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("splitLines=%v", got)
	}
	if got := splitLines(""); len(got) != 0 {
		t.Fatalf("empty split=%v", got)
	}
	if got := splitLines("a\r\nb"); len(got) != 2 || got[1] != "b" {
		t.Fatalf("crlf split=%v", got)
	}
	if !strings.Contains(defaultCacheDir(), ".cache/baidupcs-fuse") {
		t.Fatalf("defaultCacheDir=%q", defaultCacheDir())
	}
}

// serveUnixOnce 监听一次 Unix socket，先读取请求再交给 handler。
// 必须先读请求：服务端若在客户端 Encode 完成前写响应并关闭，Linux 上客户端可能
// 收到 EPIPE/ECONNRESET，导致测试误判为健康查询失败。
func serveUnixOnce(t *testing.T, path string, handler func(net.Conn, refreshRequest)) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		var req refreshRequest
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			return
		}
		handler(conn, req)
	}()
}

// 目的：status/recover/refresh 命令入口必须把参数正确传给内部实现，
// 并在正常路径输出可读结果（不依赖 os.Exit 的失败路径单独覆盖）。
func TestCommandEntries(t *testing.T) {
	root := t.TempDir()
	acct := filepath.Join(root, "acct")
	if err := os.MkdirAll(acct, 0700); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		cmdStatus([]string{"-cache-dir", root, "-account-key", "acct"})
	})
	if !strings.Contains(out, "=== baidupcs-fuse 状态 ===") || !strings.Contains(out, "活跃上传: 无") {
		t.Fatalf("status output:\n%s", out)
	}

	// recover：本地 staging 已不存在 → 删除记录后正常返回。
	if err := adapter.SaveFailedUpload(acct, adapter.FailedUploadEntry{
		RemotePath: "/gone", LocalPath: filepath.Join(acct, "gone", "data"), Size: 1,
	}); err != nil {
		t.Fatal(err)
	}
	out = captureStdout(t, func() {
		cmdRecover([]string{"-cache-dir", root, "-account-key", "acct", "-bduss", "dummy"})
	})
	if !strings.Contains(out, "本地暂存已不存在") || !strings.Contains(out, "0 成功, 0 失败") {
		t.Fatalf("recover output:\n%s", out)
	}
	if left, err := adapter.LoadFailedUploads(acct); err != nil || len(left) != 0 {
		t.Fatalf("manifest not cleaned: %+v err=%v", left, err)
	}

	// recover 空清单路径。
	emptyRoot := t.TempDir()
	out = captureStdout(t, func() {
		cmdRecover([]string{"-cache-dir", emptyRoot, "-account-key", "acct", "-bduss", "dummy"})
	})
	if !strings.Contains(out, "没有需要恢复的上传") {
		t.Fatalf("empty recover output:\n%s", out)
	}

	// refresh：通过真实 Unix socket 往返。
	sock := filepath.Join(acct, "refresh.sock")
	serveUnixOnce(t, sock, func(conn net.Conn, _ refreshRequest) {
		_ = json.NewEncoder(conn).Encode(refreshResponse{OK: true, Message: "刷新完成"})
	})
	out = captureStdout(t, func() {
		cmdRefresh([]string{"-cache-dir", root, "-account-key", "acct", "-path", "/docs"})
	})
	if !strings.Contains(out, "刷新完成") {
		t.Fatalf("refresh output:\n%s", out)
	}
}

// 目的：status 读取进度失败时必须报告错误而不是静默显示为空。
func TestRunStatus_ProgressReadError(t *testing.T) {
	root := t.TempDir()
	acct := filepath.Join(root, "acct", "uploads")
	if err := os.MkdirAll(acct, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(acct, "progress"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	runStatus(root, "acct", &out)
	if !strings.Contains(out.String(), "读取进度失败") {
		t.Fatalf("output:\n%s", out.String())
	}
}

// 目的：帮助文本必须列出全部子命令，避免 CLI 入口无提示退出。
func TestPrintUsage(t *testing.T) {
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	printUsage()
	_ = w.Close()
	os.Stderr = old
	data, _ := io.ReadAll(r)
	for _, cmd := range []string{"mount", "status", "recover", "refresh", "help"} {
		if !strings.Contains(string(data), cmd) {
			t.Fatalf("usage missing %q:\n%s", cmd, data)
		}
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(r)
		done <- string(data)
	}()
	fn()
	_ = w.Close()
	os.Stdout = old
	return <-done
}
