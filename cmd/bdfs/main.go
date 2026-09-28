package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"baidupcs-fuse/adapter"
	"baidupcs-fuse/config"
	"baidupcs-fuse/fuse"
)

const appID = 266719 // BaiduPCS-Go 应用 ID（Core PCS API 鉴权默认值；pan-api 不使用此值）

// refreshRequest/Response 与 fuse 包中的定义一致（用于 Unix socket 通信）。
type refreshRequest struct {
	Type string `json:"type"` // "refresh"（默认）或 "health"
	Path string `json:"path"`
}

type refreshResponse struct {
	OK            bool     `json:"ok"`
	Message       string   `json:"message"`
	Refreshed     int      `json:"refreshed"`
	ActiveFetches int      `json:"active_fetches"`
	StuckCount    int      `json:"stuck_count"`
	StuckDetail   []string `json:"stuck_detail"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "mount":
		cmdMount(os.Args[2:])
	case "status":
		cmdStatus(os.Args[2:])
	case "recover":
		cmdRecover(os.Args[2:])
	case "refresh":
		cmdRefresh(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		// 兼容旧用法：直接当 mount 处理
		cmdMount(os.Args[1:])
	}
}

// updateEnvFile 更新 env 文件中的 STOKEN（原子写入：先写临时文件再 rename）。
func updateEnvFile(path, bduss, stoken string) error {
	tmp := path + ".tmp"
	content := fmt.Sprintf("BDUSS=%s\nSTOKEN=%s\n", bduss, stoken)
	if err := os.WriteFile(tmp, []byte(content), 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadCreds 从 KEY=VALUE 格式文件读取 BDUSS/STOKEN（与 updateEnvFile 写入格式一致）。
func loadCreds(path string) (bduss, stoken string, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	for _, line := range splitLines(string(data)) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(kv[0])) {
		case "BDUSS":
			bduss = strings.TrimSpace(kv[1])
		case "STOKEN":
			stoken = strings.TrimSpace(kv[1])
		}
	}
	return bduss, stoken, nil
}

// loadCredsFromFile 读取 -env-file 指定的凭据文件；失败仅记录日志，不中断启动。
func loadCredsFromFile(envFile *string) (bduss, stoken string) {
	if envFile == nil || *envFile == "" {
		return "", ""
	}
	b, s, err := loadCreds(*envFile)
	if err != nil {
		log.Printf("读取凭据文件 %s 失败: %v", *envFile, err)
	}
	return b, s
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "用法: bdfs <命令> [选项]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "命令:")
	fmt.Fprintln(os.Stderr, "  mount     挂载网盘到本地目录")
	fmt.Fprintln(os.Stderr, "  status    查看挂载状态和上传进度")
	fmt.Fprintln(os.Stderr, "  recover   恢复失败的上传")
	fmt.Fprintln(os.Stderr, "  refresh   刷新本地缓存")
	fmt.Fprintln(os.Stderr, "  help      显示帮助")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "示例:")
	fmt.Fprintln(os.Stderr, "  bdfs mount -mount /mnt/pan -bduss 'xxx'")
	fmt.Fprintln(os.Stderr, "  bdfs mount -mount /mnt/pan -env-file ~/.bdfs.env   # 从文件读 BDUSS/STOKEN（推荐自启，不暴露命令行）")
	fmt.Fprintln(os.Stderr, "  BDFS_BDUSS=xxx bdfs mount -mount /mnt/pan            # 或走环境变量 $BDFS_BDUSS/$BDFS_STOKEN")
	fmt.Fprintln(os.Stderr, "  bdfs status")
	fmt.Fprintln(os.Stderr, "  bdfs recover -cache-dir ~/.cache/baidupcs-fuse")
	fmt.Fprintln(os.Stderr, "  bdfs refresh -path /docs      刷新指定目录缓存")
	fmt.Fprintln(os.Stderr, "  bdfs refresh                   刷新全部缓存目录")
}

// ============================================================
// bdfs mount
// ============================================================

func cmdMount(args []string) {
	fs := flag.NewFlagSet("mount", flag.ExitOnError)
	mountPoint := fs.String("mount", "", "本地挂载点路径（必填）")
	remotePath := fs.String("remote", "/", "网盘远程路径")
	enableWrite := fs.Bool("enable-write", false, "启用写支持（Phase 5）")
	allowOther := fs.Bool("allow-other", false, "允许其他用户访问")
	cacheDir := fs.String("cache-dir", defaultCacheDir(), "缓存目录（默认 ~/.cache/baidupcs-fuse）")
	// 空值兜底，避免磁盘缓存被创建到当前工作目录的相对路径 blocks/。
	if *cacheDir == "" {
		*cacheDir = defaultCacheDir()
	}
	readBlockSize := fs.Int64("read-block-size", 32*1024*1024, "读缓存块大小（字节，默认 32MB）")
	uploadSlice := fs.Int64("upload-slice-size", 8*1024*1024, "上传分片大小（字节，默认 8MB）")
	metadataTTL := fs.Duration("metadata-ttl", 30*time.Second, "文件 stat 缓存 TTL")
	dirTTL := fs.Duration("dir-ttl", 24*time.Hour, "目录列表缓存 TTL（默认 24h）")
	dlinkTTL := fs.Duration("dlink-ttl", 30*time.Minute, "dlink 缓存 TTL")
	maxParallel := fs.Int("max-parallel", 10, "全局最大并发下载/上传数（默认 10）")
	perFileSem := fs.Int("per-file-sem", 1, "单文件最大并发下载块数（默认1=串行；>1=启用单文件并行，需配合全局 max-parallel）")
	blockCacheSize := fs.Int64("block-cache-size", 512*1024*1024, "读缓存总容量（字节，默认 512MB）")
	curlTimeout := fs.Duration("curl-timeout", 60*time.Second, "curl 单次下载超时（默认 60s）")
	httpTimeout := fs.Duration("http-timeout", 120*time.Second, "HTTP 客户端超时（默认 120s）")
	bduss := fs.String("bduss", "", "BDUSS cookie（必填，从百度网盘登录获取）")
	stoken := fs.String("stoken", "", "STOKEN（可选，启用 PCS API 兼容模式）")
	useCorePcs := fs.Bool("use-core-pcs", false, "使用 Core PCS API（需 -bduss + -stoken；默认走 pan-api）")
	stokenRefresh := fs.Duration("stoken-refresh", 60*time.Minute, "STOKEN 自动刷新间隔（0=禁用）")
	accountKey := fs.String("account-key", "default", "账户标识（cache key 前缀）")
	envFile := fs.String("env-file", "", "环境变量文件路径（刷新 STOKEN 时自动回写）")
	dirRefreshInterval := fs.Duration("dir-refresh-interval", 24*time.Hour, "后台目录刷新间隔（0=禁用）")
	diskCacheSize := fs.Int64("disk-cache-size", 10*1024*1024*1024, "磁盘缓存容量（字节，默认 10GB，0=不限）")
	fs.Parse(args)

	// 解析凭据：命令行 -bduss/-stoken > -env-file 文件 > 环境变量 $BDFS_BDUSS/$BDFS_STOKEN
	if *bduss == "" || *stoken == "" {
		b, s := loadCredsFromFile(envFile)
		if *bduss == "" && b != "" {
			*bduss = b
		}
		if *stoken == "" && s != "" {
			*stoken = s
		}
	}
	if *bduss == "" {
		*bduss = os.Getenv("BDFS_BDUSS")
	}
	if *stoken == "" {
		*stoken = os.Getenv("BDFS_STOKEN")
	}

	if *mountPoint == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须提供 -mount 参数")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "示例:")
		fmt.Fprintln(os.Stderr, "  bdfs mount -mount /mnt/pan -bduss 'xxx'")
		fmt.Fprintln(os.Stderr, "  bdfs mount -mount /mnt/pan -remote /我的音乐 -bduss 'xxx'")
		os.Exit(1)
	}

	if *bduss == "" {
		fmt.Fprintln(os.Stderr, "错误: 必须提供 -bduss 参数")
		fmt.Fprintln(os.Stderr, "获取方法: 浏览器登录 pan.baidu.com → F12 → Application → Cookies → BDUSS")
		os.Exit(1)
	}

	opts := config.MountOptions{
		MountPoint:         *mountPoint,
		RemotePath:         *remotePath,
		EnableWrite:        *enableWrite,
		AllowOther:         *allowOther,
		CacheDir:           *cacheDir,
		ReadBlockSize:      *readBlockSize,
		UploadSliceSize:    *uploadSlice,
		MetadataTTL:        *metadataTTL,
		DirTTL:             *dirTTL,
		DlinkTTL:           *dlinkTTL,
		MaxParallel:        *maxParallel,
		PerFileSem:         *perFileSem,
		BlockCacheSize:     *blockCacheSize,
		CurlTimeout:        *curlTimeout,
		HttpTimeout:        *httpTimeout,
		AccountKey:         *accountKey,
		BDUSS:              *bduss,
		DirRefreshInterval: *dirRefreshInterval,
		DiskCacheSize:      *diskCacheSize,
	}

	// 全局传输统计
	stats := adapter.NewTransferStats()

	var pcsClient adapter.PCSClient
	if *useCorePcs {
		pcsClient = adapter.NewPCSClient(appID, *bduss, *stoken, opts.HttpTimeout)
	} else {
		pcsClient = adapter.NewPanClient(*bduss, *stoken, opts.HttpTimeout)
	}
	dlinkCache := adapter.NewDlinkCache(opts.DlinkTTL)
	cloudFS := adapter.NewBaiduCloudFS(pcsClient, dlinkCache, opts.AccountKey)
	cloudFS.SetTransferStats(stats)

	// STOKEN 自动刷新
	if *stokenRefresh > 0 && *stoken != "" {
		refresher := adapter.NewStokenRefresher(*bduss, *stoken, *stokenRefresh)
		refresher.OnRefresh = func(newToken string) {
			pcsClient.UpdateStoken(newToken)
			// 回写 env 文件（供重启后使用）
			if *envFile != "" {
				if err := updateEnvFile(*envFile, *bduss, newToken); err != nil {
					log.Printf("STOKEN 回写 %s 失败: %v", *envFile, err)
				} else {
					log.Printf("STOKEN 已回写 %s", *envFile)
				}
			}
		}
		refresher.Start()
		defer refresher.Stop()
	}

	// 传输进度日志（stderr）
	progressLog := adapter.NewProgressLogger(stats, 3*time.Second)
	progressLog.Start()
	defer progressLog.Stop()

	if opts.EnableWrite {
		cacheDirPath := opts.CacheDir
		if cacheDirPath == "" {
			home, _ := os.UserHomeDir()
			cacheDirPath = home + "/.cache/baidupcs-fuse"
		}
		stagingMgr := adapter.NewStagingManager(cacheDirPath, true)
		uploadSess := adapter.NewUploadSession(pcsClient, stagingMgr)
		uploadSess.SetSliceSize(opts.UploadSliceSize)
		uploadSess.SetMaxParallel(opts.MaxParallel)
		uploadSess.SetTransferStats(stats)
		cloudFS.SetWriteSupport(stagingMgr, uploadSess)
		log.Println("写支持已启用（--enable-write）")
	}

	server, shutdownFn, err := fuse.Mount(opts, cloudFS)
	if err != nil {
		log.Fatalf("挂载失败: %v", err)
	}
	log.Printf("已挂载 %s → %s (PID %d)", opts.MountPoint, opts.RemotePath, os.Getpid())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s := <-sig
		log.Printf("收到 %v，正在退出...", s)
		// 1. 取消所有进行中的 curl 下载（立即杀 curl 子进程）
		shutdownFn()
		// 2. 直接退出：内核自动 abort 所有 pending FUSE 操作
		//    （server.Unmount() 会死锁：fusermount 等内核 drain，但 FUSE server 已阻塞）
		os.Exit(0)
	}()

	server.Wait()
	// 干净卸载（fusermount -u / 内核卸载）后触发 shutdown：取消后台上传 + 刷盘收尾
	shutdownFn()
	log.Println("已退出")
}

// ============================================================
// bdfs status
// ============================================================

func cmdStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	cacheDir := fs.String("cache-dir", defaultCacheDir(), "缓存目录")
	fs.Parse(args)

	fmt.Println("=== baidupcs-fuse 状态 ===")
	fmt.Println()

	// 读取上传进度
	progressList, err := adapter.ListProgress(*cacheDir)
	if err != nil {
		fmt.Printf("读取进度失败: %v\n", err)
	} else if len(progressList) == 0 {
		fmt.Println("活跃上传: 无")
	} else {
		fmt.Printf("活跃上传: %d 个\n", len(progressList))
		for _, p := range progressList {
			pct := 0.0
			if p.TotalSlices > 0 {
				pct = float64(p.UploadedSlices) / float64(p.TotalSlices) * 100
			}
			// 进度条
			barWidth := 30
			filled := int(pct / 100 * float64(barWidth))
			bar := ""
			for i := 0; i < barWidth; i++ {
				if i < filled {
					bar += "█"
				} else {
					bar += "░"
				}
			}
			fmt.Printf("  %s\n", p.Path)
			fmt.Printf("    [%s] %.1f%% (%d/%d 片)\n", bar, pct, p.UploadedSlices, p.TotalSlices)
			fmt.Printf("    大小: %s / %s", formatSize(p.UploadedBytes), formatSize(p.TotalSize))
			if p.SpeedBps > 0 {
				fmt.Printf(" | 速度: %s", formatSpeed(p.SpeedBps))
			}
			fmt.Printf(" | 分片: %s\n", formatSize(p.SliceSize))
		}
		stuckUploads, _ := adapter.ListStuckUploads(*cacheDir)
		if len(stuckUploads) > 0 {
			fmt.Printf("    [疑似卡死] %d 个（静默超2分钟，建议 mount recover）\n", len(stuckUploads))
		}
	}

	fmt.Println()

	// 读取失败上传
	failedPath := *cacheDir + "/failed-uploads/manifest.json"
	if data, err := os.ReadFile(failedPath); err == nil && len(data) > 0 {
		fmt.Println("失败上传:")
		lines := splitLines(string(data))
		for _, line := range lines {
			if line == "" {
				continue
			}
			parts := splitPipe(line)
			if len(parts) >= 2 {
				fmt.Printf("  %s → %s", parts[1], parts[0])
				if len(parts) >= 3 && parts[2] != "0" {
					fmt.Printf(" (%s)", formatSize(parseSize(parts[2])))
				}
				if len(parts) >= 4 && parts[3] != "" {
					fmt.Printf(" [%s]", parts[3])
				}
				fmt.Println()
			} else {
				fmt.Printf("  %s\n", line)
			}
		}
	} else {
		fmt.Println("失败上传: 无")
	}

	// 查询活跃下载与健康状态（通过 socket 连接 running mount）
	queryHealth(*cacheDir)
}

// queryHealth 通过 Unix socket 向 running mount 请求健康状态并打印。
func queryHealth(cacheDir string) {
	sockPath := filepath.Join(cacheDir, "refresh.sock")
	conn, err := net.DialTimeout("unix", sockPath, 2*time.Second)
	if err != nil {
		fmt.Println()
		fmt.Println("下载监控: 无法连接挂载进程（可能未运行或 socket 不可用）")
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

	req := refreshRequest{Type: "health"}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return
	}
	var resp refreshResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return
	}

	fmt.Println()
	fmt.Printf("下载监控: %d 个活跃 Range GET", resp.ActiveFetches)
	if resp.StuckCount > 0 {
		fmt.Printf("，⚠️ %d 疑似卡死（>30s）", resp.StuckCount)
	}
	fmt.Println()
	for _, d := range resp.StuckDetail {
		fmt.Printf("    ⏳ %s\n", d)
	}
}

// ============================================================
// bdfs recover
// ============================================================

func cmdRecover(args []string) {
	fs := flag.NewFlagSet("recover", flag.ExitOnError)
	cacheDir := fs.String("cache-dir", defaultCacheDir(), "缓存目录")
	bduss := fs.String("bduss", "", "BDUSS cookie（必填）")
	stoken := fs.String("stoken", "", "STOKEN（可选）")
	useCorePcs := fs.Bool("use-core-pcs", false, "使用 Core PCS API（需 -bduss + -stoken）")
	envFile := fs.String("env-file", "", "凭据文件路径（启动时读取 BDUSS/STOKEN）")
	fs.Parse(args)

	// 解析凭据：命令行 -bduss/-stoken > -env-file 文件 > 环境变量 $BDFS_BDUSS/$BDFS_STOKEN
	if *bduss == "" || *stoken == "" {
		b, s := loadCredsFromFile(envFile)
		if *bduss == "" && b != "" {
			*bduss = b
		}
		if *stoken == "" && s != "" {
			*stoken = s
		}
	}
	if *bduss == "" {
		*bduss = os.Getenv("BDFS_BDUSS")
	}
	if *stoken == "" {
		*stoken = os.Getenv("BDFS_STOKEN")
	}

	if *bduss == "" {
		fmt.Fprintln(os.Stderr, "错误: recover 需要 -bduss 参数（或 -bduss/-env-file/$BDFS_BDUSS）")
		os.Exit(1)
	}

	// 读取失败 manifest
	failedPath := *cacheDir + "/failed-uploads/manifest.json"
	data, err := os.ReadFile(failedPath)
	if err != nil {
		fmt.Println("没有需要恢复的上传")
		return
	}

	fmt.Println("=== 恢复失败上传 ===")
	lines := splitLines(string(data))
	recovered := 0
	failed := 0

	var pcsClient adapter.PCSClient
	if *useCorePcs {
		pcsClient = adapter.NewPCSClient(appID, *bduss, *stoken, 0)
	} else {
		pcsClient = adapter.NewPanClient(*bduss, *stoken, 0)
	}
	dlinkCache := adapter.NewDlinkCache(30 * time.Minute)
	cloudFS := adapter.NewBaiduCloudFS(pcsClient, dlinkCache, "recover")
	stagingMgr := adapter.NewStagingManager(*cacheDir, true)
	uploadSess := adapter.NewUploadSession(pcsClient, stagingMgr)
	cloudFS.SetWriteSupport(stagingMgr, uploadSess)

	for _, line := range lines {
		if line == "" {
			continue
		}
		// 解析 manifest 行: remotePath|localPath|size|error
		parts := splitPipe(line)
		if len(parts) < 2 {
			continue
		}
		remotePath := parts[0]
		localPath := parts[1]

		fmt.Printf("恢复 %s → %s ... ", localPath, remotePath)

		// 检查本地 staging 文件是否存在
		info, err := os.Stat(localPath)
		if err != nil {
			fmt.Printf("跳过（本地文件不存在: %s）\n", localPath)
			failed++
			continue
		}

		// 直接调用 UploadSession.Upload 上传 staging 文件
		_, err = uploadSess.Upload(context.Background(), localPath, remotePath, info.Size())
		if err != nil {
			fmt.Printf("失败（%v）\n", err)
			failed++
			continue
		}

		fmt.Println("成功")
		recovered++
		// 清理已恢复的 staging 文件
		os.Remove(localPath)
		os.Remove(filepath.Dir(localPath)) // 尝试删空目录
	}

	fmt.Printf("\n恢复完成: %d 成功, %d 失败\n", recovered, failed)

	// 全部成功则清理 manifest
	if failed == 0 {
		os.Remove(failedPath)
		fmt.Println("已清理 manifest")
	}
}

// ============================================================
// bdfs refresh
// ============================================================

func cmdRefresh(args []string) {
	fs := flag.NewFlagSet("refresh", flag.ExitOnError)
	cacheDir := fs.String("cache-dir", defaultCacheDir(), "缓存目录")
	path := fs.String("path", "", "要刷新的路径（空=全部缓存目录）")
	fs.Parse(args)

	// 连接 running mount 的 Unix socket
	sockPath := filepath.Join(*cacheDir, "refresh.sock")
	conn, err := net.DialTimeout("unix", sockPath, 3*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "无法连接挂载进程: %v\n", err)
		fmt.Fprintf(os.Stderr, "请确认 bdfs mount 正在运行（socket: %s）\n", sockPath)
		os.Exit(1)
	}
	defer conn.Close()

	// 发送刷新请求
	req := refreshRequest{Path: *path}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		fmt.Fprintf(os.Stderr, "发送请求失败: %v\n", err)
		os.Exit(1)
	}

	// 读取响应
	var resp refreshResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		fmt.Fprintf(os.Stderr, "读取响应失败: %v\n", err)
		os.Exit(1)
	}

	if resp.OK {
		fmt.Println(resp.Message)
	} else {
		fmt.Fprintf(os.Stderr, "刷新失败: %s\n", resp.Message)
		os.Exit(1)
	}
}

// ============================================================
// 辅助函数
// ============================================================

func defaultCacheDir() string {
	home, _ := os.UserHomeDir()
	return home + "/.cache/baidupcs-fuse"
}

func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func formatSpeed(bps float64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bps >= GB:
		return fmt.Sprintf("%.1f GB/s", bps/float64(GB))
	case bps >= MB:
		return fmt.Sprintf("%.1f MB/s", bps/float64(MB))
	case bps >= KB:
		return fmt.Sprintf("%.1f KB/s", bps/float64(KB))
	default:
		return fmt.Sprintf("%.0f B/s", bps)
	}
}

func parseSize(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func splitLines(s string) []string {
	var lines []string
	for _, line := range []byte(s) {
		_ = line
	}
	// 简单实现
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func splitPipe(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '|' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}
