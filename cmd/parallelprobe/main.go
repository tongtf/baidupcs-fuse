package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"baidupcs-fuse/adapter"
	"baidupcs-fuse/download"
	"baidupcs-fuse/mount"
)

type fileEntry struct {
	path string
	size int64
}

func parseCreds(path string) (bduss, stoken string, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "BDUSS":
			bduss = strings.Trim(fields[1], `"`)
		case "stoken", "STOKEN":
			stoken = strings.Trim(fields[1], `"`)
		}
	}
	if bduss == "" {
		return "", "", fmt.Errorf("未从 %s 解析到 BDUSS", path)
	}
	return bduss, stoken, nil
}

func newClient(bduss, stoken string) adapter.PCSClient {
	return adapter.NewPanClient(bduss, stoken, 120*time.Second)
}

// ensureDir 递归创建目录（百度 API 要求父目录存在）。
func ensureDir(ctx context.Context, pc adapter.PCSClient, path string) error {
	if path == "" || path == "/" {
		return nil
	}
	parent := path[:strings.LastIndex(strings.TrimRight(path, "/"), "/")]
	if parent == "" || parent == "/" {
		parent = "/"
	}
	if err := ensureDir(ctx, pc, parent); err != nil {
		return err
	}
	return pc.Mkdir(ctx, path)
}

// uploadFile 先秒传，命中则直接落盘；否则退化为分片上传。
func uploadFile(ctx context.Context, pc adapter.PCSClient, localPath, remoteDir string) error {
	localPath = filepath.Clean(localPath)
	remotePath := strings.TrimRight(remoteDir, "/") + "/" + filepath.Base(localPath)

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open local: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	sliceSize := int64(8 * 1024 * 1024)
	nSlices := (size + sliceSize - 1) / sliceSize
	if nSlices > 1024 {
		sliceSize = (size + 1023) / 1024
	}
	fmt.Printf("本地文件: %s  size=%d (%.2f GB)\n", localPath, size, float64(size)/(1024*1024*1024))

	blockListMD5 := make([]string, 0, nSlices)
	buf := make([]byte, sliceSize)
	contentHash := md5.New()

	// pass1: 整文件 content-md5 + 逐片 block-md5（秒传匹配键）
	for {
		n := readFull(f, buf)
		if n == 0 {
			break
		}
		contentHash.Write(buf[:n])
		sum := md5.Sum(buf[:n])
		blockListMD5 = append(blockListMD5, hex.EncodeToString(sum[:]))
	}
	f.Close()
	contentMD5 := hex.EncodeToString(contentHash.Sum(nil))

	if err := ensureDir(ctx, pc, filepath.Dir(remotePath)); err != nil {
		return fmt.Errorf("创建目录失败: %w", err)
	}

	// 秒传：content-md5 命中网盘已有文件则直接落盘（不产生实际流量）。
	fmt.Printf("尝试秒传 (content-md5=%s)...\n", contentMD5[:16])
	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	rapidErr := pc.RapidUpload(rctx, remotePath, size, contentMD5, "", "", "", "", blockListMD5)
	cancel()
	if rapidErr == nil {
		fmt.Printf("秒传成功: %s\n", remotePath)
		return nil
	}
	if errors.Is(rapidErr, mount.ErrNeedUpload) || isRapidNotFound(rapidErr) {
		fmt.Printf("未命中秒传 (%v)，退化为分片上传\n", rapidErr)
	} else {
		fmt.Printf("秒传请求异常 (%v)，仍尝试分片上传\n", rapidErr)
	}

	return slowUpload(ctx, pc, localPath, remotePath, size, sliceSize, blockListMD5)
}

// slowUpload 分片上传：precreate → pass2 逐片传数据 → create 落盘。
func slowUpload(ctx context.Context, pc adapter.PCSClient, localPath, remotePath string, size, sliceSize int64, blockListMD5 []string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	nSlices := (size + sliceSize - 1) / sliceSize
	checksumMap := make(map[int]string, len(blockListMD5))
	for i, h := range blockListMD5 {
		checksumMap[i] = h
	}
	buf := make([]byte, sliceSize)

	uploadID, err := pc.PrecreateUpload(ctx, remotePath, size, blockListMD5)
	if err != nil {
		return fmt.Errorf("precreate: %w", err)
	}
	if uploadID == "" {
		return fmt.Errorf("precreate 返回空 uploadid（文件可能已存在或走秒传）")
	}

	fmt.Printf("分片上传: %s (%.2f GB), %d 片\n", filepath.Base(remotePath), float64(size)/(1024*1024*1024), nSlices)
	t0 := time.Now()
	for seq := int64(0); seq < nSlices; seq++ {
		n := readFull(f, buf)
		if n == 0 {
			break
		}
		offset := seq * sliceSize
		if _, err := pc.UploadSlice(ctx, uploadID, remotePath, int(seq), offset, buf[:n]); err != nil {
			return fmt.Errorf("upload slice %d: %w", seq, err)
		}
		if (seq+1)%32 == 0 || seq+1 == nSlices {
			fmt.Printf("  已传 %d/%d 片 (%.0f%%), %.0fs\n",
				seq+1, nSlices, float64(seq+1)/float64(nSlices)*100, time.Since(t0).Seconds())
		}
	}
	if err := pc.CreateSuperFile(ctx, uploadID, remotePath, size, checksumMap); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	fmt.Printf("分片上传完成: %s  耗时 %.0fs\n", remotePath, time.Since(t0).Seconds())
	return nil
}

// isRapidNotFound 将"网盘不存在该文件"类错误识别为秒传未命中（非致命）。
func isRapidNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, k := range []string{"needupload", "不存在", "not exist", "不存在于网盘"} {
		if strings.Contains(msg, k) {
			return true
		}
	}
	return false
}

// readFull 读入 buf，返回实际字节数（EOF 或读到满）。
func readFull(f *os.File, buf []byte) int {
	total := 0
	for total < len(buf) {
		n, err := f.Read(buf[total:])
		total += n
		if err != nil {
			break
		}
	}
	return total
}

// diag 用几十 KB 小请求试不同头组合，找出当前 CDN 节点接受哪套。
func diag(ctx context.Context, dlink string) error {
	fmt.Printf("诊断 dlink 节点头部策略: %s\n", strings.Split(dlink, "/")[2])

	apply := func(h http.Header) {
		h.Set("User-Agent", "pan.baidu.com")
	}
	cases := []struct {
		name string
		fn   func(http.Header)
	}{
		{"UA-only (Connection:close)", apply},
		{"+Referer pan.baidu.com", func(h http.Header) {
			h.Set("User-Agent", "pan.baidu.com")
			h.Set("Referer", "https://pan.baidu.com/")
		}},
		{"+Referer+Accept+Lang", func(h http.Header) {
			h.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) pan.baidu.com")
			h.Set("Referer", "https://pan.baidu.com/")
			h.Set("Accept", "*/*")
			h.Set("Accept-Language", "zh-CN,zh;q=0.9")
		}},
		{"无Range (整头探测)", func(h http.Header) {
			h.Set("User-Agent", "pan.baidu.com")
			h.Set("Referer", "https://pan.baidu.com/")
		}},
	}
	for i, c := range cases {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, dlink, nil)
		c.fn(req.Header)
		req.Header.Set("Connection", "close")
		req.Header.Set("Range", "bytes=0-65535") // 只拉 64KB，便宜
		resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
		if err != nil {
			fmt.Printf("[%2d] %-28s ERROR %v\n", i, c.name, err)
			continue
		}
		n, _ := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		status := "ok"
		if resp.StatusCode >= 400 {
			status = fmt.Sprintf("HTTP%d", resp.StatusCode)
		}
		fmt.Printf("[%2d] %-28s -> %s (got=%d bytes)\n", i, c.name, status, n)
	}

	// 真·curl（复刻 scheduler step-1）：看 dlink 直打的 redirect/status/headers。
	fmt.Println("\n-- curl -v dlink（生产 step-1 同款）--")
	variants := []struct {
		name   string
		headers []string
	}{
		{"裸 (step-1 现状)", nil},
		{"+Browser UA/Referer", []string{
			"User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
			"Referer: https://pan.baidu.com/",
		}},
		{"+Browser UA/Referer +BDUSS", []string{
			"User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
			"Referer: https://pan.baidu.com/",
			"Cookie: BDUSS=ZiU35ReT...; STOKEN=***",
		}},
	}
	for _, v := range variants {
		args := []string{"-sSv", "--max-time", "15", "-o", "/tmp/dl_probe_body.bin", "-D", "-", "-w", "\nHTTP_CODE %{http_code}\n"}
		for _, h := range v.headers {
			args = append(args, "-H", h)
		}
		args = append(args, dlink)
		out, err := exec.Command("curl", args...).CombinedOutput()
		fmt.Printf("\n== %s ==\n", v.name)
		if err != nil {
			fmt.Printf("curl exit: %v\n", err)
		}
		for _, line := range strings.Split(string(out), "\n") {
			l := strings.ToLower(line)
			if strings.HasPrefix(l, "http/") || strings.Contains(l, "location:") ||
				strings.HasPrefix(l, "http_code ") || strings.Contains(l, "<") {
				fmt.Printf("  %s\n", strings.TrimSpace(line))
			}
		}
		if b, err := os.ReadFile("/tmp/dl_probe_body.bin"); err == nil && len(b) > 0 {
			fmt.Printf("  [body %d bytes] %q\n", len(b), string(b))
		}
	}
	return nil
}

func runProbe(ctx context.Context, pc adapter.PCSClient, remotePath string, nConc int, bduss string) error {
	fmt.Printf("\n待测文件: %s\n", remotePath)

	// 取 size + fsID（Stat 走接口，无需 LocateDownload）。
	entry, err := pc.Stat(ctx, remotePath)
	if err != nil {
		return fmt.Errorf("Stat 失败: %w", err)
	}
	fsID, size := entry.FSID, entry.Size

	// method=download PCS URL：生产读路径优先用它（LocateDownload 的 locatedownload 签名会被 CDN 边缘以 sign error 31362 拒绝，见 reader.go:97-118）。
	// adapter.NewBaiduCloudFS 构造真实读路径对象，注入 scheduler fetcher（生产同款：CDN 下载需 BDUSS Cookie）。
	sched := download.NewScheduler(nConc+16, 2*time.Minute, 5*time.Minute)
	defer sched.Shutdown()
	if bduss != "" {
		sched.SetExtraHeaders(map[string]string{"Cookie": "BDUSS=" + bduss}) // filesystem.go:142
	}
	bfs := adapter.NewBaiduCloudFS(pc, adapter.NewDlinkCache(30*time.Minute), "")
	bfs.SetFetcher(sched.FetchRange)

	// OpenRead：内部 resolveDlink 自动走 method=download（优先，reader.go:97-118），失败 fallback locatedownload。
	reader, err := bfs.OpenRead(ctx, remotePath)
	if err != nil {
		return fmt.Errorf("OpenRead 失败(method=download/locatedownload 均不可用): %w", err)
	}
	defer reader.Close()
	fmt.Printf("fsID=%d size=%d\n", fsID, size)

	chunks := make([][2]int64, 0, nConc)
	step := (size + int64(nConc) - 1) / int64(nConc)
	for i := 0; i < nConc; i++ {
		off := int64(i) * step
		if off >= size {
			break
		}
		length := size - off
		if length > step {
			length = step
		}
		chunks = append(chunks, [2]int64{off, length})
	}

	// 单连接基线：一次 Range GET 拉完整文件（一个 Range 请求即一条连接）。
	start := time.Now()
	baseData, baseErr := reader.ReadAt(ctx, 0, int(size))
	baseElapsed := time.Since(start)
	var n int64 = int64(len(baseData))
	baseMBPS := float64(n) / baseElapsed.Seconds() / (1024 * 1024)
	fmt.Printf("\n[单连接] %.1f MB in %.2fs -> %.1f MB/s (got=%d want=%d err=%v)\n",
		float64(n)/(1024*1024), baseElapsed.Seconds(), baseMBPS, n, size, baseErr)

	// 并发：单文件多段并行，需显式放开 per-file cap（默认=1 串行）。
	sched.SetPerFileCap(len(chunks))
	fmt.Printf("\n[并发 %d] 文件切成 %d 段同时拉取...\n", len(chunks), len(chunks))
	type res struct {
		idx int
		got int64
		err error
	}
	results := make([]res, len(chunks))
	start = time.Now()
	var wg sync.WaitGroup
	for i, c := range chunks {
		wg.Add(1)
		go func(idx int, cc [2]int64) {
			defer wg.Done()
			data, err := reader.ReadAt(ctx, cc[0], int(cc[1]))
			results[idx] = res{idx: idx, got: int64(len(data)), err: err}
		}(i, c)
	}
	wg.Wait()
	total := int64(0)
	for _, r := range results {
		total += r.got
		fmt.Printf("  chunk[%d] off=%d got=%d want=%d err=%v\n",
			r.idx, chunks[r.idx][0], r.got, chunks[r.idx][1], r.err)
	}
	pElapsed := time.Since(start)
	parMBPS := float64(total) / pElapsed.Seconds() / (1024 * 1024)
	fmt.Printf("\n[并发 %d] %.1f MB in %.2fs -> %.1f MB/s\n", len(chunks), float64(total)/(1024*1024), pElapsed.Seconds(), parMBPS)

	singleOK := baseErr == nil && n == size
	allChunkOK := total == size
	for _, r := range results {
		if r.err != nil {
			allChunkOK = false
		}
	}
	fmt.Printf("\n=== 结论 ===\n")
	fmt.Printf("单连接完整性: %v (got=%d/%d)\n", singleOK, n, size)
	fmt.Printf("并发完整性:   %v (总字节=%d/%d)\n", allChunkOK, total, size)
	if baseMBPS > 0 {
		ratio := parMBPS / baseMBPS
		fmt.Printf("加速比(并发/单连接): %.2fx\n", ratio)
		switch {
		case ratio >= float64(len(chunks))*0.75 && allChunkOK:
			fmt.Println("=> 服务端支持单文件并行：聚合吞吐随并发上升。")
		case ratio < 1.3:
			fmt.Println("=> 服务端限制单文件并行：并发未带来增益（按连接/账号限速）。")
		default:
			fmt.Println("=> 部分并行收益：有提升但未达线性，存在一定限流。")
		}
	}
	return nil
}

func main() {
	// 默认凭据文件：$BDFS_ENV 或 $HOME/.bdfs.env；亦可用第一个位置参数覆盖
	credsPath := os.Getenv("BDFS_ENV")
	if credsPath == "" {
		if home, err := os.UserHomeDir(); err == nil {
			credsPath = filepath.Join(home, ".bdfs.env")
		}
	}
	if len(os.Args) > 1 && os.Args[1] != "upload" && os.Args[1] != "probe" && os.Args[1] != "diag" {
		credsPath = os.Args[1]
		os.Args = os.Args[1:] // credsPath 占位已被消费
	}
	nConc := 4
	if v := os.Getenv("PARALLEL_N"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			nConc = n
		}
	}

	bduss, stoken, err := parseCreds(credsPath)
	if err != nil {
		log.Fatalf("解析凭据失败: %v", err)
	}
	log.Printf("已加载凭据: BDUSS=%s..., STOKEN=***...", bduss[:min(8, len(bduss))])

	ctx := context.Background()
	pc := newClient(bduss, stoken)

	if len(os.Args) >= 3 && os.Args[1] == "upload" {
		if err := uploadFile(ctx, pc, os.Args[2], os.Args[3]); err != nil {
			log.Fatalf("上传失败: %v", err)
		}
		return
	}

	if len(os.Args) >= 3 && os.Args[1] == "diag" {
		dlink, _, _, _, err := pc.LocateDownload(ctx, os.Args[2])
		if err != nil {
			log.Fatalf("LocateDownload 失败: %v", err)
		}
		if err := diag(ctx, dlink); err != nil {
			log.Fatalf("诊断失败: %v", err)
		}
		return
	}

	remotePath := "/bdfuse-test/clip_vision_h.safetensors"
	if len(os.Args) >= 3 && os.Args[1] == "probe" {
		remotePath = os.Args[2]
	}
	if err := runProbe(ctx, pc, remotePath, nConc, bduss); err != nil {
		log.Fatalf("探测失败: %v", err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
