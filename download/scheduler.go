package download

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMaxFileSems = 1000 // per-file 信号量默认上限
)

// Scheduler 下载调度器。per-file sem + 全局 QPS + Range GET。
type Scheduler struct {
	globalSem    chan struct{}
	fileSems     sync.Map // fsID → chan struct{}（容量=1，不 Delete, PLAN-010）
	client       *http.Client
	extraHeaders map[string]string // 每次请求附加的 header（如 Cookie）
	curlTimeout  time.Duration     // curl 单次下载超时
	shutdownCtx  context.Context   // 全局 shutdown context，Shutdown() 取消后所有 curl 立即退出
	shutdownFn   context.CancelFunc
	// 活跃 curl 进程追踪：Shutdown() 时直接 kill，不等 ctx 传播
	activeMu   sync.Mutex
	activeCmds map[*exec.Cmd]struct{}
	// 活跃 fetch 记录（路径/范围/起始时间）：用于 status 监控与卡死检测
	fetchMu     sync.Mutex
	fetchStats  map[string]*fetchStat // requestID → 元信息
	maxFileSems int
	// fileSems 清理互斥：CleanupFileSems 可能被 getFileSem 与后台定时任务并发调用，需串行化。
	cleanupMu  sync.Mutex
	perFileCap int          // 单文件最大并发 Range 数（默认1=串行；>1=并行，由 SetPerFileCap 开启）
	semCount   atomic.Int64 // 当前 fileSems 中活跃条目计数
}

// fetchStat 记录单次 Range GET 的运行时信息，供状态监控与卡死检测使用。
type fetchStat struct {
	Path    string
	FsID    int64
	Off     int64
	Length  int64
	Started time.Time
}

// NewScheduler 创建调度器。maxParallel 控制全局最大并发。
func NewScheduler(maxParallel int, httpTimeout, curlTimeout time.Duration) *Scheduler {
	if httpTimeout <= 0 {
		httpTimeout = 120 * time.Second
	}
	if curlTimeout <= 0 {
		curlTimeout = 60 * time.Second
	}
	sCtx, sFn := context.WithCancel(context.Background())
	return &Scheduler{
		globalSem:   make(chan struct{}, maxParallel),
		client:      &http.Client{Timeout: httpTimeout},
		curlTimeout: curlTimeout,
		perFileCap:  1, // 默认串行，需 SetPerFileCap(>1) 显式开启单文件并行
		shutdownCtx: sCtx,
		shutdownFn:  sFn,
		activeCmds:  make(map[*exec.Cmd]struct{}),
		fetchStats:  make(map[string]*fetchStat),
		maxFileSems: defaultMaxFileSems,
	}
}

// RegisterFetch 记录一次 Range GET 的起始时间，供 status 监控。
func (s *Scheduler) RegisterFetch(requestID string, fsID, off, length int64) {
	s.fetchMu.Lock()
	defer s.fetchMu.Unlock()
	if _, exists := s.fetchStats[requestID]; !exists {
		s.fetchStats[requestID] = &fetchStat{FsID: fsID, Off: off, Length: length, Started: time.Now()}
	}
}

// UnregisterFetch 清除一次 Range GET 的记录。
func (s *Scheduler) UnregisterFetch(requestID string) {
	s.fetchMu.Lock()
	delete(s.fetchStats, requestID)
	s.fetchMu.Unlock()
}

// StuckFetches 返回运行时间超过 threshold 的活跃 fetch（疑似卡死）。
func (s *Scheduler) StuckFetches(threshold time.Duration) []fetchStat {
	s.fetchMu.Lock()
	defer s.fetchMu.Unlock()
	now := time.Now()
	var stuck []fetchStat
	for id, st := range s.fetchStats {
		if now.Sub(st.Started) > threshold {
			stuck = append(stuck, *st)
			_ = id // requestID 仅用于 map key，状态输出用路径/范围
		}
	}
	return stuck
}

// ActiveFetchCount 返回当前活跃的 Range GET 数量。
func (s *Scheduler) ActiveFetchCount() int {
	s.fetchMu.Lock()
	defer s.fetchMu.Unlock()
	return len(s.fetchStats)
}

// Shutdown 取消所有进行中的 curl 下载（Ctrl+C / 卸载时调用）。
// 1) 取消 shutdownCtx（阻止新 curl 启动）
// 2) 强杀所有活跃 curl 进程（不等 ctx 传播）
func (s *Scheduler) Shutdown() {
	s.shutdownFn() // 取消 shutdownCtx
	s.activeMu.Lock()
	cmds := make([]*exec.Cmd, 0, len(s.activeCmds))
	for cmd := range s.activeCmds {
		cmds = append(cmds, cmd)
	}
	s.activeMu.Unlock()
	for _, cmd := range cmds {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}
}

// IsShutdown 检查是否已关闭（FUSE Read 入口用，不依赖 ctx 传播）。
func (s *Scheduler) IsShutdown() bool {
	select {
	case <-s.shutdownCtx.Done():
		return true
	default:
		return false
	}
}

// SetExtraHeaders 设置每次请求附加的 header（如 Cookie）。
func (s *Scheduler) SetExtraHeaders(headers map[string]string) {
	s.extraHeaders = headers
}

func (s *Scheduler) getFileSem(fsID int64) chan struct{} {
	// 检查容量，超限则清理
	if s.semCount.Load() > int64(s.maxFileSems) {
		log.Printf("fileSems capacity exceeded (%d > %d), cleaning up", s.semCount.Load(), s.maxFileSems)
		s.CleanupFileSems()
	}
	cap := s.perFileCap
	if cap < 1 {
		cap = 1
	}
	val, loaded := s.fileSems.LoadOrStore(fsID, make(chan struct{}, cap))
	if !loaded {
		s.semCount.Add(1)
	}
	return val.(chan struct{})
}

// SetPerFileCap 设置单文件最大并发 Range 数（默认 1=串行）。
// >1 时同文件的多个 Range GET 可在信号量内并行，但仍受 globalSem（全局 max-parallel）约束。
func (s *Scheduler) SetPerFileCap(n int) {
	if n >= 1 {
		s.perFileCap = n
	}
}

// SetMaxFileSems 设置 per-file 信号量上限。
func (s *Scheduler) SetMaxFileSems(max int) {
	if max > 0 {
		s.maxFileSems = max
	}
}

// CleanupFileSems 清理无活跃使用者的 per-file 信号量，防止 sync.Map 无限增长。
func (s *Scheduler) CleanupFileSems() {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	s.fileSems.Range(func(key, value interface{}) bool {
		sem := value.(chan struct{})
		if len(sem) == 0 {
			// CompareAndDelete 仅在我们真正删除成功时返回 true，并发清理不会重复对同一条目 -1。
			if s.fileSems.CompareAndDelete(key, value) {
				s.semCount.Add(-1)
			}
		}
		return true
	})
}

// generateRequestID 生成请求幂等键，用于重试时标识相同请求。
// 防止 CDN 缓存污染：相同请求重试时携带相同 ID。
func generateRequestID(fsID int64, dlink string, off, length int64) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%d:%d", fsID, dlink, off, length)))
	return hex.EncodeToString(h[:8])
}

// FetchRange 执行单次 Range GET。fsID 用于 per-file 限流（D2）。
// 重试时携带相同 requestID，防止 CDN 缓存污染。
func (s *Scheduler) FetchRange(ctx context.Context, fsID int64, dlink string, off, length int64) ([]byte, error) {
	fileSem := s.getFileSem(fsID)
	requestID := generateRequestID(fsID, dlink, off, length)
	s.RegisterFetch(requestID, fsID, off, length)
	defer s.UnregisterFetch(requestID) // 函数退出时清理（成功/失败/重试耗尽/ctx取消）

	var lastErr error
	for attempt := 0; attempt <= 3; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(300*attempt) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		// 获取信号量
		select {
		case s.globalSem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		// per-file 信号量获取也需 ctx 保护：若同 fsID 的读 goroutine 卡死占用 sem，
		// 无保护的裸 send 会让后续请求无限阻塞且无法取消（hang bug 的放大器）。
		select {
		case fileSem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		data, err := s.doFetch(ctx, dlink, off, length, requestID)

		// 释放信号量
		<-fileSem
		<-s.globalSem

		if err != nil {
			lastErr = err
			// 403/416 不重试（由上层处理 dlink 刷新）
			if err == ErrCDNForbidden || err == ErrRangeNotSatisfiable {
				return nil, err
			}
			continue
		}
		return data, nil
	}
	return nil, lastErr
}

func (s *Scheduler) doFetch(ctx context.Context, dlink string, off, length int64, requestID string) ([]byte, error) {
	end := off + length - 1

	// 所有下载统一用 curl（CDN URL 是一次性的；Go HTTP client 的 TLS 指纹会被 CDN 拒绝）
	return s.doFetchCurl(ctx, dlink, off, end, length, requestID)
}

// doFetchCurl 用 curl 下载数据。
// 两步：1）获取 CDN URL（从 PCS redirect） 2）用 curl 从 CDN 下载（不带 PCS headers）。
// 使用 mergedCtx 确保 Shutdown() 或 FUSE 操作取消时 curl 立即被杀。
// 活跃 curl 进程注册到 activeCmds，Shutdown() 时强杀。
// requestID 用于重试时标识相同请求，防止 CDN 缓存污染。
func (s *Scheduler) doFetchCurl(ctx context.Context, dlink string, off, end, length int64, requestID string) ([]byte, error) {
	// 合并操作 ctx 和 shutdown ctx：任一取消则 curl 立即被杀
	mergedCtx, mergedCancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.shutdownCtx.Done():
			mergedCancel()
		case <-mergedCtx.Done():
		}
	}()

	log.Printf("doFetchCurl: requestID=%s dlink=%s range=%d-%d length=%d",
		requestID, dlink[:minInt(len(dlink), 50)], off, end, length)

	// 第一步：从 PCS 获取 CDN URL（带 BDUSS cookie 认证）。
	// -D - 输出响应头（供解析 Location），-w 输出最终 HTTP 状态码（供语义化错误映射）。
	getArgs := []string{"-sS", "--max-time", "10", "-o", "/dev/null", "-D", "-", "-w", "\nHTTP_CODE %{http_code}"}
	for k, v := range s.extraHeaders {
		getArgs = append(getArgs, "-H", k+": "+v)
	}
	getArgs = append(getArgs, dlink)

	cmd := exec.CommandContext(mergedCtx, "curl", getArgs...)
	s.trackCmd(cmd)
	out, err := cmd.Output()
	s.untrackCmd(cmd)
	if err != nil {
		return nil, fmt.Errorf("curl get CDN URL failed: %w", err)
	}

	// 解析响应头中的 Location（CDN redirect）与 HTTP 状态码（语义化错误映射）。
	// 状态码优先取 -w 标记，兜底取响应头首行 "HTTP/1.1 4xx"。
	cdnURL := ""
	statusCode := ""
	for _, line := range strings.Split(string(out), "\n") {
		if statusCode == "" && strings.HasPrefix(line, "HTTP/") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				statusCode = fields[1]
			}
		}
		if statusCode == "" && strings.HasPrefix(strings.ToLower(line), "http_code ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				statusCode = fields[1]
			}
			continue
		}
		if cdnURL == "" && strings.HasPrefix(strings.ToLower(line), "location:") {
			cdnURL = strings.TrimSpace(line[len("location:"):])
		}
	}

	// PCS 返回的 CDN 级错误直接语义化，交由上层刷新 dlink（403/416 不重试）。
	switch statusCode {
	case "403":
		return nil, ErrCDNForbidden
	case "416":
		return nil, ErrRangeNotSatisfiable
	}

	if cdnURL == "" {
		return nil, fmt.Errorf("no Location header in PCS response (http %s)", statusCode)
	}

	// 第二步：从 CDN 下载数据。CDN 防盗链要求 User-Agent=pan.baidu.com（缺则 403/31326）且必须 Connection: close
	// （keep-alive 连接会被 CDN 拒绝），--http1.1 确保 HTTPS 上 Connection: close 生效。
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)
	curlTimeoutSec := int(s.curlTimeout.Seconds())
	if curlTimeoutSec < 10 {
		curlTimeoutSec = 10
	}
	cmd2 := exec.CommandContext(mergedCtx, "curl", "-sS", "--max-time", fmt.Sprintf("%d", curlTimeoutSec),
		"--http1.1",
		"-H", "Connection: close",
		"-H", "User-Agent: pan.baidu.com",
		"-H", "Range: "+rangeHeader,
		"-o", "-",
		cdnURL,
	)
	s.trackCmd(cmd2)
	var stdout bytes.Buffer
	cmd2.Stdout = &stdout
	err = cmd2.Start()
	if err == nil {
		// 竞态等待 curl 退出：即使 curl --max-time 未生效（卡在网络层或进程无响应），
		// 也保证 Wait() 不永久阻塞。客户端死亡时 FUSE ctx 不取消，必须显式兜底。
		waitDone := make(chan error, 1)
		go func() { waitDone <- cmd2.Wait() }()

		waitTimeout := s.curlTimeout + 10*time.Second
		select {
		case err = <-waitDone:
			// curl 正常结束（含被 mergedCtx 取消）
		case <-time.After(waitTimeout):
			log.Printf("doFetchCurl: download timed out after %v, killing range (off=%d len=%d)", waitTimeout, off, length)
			mergedCancel()
			_ = cmd2.Process.Kill()
			err = <-waitDone // 等待进程真正退出，避免僵尸
		}
	}
	s.untrackCmd(cmd2)
	if err != nil {
		return nil, fmt.Errorf("curl CDN download failed: %w", err)
	}
	data := stdout.Bytes()
	if int64(len(data)) != length {
		return nil, ErrShortRead
	}
	return data, nil
}

// trackCmd 注册活跃 curl 进程（Shutdown 时可强杀）
func (s *Scheduler) trackCmd(cmd *exec.Cmd) {
	s.activeMu.Lock()
	s.activeCmds[cmd] = struct{}{}
	s.activeMu.Unlock()
}

// untrackCmd 注销已完成的 curl 进程
func (s *Scheduler) untrackCmd(cmd *exec.Cmd) {
	s.activeMu.Lock()
	delete(s.activeCmds, cmd)
	s.activeMu.Unlock()
}

// minInt 返回两个整数中较小的一个。
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
