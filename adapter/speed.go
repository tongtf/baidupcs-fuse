package adapter

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// SpeedTracker 传输速度跟踪器
// ============================================================

// SpeedTracker 追踪传输速度（滑动窗口）。
type SpeedTracker struct {
	mu        sync.Mutex
	windows   []speedSample // 滑动窗口（最近 N 个采样点）
	windowSec int           // 窗口大小（秒）
}

type speedSample struct {
	at    time.Time
	bytes int64
}

// NewSpeedTracker 创建速度跟踪器。windowSec 为滑动窗口大小（秒）。
func NewSpeedTracker(windowSec int) *SpeedTracker {
	if windowSec <= 0 {
		windowSec = 5
	}
	return &SpeedTracker{
		windows:   make([]speedSample, 0, windowSec*2),
		windowSec: windowSec,
	}
}

// AddBytes 记录传输了 n 字节。
func (t *SpeedTracker) AddBytes(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.windows = append(t.windows, speedSample{at: now, bytes: n})
	// 淘汰窗口外的样本
	cutoff := now.Add(-time.Duration(t.windowSec) * time.Second)
	i := 0
	for i < len(t.windows) && t.windows[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		t.windows = t.windows[i:]
	}
}

// Speed 返回当前速度（bytes/sec）。
func (t *SpeedTracker) Speed() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.windows) < 2 {
		return 0
	}
	now := time.Now()
	cutoff := now.Add(-time.Duration(t.windowSec) * time.Second)
	var totalBytes int64
	var oldest time.Time
	for _, s := range t.windows {
		if s.at.Before(cutoff) {
			continue
		}
		if oldest.IsZero() {
			oldest = s.at
		}
		totalBytes += s.bytes
	}
	if oldest.IsZero() || totalBytes == 0 {
		return 0
	}
	elapsed := now.Sub(oldest).Seconds()
	if elapsed < 0.1 {
		elapsed = 0.1
	}
	return float64(totalBytes) / elapsed
}

// FormatSpeed 格式化速度为可读字符串。
func FormatSpeed(bps float64) string {
	switch {
	case bps >= 1<<30:
		return fmt.Sprintf("%.1f GB/s", bps/(1<<30))
	case bps >= 1<<20:
		return fmt.Sprintf("%.1f MB/s", bps/(1<<20))
	case bps >= 1<<10:
		return fmt.Sprintf("%.1f KB/s", bps/(1<<10))
	default:
		return fmt.Sprintf("%.0f B/s", bps)
	}
}

// ============================================================
// TransferStats 全局传输统计
// ============================================================

// TransferStats 全局上传/下载统计，供 bdfs status 和 stderr 输出使用。
type TransferStats struct {
	UploadBytes   atomic.Int64
	DownloadBytes atomic.Int64
	UploadSpeed   *SpeedTracker
	DownloadSpeed *SpeedTracker

	// 活跃传输追踪
	mu       sync.Mutex
	uploads  map[string]*TransferTask // path → task
	downloads map[string]*TransferTask
}

// TransferTask 单个文件的传输任务。
type TransferTask struct {
	Path      string
	TotalSize int64
	Done      int64 // 已传输字节
	IsUpload  bool
	StartedAt time.Time
}

// NewTransferStats 创建全局传输统计。
func NewTransferStats() *TransferStats {
	return &TransferStats{
		UploadSpeed:   NewSpeedTracker(5),
		DownloadSpeed: NewSpeedTracker(5),
		uploads:       make(map[string]*TransferTask),
		downloads:     make(map[string]*TransferTask),
	}
}

// TrackUpload 记录上传字节。
func (s *TransferStats) TrackUpload(path string, bytes int64) {
	s.UploadBytes.Add(bytes)
	s.UploadSpeed.AddBytes(bytes)
	s.mu.Lock()
	if t, ok := s.uploads[path]; ok {
		t.Done += bytes
	}
	s.mu.Unlock()
}

// TrackDownload 记录下载字节。
func (s *TransferStats) TrackDownload(path string, bytes int64) {
	s.DownloadBytes.Add(bytes)
	s.DownloadSpeed.AddBytes(bytes)
	s.mu.Lock()
	if t, ok := s.downloads[path]; ok {
		t.Done += bytes
	}
	s.mu.Unlock()
}

// StartUpload 开始追踪一个上传任务。
func (s *TransferStats) StartUpload(path string, totalSize int64) {
	s.mu.Lock()
	s.uploads[path] = &TransferTask{
		Path:      path,
		TotalSize: totalSize,
		IsUpload:  true,
		StartedAt: time.Now(),
	}
	s.mu.Unlock()
}

// EndUpload 结束追踪一个上传任务。
func (s *TransferStats) EndUpload(path string) {
	s.mu.Lock()
	delete(s.uploads, path)
	s.mu.Unlock()
}

// StartDownload 开始追踪一个下载任务。
func (s *TransferStats) StartDownload(path string, totalSize int64) {
	s.mu.Lock()
	s.downloads[path] = &TransferTask{
		Path:      path,
		TotalSize: totalSize,
		IsUpload:  false,
		StartedAt: time.Now(),
	}
	s.mu.Unlock()
}

// EndDownload 结束追踪一个下载任务。
func (s *TransferStats) EndDownload(path string) {
	s.mu.Lock()
	delete(s.downloads, path)
	s.mu.Unlock()
}

// ActiveTasks 返回当前活跃传输任务列表。
func (s *TransferStats) ActiveTasks() []TransferTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tasks []TransferTask
	for _, t := range s.uploads {
		tasks = append(tasks, *t)
	}
	for _, t := range s.downloads {
		tasks = append(tasks, *t)
	}
	return tasks
}

// ============================================================
// ProgressLogger 向 stderr 周期输出传输进度
// ============================================================

// ProgressLogger 周期性向 stderr 输出传输进度。
type ProgressLogger struct {
	stats    *TransferStats
	interval time.Duration
	stopCh   chan struct{}
}

// NewProgressLogger 创建进度日志器。
func NewProgressLogger(stats *TransferStats, interval time.Duration) *ProgressLogger {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	return &ProgressLogger{
		stats:    stats,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start 启动周期输出。
func (l *ProgressLogger) Start() {
	go l.loop()
}

// Stop 停止周期输出。
func (l *ProgressLogger) Stop() {
	close(l.stopCh)
}

func (l *ProgressLogger) loop() {
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.print()
		case <-l.stopCh:
			return
		}
	}
}

func (l *ProgressLogger) print() {
	tasks := l.stats.ActiveTasks()
	if len(tasks) == 0 {
		return
	}

	for _, t := range tasks {
		pct := 0.0
		if t.TotalSize > 0 {
			pct = float64(t.Done) / float64(t.TotalSize) * 100
		}
		elapsed := time.Since(t.StartedAt).Seconds()
		var speed float64
		if elapsed > 0 {
			speed = float64(t.Done) / elapsed
		}

		direction := "↓"
		if t.IsUpload {
			direction = "↑"
		}

		log.Printf("[传输] %s %s %s/%s (%.1f%%) %s",
			direction, t.Path,
			FormatSize(t.Done), FormatSize(t.TotalSize),
			pct, FormatSpeed(speed))
	}
}

// FormatSize 格式化文件大小。
func FormatSize(bytes int64) string {
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
