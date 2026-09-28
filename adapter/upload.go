package adapter

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// UploadSession 上传会话管理器。
type UploadSession struct {
	client      PCSClient
	stagingMgr  *StagingManager
	sliceSize   int64 // 有效默认 8MB（由 flag/config 经 SetSliceSize 设置），大文件按分片数上限自动放大
	maxRetries  int
	maxParallel int            // 最大并行分片上传数（默认 1=串行）
	stats       *TransferStats // 全局传输统计（可选）
}

// NewUploadSession 创建上传会话。
func NewUploadSession(client PCSClient, stagingMgr *StagingManager) *UploadSession {
	return &UploadSession{
		client:      client,
		stagingMgr:  stagingMgr,
		sliceSize:   8 * 1024 * 1024, // 与 flag/config 默认一致：8MB（VIP 优化）
		maxRetries:  3,
		maxParallel: 1,
	}
}

// SetMaxParallel 设置最大并行分片上传数。
func (u *UploadSession) SetMaxParallel(n int) {
	if n > 0 {
		u.maxParallel = n
	}
}

// SetTransferStats 注入全局传输统计。
func (u *UploadSession) SetTransferStats(stats *TransferStats) {
	u.stats = stats
}

// SetSliceSize 设置上传分片大小。
func (u *UploadSession) SetSliceSize(size int64) {
	if size > 0 {
		u.sliceSize = size
	}
}

// UploadResult 上传结果。
type UploadResult struct {
	Path string
	Size int64
}

// Upload 执行上传：检查空文件 → RapidUpload → 分片上传 → 合并。
func (u *UploadSession) Upload(ctx context.Context, stagingPath, remotePath string, size int64) (*UploadResult, error) {
	// 空文件处理（AUDIT-004）
	if size == 0 {
		return u.uploadEmptyFile(ctx, remotePath)
	}

	// 无论成功/失败，上传终了即清除进度标记：失败上传不应再挂在 status 里显示“活跃/卡死”。
	defer RemoveProgress(u.stagingMgr.CacheDir(), remotePath)

	file, err := os.Open(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("open staging file: %w", err)
	}
	defer file.Close()

	return u.UploadFile(ctx, file, remotePath, size)
}

// UploadFile 执行上传（接受已打开的文件，Sync 路径用，避免 close/reopen）。
func (u *UploadSession) UploadFile(ctx context.Context, file *os.File, remotePath string, size int64) (*UploadResult, error) {
	if size == 0 {
		return u.uploadEmptyFile(ctx, remotePath)
	}

	// 计算分片大小（AUDIT-012: >4GB 自动放大）
	sliceSize := u.sliceSize
	if size > 4*1024*1024*1024 {
		sliceSize = maxInt64(4*1024*1024, int64(math.Ceil(float64(size)/1024)))
	}

	blocks, err := u.computeBlockList(file, size, sliceSize)
	if err != nil {
		return nil, fmt.Errorf("compute block list: %w", err)
	}

	// RapidUpload（秒传）
	rapidOK, err := u.tryRapidUpload(ctx, file, size, remotePath, blocks)
	if err == nil && rapidOK {
		return &UploadResult{Path: remotePath, Size: size}, nil
	}

	// 常规分片上传
	return u.uploadBySlices(ctx, file, remotePath, size, sliceSize, blocks)
}

// uploadEmptyFile 上传空文件（AUDIT-004）。
// 空文件 block_list 必须为 ["d41d8cd9..."]（不能传 []），否则 precreate errno=2。
func (u *UploadSession) uploadEmptyFile(ctx context.Context, remotePath string) (*UploadResult, error) {
	emptyMD5 := "d41d8cd98f00b204e9800998ecf8427e"
	// 用 overwrite (rtype=2) 尝试秒传：文件内容一致则直接成功
	err := u.client.RapidUpload(ctx, remotePath, 0, emptyMD5, "", "", "overwrite", "", []string{emptyMD5})
	if err == nil {
		return &UploadResult{Path: remotePath, Size: 0}, nil
	}
	// ErrNeedUpload（文件不存在）→ 走 precreate + 直接合并（空文件无需分片上传）
	uploadID, err := u.client.PrecreateUpload(ctx, remotePath, 0, []string{emptyMD5})
	if err != nil {
		return nil, fmt.Errorf("precreate empty file: %w", err)
	}
	if err := u.client.CreateSuperFile(ctx, uploadID, remotePath, 0, map[int]string{0: emptyMD5}); err != nil {
		return nil, fmt.Errorf("create super file (empty): %w", err)
	}
	return &UploadResult{Path: remotePath, Size: 0}, nil
}

// computeBlockList 逐片计算 MD5（D7: blockListMD5）。
func (u *UploadSession) computeBlockList(file *os.File, size, sliceSize int64) ([]string, error) {
	blocks := make([]string, 0, int(math.Ceil(float64(size)/float64(sliceSize))))
	buf := make([]byte, sliceSize)
	for off := int64(0); off < size; off += sliceSize {
		readSize := sliceSize
		if off+readSize > size {
			readSize = size - off
		}
		n, err := file.ReadAt(buf[:readSize], off)
		if err != nil && err != io.EOF {
			return nil, err
		}
		h := md5.Sum(buf[:n])
		blocks = append(blocks, hex.EncodeToString(h[:]))
	}
	return blocks, nil
}

// tryRapidUpload 尝试秒传。
// 大文件（>25MB）跳过全量 contentMD5 计算（省 1-3s IO），只传 slice-md5 + block_list。
func (u *UploadSession) tryRapidUpload(ctx context.Context, file *os.File, size int64, remotePath string, blocks []string) (bool, error) {
	if len(blocks) == 0 {
		return false, nil
	}

	// 前 256KB slice-md5（所有文件都算，开销可忽略）
	file.Seek(0, io.SeekStart)
	sliceBuf := make([]byte, 256*1024)
	n, _ := file.Read(sliceBuf)
	sliceMD5 := md5.Sum(sliceBuf[:n])

	// 全量 contentMD5：小文件（≤25MB）算，大文件跳过（百度 API 支持空值）
	var contentMD5 string
	const rapidThreshold = 25 * 1024 * 1024
	if size <= rapidThreshold {
		file.Seek(0, io.SeekStart)
		fullMD5 := md5.New()
		if _, err := io.Copy(fullMD5, file); err != nil {
			return false, err
		}
		contentMD5 = hex.EncodeToString(fullMD5.Sum(nil))
	}

	if err := u.client.RapidUpload(ctx, remotePath, size, contentMD5, hex.EncodeToString(sliceMD5[:]), "", "skip", "", blocks); err != nil {
		return false, err
	}
	return true, nil
}

// uploadBySlices 分片上传（支持并行）。
func (u *UploadSession) uploadBySlices(ctx context.Context, file *os.File, remotePath string, size, sliceSize int64, blocks []string) (*UploadResult, error) {
	// Precreate 获取 uploadID
	uploadID, err := u.client.PrecreateUpload(ctx, remotePath, size, blocks)
	if err != nil {
		return nil, fmt.Errorf("precreate: %w", err)
	}

	// 写入上传进度（#10 IPC）
	totalSlices := len(blocks)
	cacheDir := u.stagingMgr.cacheDir
	progress := &UploadProgress{
		Path:           remotePath,
		UploadID:       uploadID,
		TotalSize:      size,
		SliceSize:      sliceSize,
		TotalSlices:    totalSlices,
		StartedAt:      time.Now().Format(time.RFC3339),
		LastActivityAt: time.Now().Format(time.RFC3339),
	}
	_ = WriteProgress(cacheDir, progress)

	// 开始追踪传输速度
	if u.stats != nil {
		u.stats.StartUpload(remotePath, size)
	}

	if size == 0 {
		// 空文件直接合并
		checksumMap := map[int]string{0: blocks[0]}
		if err := u.client.CreateSuperFile(ctx, uploadID, remotePath, 0, checksumMap); err != nil {
			return nil, fmt.Errorf("create super file (empty): %w", err)
		}
		RemoveProgress(cacheDir, remotePath)
		return &UploadResult{Path: remotePath, Size: 0}, nil
	}

	// 并行上传：worker pool + sync.Map 收集结果
	var (
		checksumMap          sync.Map // seq → md5Hash
		uploaded             atomic.Int32
		uploadedBytesCounter atomic.Int64
		firstErr             atomic.Value
	)

	workerCount := u.maxParallel
	if workerCount > totalSlices {
		workerCount = totalSlices
	}
	if workerCount < 1 {
		workerCount = 1
	}

	// 任务通道
	type sliceTask struct {
		seq int
		off int64
		end int64
	}
	tasks := make(chan sliceTask, totalSlices)
	for i := 0; i < totalSlices; i++ {
		off := int64(i) * sliceSize
		end := off + sliceSize
		if end > size {
			end = size
		}
		tasks <- sliceTask{seq: i, off: off, end: end}
	}
	close(tasks)

	// 启动 worker
	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, sliceSize) // 每个 worker 独立 buffer
			for task := range tasks {
				if firstErr.Load() != nil {
					return // 有错误则停止后续任务
				}
				readSize := task.end - task.off
				n, err := file.ReadAt(buf[:readSize], task.off)
				if err != nil && err != io.EOF {
					firstErr.Store(fmt.Errorf("read slice %d: %w", task.seq, err))
					return
				}

				// 重试逻辑
				var lastErr error
				for retry := 0; retry <= u.maxRetries; retry++ {
				if retry > 0 {
					log.Printf("UploadSlice retry: uploadID=%s seq=%d attempt=%d", uploadID, task.seq, retry)
					select {
					case <-time.After(time.Duration(1<<(retry-1)) * time.Second):
					case <-ctx.Done():
						firstErr.Store(fmt.Errorf("upload slice %d backoff cancelled: %w", task.seq, ctx.Err()))
						return
					}
				}
					md5Hash, err := u.client.UploadSlice(ctx, uploadID, remotePath, task.seq, task.off, buf[:n])
					if err == nil {
						checksumMap.Store(task.seq, md5Hash)
						lastErr = nil
						break
					}
					lastErr = err
				}
				if lastErr != nil {
					firstErr.Store(fmt.Errorf("upload slice %d failed: %w", task.seq, lastErr))
					return
				}
				// 更新进度
				newCount := uploaded.Add(1)
				uploadedBytes := uploadedBytesCounter.Add(int64(n))
				var speed float64
				if u.stats != nil {
					u.stats.TrackUpload(remotePath, int64(n))
					speed = u.stats.UploadSpeed.Speed()
				}
				_ = UpdateProgress(cacheDir, remotePath, int(newCount), uploadedBytes, speed)
			}
		}()
	}
	wg.Wait()

	if errVal := firstErr.Load(); errVal != nil {
		if u.stats != nil {
			u.stats.EndUpload(remotePath)
		}
		if err, ok := errVal.(error); ok {
			return nil, err
		}
		return nil, fmt.Errorf("upload failed: %v", errVal)
	}

	// 合并分片：从 sync.Map 转为 map[int]string
	finalMap := make(map[int]string, totalSlices)
	checksumMap.Range(func(k, v interface{}) bool {
		seq, ok1 := k.(int)
		md5Hash, ok2 := v.(string)
		if ok1 && ok2 {
			finalMap[seq] = md5Hash
		}
		return true
	})

	if err := u.client.CreateSuperFile(ctx, uploadID, remotePath, size, finalMap); err != nil {
		if u.stats != nil {
			u.stats.EndUpload(remotePath)
		}
		return nil, fmt.Errorf("create super file: %w", err)
	}

	// 上传成功，删除进度文件
	RemoveProgress(cacheDir, remotePath)
	if u.stats != nil {
		u.stats.EndUpload(remotePath)
	}

	return &UploadResult{Path: remotePath, Size: size}, nil
}

// FailedUploadManifest 失败上传记录。
type FailedUploadManifest struct {
	Entries []FailedUploadEntry `json:"entries"`
}

type FailedUploadEntry struct {
	RemotePath string `json:"remote_path"`
	LocalPath  string `json:"local_path"`
	Size       int64  `json:"size"`
	Error      string `json:"error"`
}

// SaveFailedUpload 保存失败上传到 manifest.json（AUDIT-016）。
func SaveFailedUpload(cacheDir string, entry FailedUploadEntry) error {
	dir := filepath.Join(cacheDir, "failed-uploads")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	tmpPath := manifestPath + ".tmp"

	// 读取已有 manifest（保留历史记录）
	manifest := FailedUploadManifest{}
	if data, err := os.ReadFile(manifestPath); err == nil {
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, "|", 4)
			if len(parts) < 2 {
				continue
			}
			e := FailedUploadEntry{RemotePath: parts[0], LocalPath: parts[1]}
			if len(parts) >= 3 {
				fmt.Sscanf(parts[2], "%d", &e.Size)
			}
			if len(parts) >= 4 {
				e.Error = parts[3]
			}
			manifest.Entries = append(manifest.Entries, e)
		}
	}
	manifest.Entries = append(manifest.Entries, entry)

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, e := range manifest.Entries {
		fmt.Fprintf(f, "%s|%s|%d|%s\n", e.RemotePath, e.LocalPath, e.Size, e.Error)
	}
	return os.Rename(tmpPath, manifestPath)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ============================================================
// Upload Progress IPC (#10)
// ============================================================

// UploadProgress 上传进度信息，写入 progress.json 供 bdfs status 读取。
type UploadProgress struct {
	Path           string  `json:"path"`
	UploadID       string  `json:"upload_id"`
	TotalSize      int64   `json:"total_size"`
	SliceSize      int64   `json:"slice_size"`
	TotalSlices    int     `json:"total_slices"`
	UploadedSlices int     `json:"uploaded_slices"`
	UploadedBytes  int64   `json:"uploaded_bytes"`
	StartedAt      string  `json:"started_at"`
	LastActivityAt string  `json:"last_activity_at"`    // 最后活动（分片成功/进度刷新）时间，供卡死检测
	SpeedBps       float64 `json:"speed_bps,omitempty"` // 实时速度（bytes/sec）
}

// progressDir 返回 progress 文件目录。
func progressDir(cacheDir string) string {
	return filepath.Join(cacheDir, "uploads", "progress")
}

// progressPath 返回指定路径的 progress 文件路径。
func progressPath(cacheDir, remotePath string) string {
	h := sha256.Sum256([]byte(remotePath))
	return filepath.Join(progressDir(cacheDir), fmt.Sprintf("%x.json", h))
}

// WriteProgress 写入上传进度文件。
func WriteProgress(cacheDir string, p *UploadProgress) error {
	dir := progressDir(cacheDir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	tmpPath := progressPath(cacheDir, p.Path) + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, progressPath(cacheDir, p.Path))
}

// UpdateProgress 更新已上传分片数和字节数。
func UpdateProgress(cacheDir, remotePath string, uploadedSlices int, uploadedBytes int64, speedBps float64) error {
	path := progressPath(cacheDir, remotePath)
	data, err := os.ReadFile(path)
	if err != nil {
		return err // progress 文件不存在，忽略
	}
	var p UploadProgress
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	p.UploadedSlices = uploadedSlices
	p.UploadedBytes = uploadedBytes
	p.LastActivityAt = time.Now().Format(time.RFC3339)
	p.SpeedBps = speedBps
	return WriteProgress(cacheDir, &p)
}

// RemoveProgress 上传成功后删除 progress 文件。
func RemoveProgress(cacheDir, remotePath string) {
	os.Remove(progressPath(cacheDir, remotePath))
}

// ListProgress 读取所有活跃上传进度（供 bdfs status 使用）。
func ListProgress(cacheDir string) ([]UploadProgress, error) {
	dir := progressDir(cacheDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var results []UploadProgress
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var p UploadProgress
		if err := json.Unmarshal(data, &p); err != nil {
			continue
		}
		results = append(results, p)
	}
	return results, nil
}

// stuckThreshold 是 status 判定"疑似卡死上传"的静默时长阈值（与下载 StuckFetches 对齐）。
const stuckThreshold = 2 * time.Minute

// ListStuckUploads 返回 progress.json 中最后活动超过阈值的活跃上传——即 status 需告警的疑似卡死对象。
func ListStuckUploads(cacheDir string) ([]UploadProgress, error) {
	progressList, err := ListProgress(cacheDir)
	if err != nil {
		return nil, err
	}
	var stuck []UploadProgress
	for _, p := range progressList {
		last, err := time.Parse(time.RFC3339, p.LastActivityAt)
		if err != nil {
			last, _ = time.Parse(time.RFC3339, p.StartedAt) // 旧进度无 LastActivityAt，回退到 StartedAt
		}
		if !last.IsZero() && time.Since(last) > stuckThreshold {
			stuck = append(stuck, p)
		}
	}
	return stuck, nil
}
