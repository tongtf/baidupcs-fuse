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
		maxParallel: 2,
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
		if sliceSize > 64*1024*1024 {
			sliceSize = 64 * 1024 * 1024 // 上限，避免大文件把单片 buffer 撑到 GB 级
		}
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
	if uploadID == "" {
		return &UploadResult{Path: remotePath, Size: 0}, nil
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
		if int64(n) != readSize {
			return nil, fmt.Errorf("staging short read at %d: %d != %d", off, n, readSize)
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
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	sliceLen := int64(256 * 1024)
	if size < sliceLen {
		sliceLen = size
	}
	sliceBuf := make([]byte, sliceLen)
	n, err := file.Read(sliceBuf)
	if err != nil && err != io.EOF {
		return false, err
	}
	if int64(n) != sliceLen {
		return false, fmt.Errorf("staging short read for slice-md5: %d != %d", n, sliceLen)
	}
	sliceMD5 := md5.Sum(sliceBuf[:n])

	// 全量 contentMD5：小文件（≤25MB）算，大文件跳过（百度 API 支持空值）。
	// 必须只哈希逻辑 size，staging 尾部可能残留 truncate 前的旧数据。
	var contentMD5 string
	const rapidThreshold = 25 * 1024 * 1024
	if size <= rapidThreshold {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return false, err
		}
		fullMD5 := md5.New()
		if _, err := io.CopyN(fullMD5, file, size); err != nil {
			return false, err
		}
		contentMD5 = hex.EncodeToString(fullMD5.Sum(nil))
	}

	// 写路径必须覆盖同名文件；skip 在目标已存在时可能直接返回成功而保留旧内容。
	if err := u.client.RapidUpload(ctx, remotePath, size, contentMD5, hex.EncodeToString(sliceMD5[:]), "", "overwrite", "", blocks); err != nil {
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
	if uploadID == "" {
		// Precreate 返回 return_type=2：服务端已秒传成功，无需再传分片。
		return &UploadResult{Path: remotePath, Size: size}, nil
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

	var lastProgressAt atomic.Int64
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
				if int64(n) != readSize {
					firstErr.Store(fmt.Errorf("read slice %d: short read %d != %d", task.seq, n, readSize))
					return
				}

				// 重试逻辑
				var lastErr error
				for retry := 0; retry <= u.maxRetries; retry++ {
					if retry > 0 {
						if os.Getenv("BDFS_DEBUG") != "" {
							log.Printf("UploadSlice retry: uploadID=%s seq=%d attempt=%d", uploadID, task.seq, retry)
						}
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
				nowMs := time.Now().UnixMilli()
				prev := lastProgressAt.Load()
				if int(newCount) == totalSlices || nowMs-prev >= 500 {
					if lastProgressAt.CompareAndSwap(prev, nowMs) {
						_ = UpdateProgress(cacheDir, remotePath, int(newCount), uploadedBytes, speed)
					}
				}
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

// failedMu 串行化失败清单的读-改-写，避免并发上传互相覆盖。
var failedMu sync.Mutex

// SaveFailedUpload 保存失败上传到 manifest.json（JSON 格式，按 remote path 去重）。
func SaveFailedUpload(cacheDir string, entry FailedUploadEntry) error {
	failedMu.Lock()
	defer failedMu.Unlock()

	dir := filepath.Join(cacheDir, "failed-uploads")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	_ = os.Chmod(dir, 0700)
	manifestPath := filepath.Join(dir, "manifest.json")

	manifest := FailedUploadManifest{}
	if data, err := os.ReadFile(manifestPath); err == nil {
		entries, perr := parseFailedManifest(data)
		if perr != nil {
			return fmt.Errorf("failed upload manifest is corrupt; refusing to overwrite: %w", perr)
		}
		manifest.Entries = entries
	} else if !os.IsNotExist(err) {
		return err // 读不到旧清单时绝不能覆盖它，否则会丢失败记录
	}
	replaced := false
	oldLocal := ""
	for i := range manifest.Entries {
		if manifest.Entries[i].RemotePath == entry.RemotePath {
			oldLocal = manifest.Entries[i].LocalPath
			manifest.Entries[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		manifest.Entries = append(manifest.Entries, entry)
	}
	if err := writeFailedManifestLocked(manifestPath, manifest); err != nil {
		return err
	}
	// 新记录已落盘后才能删旧 staging；否则写清单失败会让旧记录指向不存在的文件。
	if replaced && oldLocal != "" && oldLocal != entry.LocalPath {
		_ = os.Remove(oldLocal)
	}
	return nil
}

// LoadFailedUploads 读取失败上传清单（兼容旧版 pipe 分隔格式）。
func LoadFailedUploads(cacheDir string) ([]FailedUploadEntry, error) {
	failedMu.Lock()
	defer failedMu.Unlock()
	data, err := os.ReadFile(filepath.Join(cacheDir, "failed-uploads", "manifest.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseFailedManifest(data)
}

// RemoveFailedUpload 从失败清单中删除一条；保留其余条目。
func RemoveFailedUpload(cacheDir, remotePath string) error {
	failedMu.Lock()
	defer failedMu.Unlock()
	manifestPath := filepath.Join(cacheDir, "failed-uploads", "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	entries, perr := parseFailedManifest(data)
	if perr != nil {
		return fmt.Errorf("failed upload manifest is corrupt; refusing to rewrite: %w", perr)
	}
	kept := entries[:0]
	for _, e := range entries {
		if e.RemotePath != remotePath {
			kept = append(kept, e)
		}
	}
	if len(kept) == 0 {
		return os.Remove(manifestPath)
	}
	return writeFailedManifestLocked(manifestPath, FailedUploadManifest{Entries: kept})
}

func parseFailedManifest(data []byte) ([]FailedUploadEntry, error) {
	trimmed := bytesTrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if trimmed[0] == '{' || trimmed[0] == '[' {
		var m FailedUploadManifest
		if trimmed[0] == '[' {
			if err := json.Unmarshal(trimmed, &m.Entries); err != nil {
				return nil, err
			}
			return m.Entries, nil
		}
		if err := json.Unmarshal(trimmed, &m); err != nil {
			return nil, err
		}
		return m.Entries, nil
	}
	// 兼容旧版：remote|local|size|error
	var entries []FailedUploadEntry
	for i, line := range strings.Split(string(trimmed), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 4)
		if len(parts) < 2 {
			return nil, fmt.Errorf("legacy failed manifest line %d: expected remote|local", i+1)
		}
		e := FailedUploadEntry{RemotePath: parts[0], LocalPath: parts[1]}
		if len(parts) >= 3 {
			if _, err := fmt.Sscanf(parts[2], "%d", &e.Size); err != nil {
				return nil, fmt.Errorf("legacy failed manifest line %d: invalid size: %w", i+1, err)
			}
		}
		if len(parts) >= 4 {
			e.Error = parts[3]
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func bytesTrimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\r' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 {
		c := b[len(b)-1]
		if c != ' ' && c != '\n' && c != '\r' && c != '\t' {
			break
		}
		b = b[:len(b)-1]
	}
	return b
}

func writeFailedManifestLocked(path string, manifest FailedUploadManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".manifest-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	_ = os.Chmod(path, 0600)
	return nil
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

// progressMu 串行化进度文件的读-改-写（worker 并发 UpdateProgress）。
var progressMu sync.Mutex

// WriteProgress 写入上传进度文件。
func WriteProgress(cacheDir string, p *UploadProgress) error {
	progressMu.Lock()
	defer progressMu.Unlock()
	return writeProgressLocked(cacheDir, p)
}

func writeProgressLocked(cacheDir string, p *UploadProgress) error {
	dir := progressDir(cacheDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	_ = os.Chmod(dir, 0700)
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".progress-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, progressPath(cacheDir, p.Path)); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// UpdateProgress 更新已上传分片数和字节数（进程内原子读-改-写）。
func UpdateProgress(cacheDir, remotePath string, uploadedSlices int, uploadedBytes int64, speedBps float64) error {
	progressMu.Lock()
	defer progressMu.Unlock()
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
	return writeProgressLocked(cacheDir, &p)
}

// RemoveProgress 上传成功后删除 progress 文件。
func RemoveProgress(cacheDir, remotePath string) {
	progressMu.Lock()
	os.Remove(progressPath(cacheDir, remotePath))
	progressMu.Unlock()
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
