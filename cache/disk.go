package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// DiskCache 持久化块缓存。远程文件不可变，纯追加式，无失效逻辑。
//
// 一致性约定：
//   - 数据文件先原子落盘，再更新 manifest；
//   - Get 校验实际文件长度，坏块/截断块直接删除并当 miss；
//   - 写文件使用 tmp+fsync+rename，避免半写块；
//   - manifest 原子写，启动时对账（缺失/长度不符/孤儿文件）。
type DiskCache struct {
	mu       sync.RWMutex
	putMu    sync.Mutex
	flushMu  sync.Mutex                // 串行化 manifest 刷盘，避免并发 rename 旧快照覆盖新快照
	baseDir  string                    // blocks 存储根目录
	manifest map[string]*DiskBlockMeta // cacheKey → 元数据
	maxSize  int64                     // 最大磁盘使用（字节），0=不限
	curSize  int64                     // 当前磁盘使用
	dirty    bool                      // manifest 是否需要刷盘
}

// DiskBlockMeta 单个缓存块的元数据。
type DiskBlockMeta struct {
	Path       string    `json:"path"`             // 原始文件路径标识（account:fsid:size:mtime）
	File       string    `json:"file"`             // 相对 baseDir 的块文件路径（完整哈希）
	Offset     int64     `json:"offset"`           // 块偏移
	Size       int64     `json:"size"`             // 块实际大小
	SHA256     string    `json:"sha256,omitempty"` // 块内容哈希（防同长度损坏）
	AccessedAt time.Time `json:"accessed_at"`      // 最近访问时间（LRU 用）
}

// DiskManifest 整个 manifest 的序列化格式。
type DiskManifest struct {
	Version int                       `json:"v"`
	Blocks  map[string]*DiskBlockMeta `json:"blocks"`
}

// NewDiskCache 创建磁盘缓存。maxBytes=0 表示不限制。
func NewDiskCache(cacheDir string, maxBytes int64) *DiskCache {
	dc := &DiskCache{
		baseDir:  filepath.Join(cacheDir, "blocks"),
		manifest: make(map[string]*DiskBlockMeta),
		maxSize:  maxBytes,
	}
	if err := os.MkdirAll(dc.baseDir, 0700); err != nil {
		log.Printf("DiskCache: mkdir %s: %v", dc.baseDir, err)
	}
	_ = os.Chmod(dc.baseDir, 0700)
	dc.loadManifest()
	return dc
}

// Get 从磁盘获取缓存块。命中则返回数据副本并更新访问时间。
func (dc *DiskCache) Get(key BlockKey) ([]byte, bool) {
	k := key.String()
	dc.mu.RLock()
	meta, ok := dc.manifest[k]
	if !ok {
		dc.mu.RUnlock()
		return nil, false
	}
	path := dc.blockPath(meta)
	expected := meta.Size
	expectedHash := meta.SHA256
	dc.mu.RUnlock()

	data, err := os.ReadFile(path)
	corrupt := err != nil || int64(len(data)) != expected
	if !corrupt && expectedHash != "" {
		sum := sha256.Sum256(data)
		corrupt = hex.EncodeToString(sum[:]) != expectedHash
	}
	if corrupt {
		// 文件丢失/截断/同长度损坏 → 删除映射，当 miss 处理；绝不把坏块交给上层。
		dc.mu.Lock()
		if m, ok := dc.manifest[k]; ok {
			_ = os.Remove(dc.blockPath(m))
			dc.curSize -= m.Size
			if dc.curSize < 0 {
				dc.curSize = 0
			}
			delete(dc.manifest, k)
			dc.dirty = true
		}
		dc.mu.Unlock()
		if err != nil {
			log.Printf("DiskCache.Get %s: %v (treated as miss)", k, err)
		} else {
			log.Printf("DiskCache.Get %s: checksum mismatch (treated as miss)", k)
		}
		return nil, false
	}

	// 近似 LRU：60s 内重复命中不写 manifest，避免每读一次都刷盘。
	dc.mu.Lock()
	if m, ok := dc.manifest[k]; ok && time.Since(m.AccessedAt) > time.Minute {
		m.AccessedAt = time.Now()
		dc.dirty = true
	}
	dc.mu.Unlock()
	return data, true
}

// Has 判断块是否在磁盘缓存中且长度与 manifest 一致（不读取数据）。
func (dc *DiskCache) Has(key BlockKey) bool {
	k := key.String()
	dc.mu.RLock()
	meta, ok := dc.manifest[k]
	if !ok {
		dc.mu.RUnlock()
		return false
	}
	path := dc.blockPath(meta)
	expected := meta.Size
	dc.mu.RUnlock()

	info, err := os.Stat(path)
	return err == nil && info.Size() == expected
}

// Put 将数据写入磁盘缓存（原子写 + 容量淘汰）。
func (dc *DiskCache) Put(key BlockKey, data []byte) {
	k := key.String()
	dataSize := int64(len(data))
	if dataSize <= 0 {
		return
	}
	if dc.maxSize > 0 && dataSize > dc.maxSize {
		return // 单块超过总容量，缓存它只会把所有其它块挤掉
	}

	dc.putMu.Lock()
	defer dc.putMu.Unlock()

	// 先写临时文件（不持 manifest 锁，避免大块 I/O 阻塞读）。
	bp := dc.blockPathFor(key)
	if err := os.MkdirAll(filepath.Dir(bp), 0700); err != nil {
		log.Printf("DiskCache.Put mkdir: %v", err)
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(bp), ".tmp-*")
	if err != nil {
		log.Printf("DiskCache.Put temp: %v", err)
		return
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		cleanup()
		return
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		log.Printf("DiskCache.Put write: %v", err)
		return
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		log.Printf("DiskCache.Put fsync: %v", err)
		return
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		log.Printf("DiskCache.Put close: %v", err)
		return
	}

	// 更新 manifest：替换旧块（同一 key）并淘汰到容量以下。
	dc.mu.Lock()
	if old, ok := dc.manifest[k]; ok {
		_ = os.Remove(dc.blockPath(old))
		dc.curSize -= old.Size
		delete(dc.manifest, k)
	}
	if dc.maxSize > 0 && dc.curSize+dataSize > dc.maxSize {
		dc.evictLocked(dc.curSize + dataSize - dc.maxSize)
	}
	dc.mu.Unlock()

	if err := os.Rename(tmpName, bp); err != nil {
		cleanup()
		log.Printf("DiskCache.Put rename: %v", err)
		return
	}
	_ = os.Chmod(bp, 0600)

	rel, _ := filepath.Rel(dc.baseDir, bp)
	dc.mu.Lock()
	// 可能被并发 Flush 的 evict 逻辑删除？putMu 保证 Put 串行，但 Get 只删坏块。
	sum := sha256.Sum256(data)
	dc.manifest[k] = &DiskBlockMeta{
		Path:       keyPath(key),
		File:       rel,
		Offset:     key.Offset,
		Size:       dataSize,
		SHA256:     hex.EncodeToString(sum[:]),
		AccessedAt: time.Now(),
	}
	dc.curSize += dataSize
	if dc.curSize < 0 {
		dc.curSize = 0
	}
	dc.dirty = true
	dc.mu.Unlock()
}

// InvalidateFSID 删除某账户下某个 fsID 的所有磁盘块（上传/截断后防旧内容复用）。
func (dc *DiskCache) InvalidateFSID(account string, fsID int64) {
	prefix := fmt.Sprintf("%s:%d:", account, fsID)
	// 与 Put 串行：否则可能在 Put 写完 tmp、尚未登记 manifest 的窗口漏删。
	dc.putMu.Lock()
	defer dc.putMu.Unlock()
	dc.mu.Lock()
	defer dc.mu.Unlock()
	removed := false
	for k, m := range dc.manifest {
		if !strings.HasPrefix(m.Path, prefix) {
			continue
		}
		_ = os.Remove(dc.blockPath(m))
		dc.curSize -= m.Size
		delete(dc.manifest, k)
		removed = true
	}
	if dc.curSize < 0 {
		dc.curSize = 0
	}
	if removed {
		dc.dirty = true
	}
}

// Size 返回当前磁盘缓存使用量。
func (dc *DiskCache) Size() int64 {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	return dc.curSize
}

// Flush 将 manifest 刷盘。
func (dc *DiskCache) Flush() {
	dc.flushMu.Lock()
	defer dc.flushMu.Unlock()
	dc.mu.Lock()
	if !dc.dirty {
		dc.mu.Unlock()
		return
	}
	dc.dirty = false
	dc.mu.Unlock()
	if err := dc.saveManifest(); err != nil {
		dc.mu.Lock()
		dc.dirty = true // 下次继续尝试，不能因为一次失败丢掉 manifest 更新
		dc.mu.Unlock()
	}
}

// Close 刷盘并释放资源。
func (dc *DiskCache) Close() {
	dc.Flush()
}

// ============================================================
// 内部方法
// ============================================================

// blockPath 根据 manifest 中的 meta 计算块文件路径（Get 用）。
func (dc *DiskCache) blockPath(meta *DiskBlockMeta) string {
	if meta.File != "" {
		p := filepath.Join(dc.baseDir, filepath.FromSlash(meta.File))
		// manifest 可能损坏/被篡改：只允许 baseDir 内的相对路径，
		// 否则 cleanup/evict 的 os.Remove 可能删到缓存目录之外。
		rel, err := filepath.Rel(dc.baseDir, p)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return p
		}
	}
	return blockPathFromKeyPath(dc.baseDir, meta.Path, meta.Offset)
}

// blockPathFor 根据 BlockKey 计算块文件路径（Put 用）。
func (dc *DiskCache) blockPathFor(key BlockKey) string {
	return blockPathFromKeyPath(dc.baseDir, keyPath(key), key.Offset)
}

// blockPathFromKeyPath 使用完整 SHA-256（两级目录）避免 32 位哈希碰撞。
func blockPathFromKeyPath(baseDir, keyPath string, offset int64) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", keyPath, offset)))
	hexs := hex.EncodeToString(h[:])
	return filepath.Join(baseDir, hexs[:2], hexs[2:]+".block")
}

// keyPath 生成 cache key 对应的路径标识（不含 offset）。
func keyPath(key BlockKey) string {
	return fmt.Sprintf("%s:%d:%d:%d:%d", key.AccountID, key.FSID, key.Size, key.MtimeSec, key.Epoch)
}

// String 返回 BlockKey 的字符串表示（manifest key，含 offset）。
func (key BlockKey) String() string {
	return fmt.Sprintf("%s:%d:%d:%d:%d:%d", key.AccountID, key.FSID, key.Size, key.MtimeSec, key.Epoch, key.Offset)
}

// evictLocked 淘汰最旧的块，直到释放 targetBytes。
// 调用者必须持有 dc.mu（写锁）。
func (dc *DiskCache) evictLocked(targetBytes int64) {
	if targetBytes <= 0 || len(dc.manifest) == 0 {
		return
	}

	type entry struct {
		key  string
		meta *DiskBlockMeta
	}
	entries := make([]entry, 0, len(dc.manifest))
	for k, m := range dc.manifest {
		entries = append(entries, entry{k, m})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].meta.AccessedAt.Before(entries[j].meta.AccessedAt)
	})

	freed := int64(0)
	for _, e := range entries {
		if freed >= targetBytes {
			break
		}
		_ = os.Remove(dc.blockPath(e.meta))
		freed += e.meta.Size
		dc.curSize -= e.meta.Size
		delete(dc.manifest, e.key)
	}
	if dc.curSize < 0 {
		dc.curSize = 0
	}
	dc.dirty = true
}

// loadManifest 从磁盘加载 manifest，并对账数据文件。
func (dc *DiskCache) loadManifest() {
	manifestPath := filepath.Join(dc.baseDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("DiskCache manifest 读取失败: %v", err)
		}
		// manifest 缺失/不可读时，blocks 目录中的数据无法再被索引，直接清掉避免永久泄漏。
		dc.cleanupOrphans(nil)
		return
	}
	var m DiskManifest
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("DiskCache manifest 解析失败: %v", err)
		dc.cleanupOrphans(nil)
		return
	}
	if m.Blocks == nil {
		m.Blocks = make(map[string]*DiskBlockMeta) // 防御：JSON 无 "blocks" 字段
	}

	kept := make(map[string]*DiskBlockMeta, len(m.Blocks))
	expected := make(map[string]struct{}, len(m.Blocks))
	var cur int64
	for k, meta := range m.Blocks {
		if meta == nil {
			continue
		}
		path := dc.blockPath(meta)
		info, statErr := os.Stat(path)
		if statErr != nil || info.Size() != meta.Size || meta.Size <= 0 {
			_ = os.Remove(path)
			continue
		}
		if meta.File == "" {
			rel, _ := filepath.Rel(dc.baseDir, path)
			meta.File = rel
		}
		kept[k] = meta
		expected[path] = struct{}{}
		cur += meta.Size
	}
	dc.manifest = kept
	dc.curSize = cur
	dc.cleanupOrphans(expected)
	log.Printf("DiskCache 加载 manifest: %d 块, %.1f MB", len(dc.manifest), float64(dc.curSize)/(1024*1024))
}

// cleanupOrphans 删除不在 manifest 中的数据文件（.tmp-* 与 *.block）。
func (dc *DiskCache) cleanupOrphans(expected map[string]struct{}) {
	_ = filepath.WalkDir(dc.baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		name := d.Name()
		if name == "manifest.json" {
			return nil
		}
		if _, ok := expected[path]; ok {
			return nil
		}
		if filepath.Ext(name) == ".block" || (len(name) > 5 && name[:5] == ".tmp-") {
			_ = os.Remove(path)
		}
		return nil
	})
}

// saveManifest 将 manifest 刷盘（原子写：tmp+fsync+rename）。
func (dc *DiskCache) saveManifest() error {
	dc.mu.RLock()
	snapshot := make(map[string]*DiskBlockMeta, len(dc.manifest))
	for k, v := range dc.manifest {
		cp := *v
		snapshot[k] = &cp
	}
	dc.mu.RUnlock()

	m := DiskManifest{Version: 2, Blocks: snapshot}
	data, err := json.Marshal(m)
	if err != nil {
		log.Printf("DiskCache manifest 序列化失败: %v", err)
		return err
	}

	manifestPath := filepath.Join(dc.baseDir, "manifest.json")
	tmp, err := os.CreateTemp(dc.baseDir, ".manifest-*")
	if err != nil {
		log.Printf("DiskCache manifest temp: %v", err)
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
		log.Printf("DiskCache manifest 写入失败: %v", err)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		log.Printf("DiskCache manifest fsync 失败: %v", err)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, manifestPath); err != nil {
		os.Remove(tmpName)
		log.Printf("DiskCache manifest rename 失败: %v", err)
		return err
	}
	_ = os.Chmod(manifestPath, 0600)
	return nil
}
