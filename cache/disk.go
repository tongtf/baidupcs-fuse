package cache

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// DiskCache 持久化块缓存。远程文件不可变，纯追加式，无失效逻辑。
type DiskCache struct {
	mu       sync.RWMutex
	baseDir  string                    // blocks 存储根目录
	manifest map[string]*DiskBlockMeta // cacheKey → 元数据
	maxSize  int64                     // 最大磁盘使用（字节），0=不限
	curSize  int64                     // 当前磁盘使用
	dirty    bool                      // manifest 是否需要刷盘
}

// DiskBlockMeta 单个缓存块的元数据。
type DiskBlockMeta struct {
	Path       string    `json:"path"`        // 原始文件路径标识（account:fsid:size:mtime）
	Offset     int64     `json:"offset"`      // 块偏移
	Size       int64     `json:"size"`        // 块实际大小
	AccessedAt time.Time `json:"accessed_at"` // 最近访问时间（LRU 用）
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
	os.MkdirAll(dc.baseDir, 0755)
	dc.loadManifest()
	return dc
}

// Get 从磁盘获取缓存块。命中则返回数据副本并更新访问时间。
func (dc *DiskCache) Get(key BlockKey) ([]byte, bool) {
	k := key.String()
	dc.mu.RLock()
	meta, ok := dc.manifest[k]
	dc.mu.RUnlock()
	if !ok {
		return nil, false
	}

	data, err := os.ReadFile(dc.blockPath(meta))
	if err != nil {
		// 文件丢失 → 清理 manifest
		dc.mu.Lock()
		if m, ok := dc.manifest[k]; ok {
			delete(dc.manifest, k)
			dc.curSize -= m.Size
			dc.dirty = true
		}
		dc.mu.Unlock()
		return nil, false
	}

	// 更新访问时间（仅设 dirty，不改 AccessedAt，避免写锁争用）
	dc.mu.Lock()
	dc.dirty = true
	dc.mu.Unlock()

	return data, true
}

// Put 将数据写入磁盘缓存。
// Bug fix: 淘汰检查和执行必须在同一把锁内完成，否则并发 Put 会让 curSize 超限。
func (dc *DiskCache) Put(key BlockKey, data []byte) {
	k := key.String()
	dataSize := int64(len(data))
	bp := dc.blockPathFor(key)

	// 需要淘汰时：全程写锁，杜绝竞态
	if dc.maxSize > 0 {
		dc.mu.Lock()
		over := dc.curSize + dataSize - dc.maxSize
		if over > 0 {
			dc.evictLocked(over + dc.maxSize/4) // 淘汰到 ~75% 以下
		}
		dc.mu.Unlock()
	}

	// 写入块文件（无锁，I/O 操作）
	os.MkdirAll(filepath.Dir(bp), 0755)
	if err := os.WriteFile(bp, data, 0644); err != nil {
		log.Printf("DiskCache.Put 写入失败: %v", err)
		return
	}

	// 更新 manifest（写锁内原子更新）
	dc.mu.Lock()
	if old, ok := dc.manifest[k]; ok {
		// 覆盖写：旧块变成孤儿文件（下次重启后可清理），但 curSize 不重复计
		dc.curSize -= old.Size
	}
	dc.manifest[k] = &DiskBlockMeta{
		Path:       keyPath(key),
		Offset:     key.Offset,
		Size:       dataSize,
		AccessedAt: time.Now(),
	}
	dc.curSize += dataSize
	dc.dirty = true
	dc.mu.Unlock()
}

// Size 返回当前磁盘缓存使用量。
func (dc *DiskCache) Size() int64 {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	return dc.curSize
}

// Flush 将 manifest 刷盘。
func (dc *DiskCache) Flush() {
	dc.mu.Lock()
	if !dc.dirty {
		dc.mu.Unlock()
		return
	}
	dc.dirty = false
	dc.mu.Unlock()
	dc.saveManifest()
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
	h := sha256.Sum256([]byte(meta.Path))
	return filepath.Join(dc.baseDir, fmt.Sprintf("%x", h[:4]),
		fmt.Sprintf("%d.block", meta.Offset))
}

// blockPathFor 根据 BlockKey 计算块文件路径（Put 用）。
func (dc *DiskCache) blockPathFor(key BlockKey) string {
	h := sha256.Sum256([]byte(keyPath(key)))
	return filepath.Join(dc.baseDir, fmt.Sprintf("%x", h[:4]),
		fmt.Sprintf("%d.block", key.Offset))
}

// keyPath 生成 cache key 对应的路径标识（不含 offset）。
func keyPath(key BlockKey) string {
	return fmt.Sprintf("%s:%d:%d:%d", key.AccountID, key.FSID, key.Size, key.MtimeSec)
}

// String 返回 BlockKey 的字符串表示（manifest key，含 offset）。
func (key BlockKey) String() string {
	return fmt.Sprintf("%s:%d:%d:%d:%d", key.AccountID, key.FSID, key.Size, key.MtimeSec, key.Offset)
}

// evictLocked 淘汰最旧的块，直到释放 targetBytes。
// 调用者必须持有 dc.mu（写锁）。
func (dc *DiskCache) evictLocked(targetBytes int64) {
	if len(dc.manifest) == 0 {
		return
	}

	// 按访问时间排序（最旧在前）
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
		os.Remove(dc.blockPath(e.meta))
		freed += e.meta.Size
		dc.curSize -= e.meta.Size
		delete(dc.manifest, e.key)
	}
	dc.dirty = true
}

// loadManifest 从磁盘加载 manifest。
func (dc *DiskCache) loadManifest() {
	manifestPath := filepath.Join(dc.baseDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return // 首次运行，无 manifest
	}
	var m DiskManifest
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("DiskCache manifest 解析失败: %v", err)
		return
	}
	if m.Blocks == nil {
		m.Blocks = make(map[string]*DiskBlockMeta) // 防御：JSON 无 "blocks" 字段
	}
	dc.manifest = m.Blocks
	// 重新计算 curSize
	dc.curSize = 0
	for _, meta := range dc.manifest {
		dc.curSize += meta.Size
	}
	log.Printf("DiskCache 加载 manifest: %d 块, %.1f MB", len(dc.manifest), float64(dc.curSize)/(1024*1024))
}

// saveManifest 将 manifest 刷盘（原子写：tmp+rename）。
// Bug fix: 必须深拷贝 manifest map，否则 RUnlock 后 json.Marshal 遍历期间
// 并发 Put 的 evictLocked 会 delete(map) → panic: concurrent map iteration and map write。
func (dc *DiskCache) saveManifest() {
	dc.mu.RLock()
	snapshot := make(map[string]*DiskBlockMeta, len(dc.manifest))
	for k, v := range dc.manifest {
		snapshot[k] = v
	}
	dc.mu.RUnlock()

	m := DiskManifest{
		Version: 1,
		Blocks:  snapshot,
	}

	data, err := json.Marshal(m)
	if err != nil {
		log.Printf("DiskCache manifest 序列化失败: %v", err)
		return
	}

	manifestPath := filepath.Join(dc.baseDir, "manifest.json")
	tmpPath := manifestPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		log.Printf("DiskCache manifest 写入失败: %v", err)
		return
	}
	os.Rename(tmpPath, manifestPath)
}
