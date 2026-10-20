package cache

import (
	"container/list"
	"sync"
	"time"

	"baidupcs-fuse/mount"
)

// MetadataCache 文件 stat + 目录列表的内存 TTL 缓存。
//
// 并发/一致性约定：
//   - 返回给调用方的 RemoteEntry 一律是副本，调用方可以修改自己的 node.entry；
//   - 写操作后必须调用 Invalidate/InvalidateDir/InvalidateParent/InvalidatePrefix；
//   - 文件和目录都使用 LRU 淘汰，淘汰是 O(1)（container/list）；
//   - 支持短 TTL 负缓存，避免反复 Stat 不存在的路径。
type MetadataCache struct {
	mu       sync.RWMutex
	entries  map[string]*cacheEntry
	entryLRU *list.List
	dirs     map[string]*dirCacheEntry
	dirLRU   *list.List
	negative map[string]time.Time
	fileTTL  time.Duration
	dirTTL   time.Duration
	negTTL   time.Duration
	prefix   string // accountKey 前缀
	maxFiles int    // 0=不限
	maxDirs  int
	// 负缓存也必须有界：扫描大量不存在路径时不能无限增长。
	maxNegative int
}

type cacheEntry struct {
	key      string
	entry    *mount.RemoteEntry
	expireAt time.Time
	elem     *list.Element
}

type dirCacheEntry struct {
	key             string
	entries         []*mount.RemoteEntry
	expireAt        time.Time
	lastRefreshedAt time.Time
	elem            *list.Element
}

// NewMetadataCache 创建缓存。accountKey 用于隔离不同账户的缓存 key。
func NewMetadataCache(accountKey string, fileTTL, dirTTL, negTTL time.Duration) *MetadataCache {
	return &MetadataCache{
		entries:     make(map[string]*cacheEntry),
		entryLRU:    list.New(),
		dirs:        make(map[string]*dirCacheEntry),
		dirLRU:      list.New(),
		negative:    make(map[string]time.Time),
		fileTTL:     fileTTL,
		dirTTL:      dirTTL,
		negTTL:      negTTL,
		prefix:      accountKey,
		maxFiles:    10000,
		maxDirs:     5000,
		maxNegative: 10000,
	}
}

func (c *MetadataCache) key(path string) string {
	return c.prefix + ":" + path
}

// Get 获取文件 stat 缓存副本。命中且未过期返回 (entry, true)。
func (c *MetadataCache) Get(path string) (*mount.RemoteEntry, bool) {
	k := c.key(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[k]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expireAt) {
		c.removeEntryLocked(k, e)
		return nil, false
	}
	c.entryLRU.MoveToBack(e.elem)
	return cloneRemoteEntry(e.entry), true
}

// Set 写入文件 stat 缓存（存储副本）。
func (c *MetadataCache) Set(path string, entry *mount.RemoteEntry) {
	if entry == nil {
		return
	}
	k := c.key(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.negative, k)
	if e, ok := c.entries[k]; ok {
		e.entry = cloneRemoteEntry(entry)
		e.expireAt = time.Now().Add(c.fileTTL)
		c.entryLRU.MoveToBack(e.elem)
		return
	}
	e := &cacheEntry{key: k, entry: cloneRemoteEntry(entry), expireAt: time.Now().Add(c.fileTTL)}
	e.elem = c.entryLRU.PushBack(e)
	c.entries[k] = e
	for c.maxFiles > 0 && len(c.entries) > c.maxFiles {
		c.removeEntryLocked(c.entryLRU.Front().Value.(*cacheEntry).key, c.entryLRU.Front().Value.(*cacheEntry))
	}
}

// GetNegative 判断路径是否在负缓存 TTL 内。
func (c *MetadataCache) GetNegative(path string) bool {
	k := c.key(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	exp, ok := c.negative[k]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(c.negative, k)
		return false
	}
	return true
}

// SetNegative 记录一次“不存在”，持续 negTTL。
func (c *MetadataCache) SetNegative(path string) {
	if c.negTTL <= 0 {
		return
	}
	k := c.key(path)
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxNegative > 0 && len(c.negative) >= c.maxNegative {
		for nk, exp := range c.negative {
			if now.After(exp) {
				delete(c.negative, nk)
			}
		}
		for nk := range c.negative {
			if len(c.negative) < c.maxNegative {
				break
			}
			delete(c.negative, nk)
		}
	}
	c.negative[k] = now.Add(c.negTTL)
}

// GetDir 获取目录列表缓存副本。
func (c *MetadataCache) GetDir(path string) ([]*mount.RemoteEntry, bool) {
	k := c.key(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.dirs[k]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expireAt) {
		c.removeDirLocked(k, e)
		return nil, false
	}
	c.dirLRU.MoveToBack(e.elem)
	out := make([]*mount.RemoteEntry, len(e.entries))
	for i, en := range e.entries {
		out[i] = cloneRemoteEntry(en)
	}
	return out, true
}

// SetDir 写入目录列表缓存（存储副本）。
func (c *MetadataCache) SetDir(path string, entries []*mount.RemoteEntry) {
	k := c.key(path)
	cloned := make([]*mount.RemoteEntry, len(entries))
	for i, en := range entries {
		cloned[i] = cloneRemoteEntry(en)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// 目录列表出现某条目时，清除它的负缓存，避免 readdir 看得到但 lookup 仍 ENOENT。
	for _, en := range cloned {
		if en != nil {
			delete(c.negative, c.key(joinMetaPath(path, en.Name)))
		}
	}
	if e, ok := c.dirs[k]; ok {
		e.entries = cloned
		e.expireAt = time.Now().Add(c.dirTTL)
		e.lastRefreshedAt = time.Now()
		c.dirLRU.MoveToBack(e.elem)
		return
	}
	e := &dirCacheEntry{key: k, entries: cloned, expireAt: time.Now().Add(c.dirTTL), lastRefreshedAt: time.Now()}
	e.elem = c.dirLRU.PushBack(e)
	c.dirs[k] = e
	for c.maxDirs > 0 && len(c.dirs) > c.maxDirs {
		front := c.dirLRU.Front().Value.(*dirCacheEntry)
		c.removeDirLocked(front.key, front)
	}
}

// Invalidate 失效单个文件 stat 缓存与负缓存。
func (c *MetadataCache) Invalidate(path string) {
	k := c.key(path)
	c.mu.Lock()
	if e, ok := c.entries[k]; ok {
		c.removeEntryLocked(k, e)
	}
	delete(c.negative, k)
	c.mu.Unlock()
}

// InvalidateDir 失效单个目录列表缓存。
func (c *MetadataCache) InvalidateDir(dirPath string) {
	k := c.key(dirPath)
	c.mu.Lock()
	if e, ok := c.dirs[k]; ok {
		c.removeDirLocked(k, e)
	}
	c.mu.Unlock()
}

// joinMetaPath 拼接 cache key 用的子路径（根目录特殊处理）。
func joinMetaPath(dir, name string) string {
	if dir == "" || dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

// InvalidateParent 失效 parent dir 的 listing 缓存（mkdir/create/unlink/rename 后）。
func (c *MetadataCache) InvalidateParent(childPath string) {
	parent := "/"
	for i := len(childPath) - 1; i >= 0; i-- {
		if childPath[i] == '/' {
			if i > 0 {
				parent = childPath[:i]
			}
			break
		}
	}
	c.InvalidateDir(parent)
}

// InvalidatePrefix 失效 path 及其整棵子树的 stat/dir/negative 缓存（目录 rename/delete 后）。
func (c *MetadataCache) InvalidatePrefix(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	base := c.key(path)
	child := base + "/"
	match := func(k string) bool {
		return k == base || (len(k) > len(child) && k[:len(child)] == child)
	}
	for k, e := range c.entries {
		if match(k) {
			c.removeEntryLocked(k, e)
		}
	}
	for k, e := range c.dirs {
		if match(k) {
			c.removeDirLocked(k, e)
		}
	}
	for k := range c.negative {
		if match(k) {
			delete(c.negative, k)
		}
	}
}

func (c *MetadataCache) removeEntryLocked(k string, e *cacheEntry) {
	delete(c.entries, k)
	if e.elem != nil {
		c.entryLRU.Remove(e.elem)
		e.elem = nil
	}
}

func (c *MetadataCache) removeDirLocked(k string, e *dirCacheEntry) {
	delete(c.dirs, k)
	if e.elem != nil {
		c.dirLRU.Remove(e.elem)
		e.elem = nil
	}
}

func cloneRemoteEntry(e *mount.RemoteEntry) *mount.RemoteEntry {
	if e == nil {
		return nil
	}
	cp := *e
	return &cp
}

// DirInfo 后台刷新用的目录信息。
type DirInfo struct {
	Key  string // 缓存 key（accountKey:path）
	Path string // 原始路径
}

// ListDirs 返回所有缓存的目录路径（后台刷新用）。
func (c *MetadataCache) ListDirs() []DirInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]DirInfo, 0, len(c.dirs))
	for k := range c.dirs {
		path := k
		if len(k) > len(c.prefix)+1 {
			path = k[len(c.prefix)+1:]
		}
		result = append(result, DirInfo{Key: k, Path: path})
	}
	return result
}

// SetDirRefreshed 更新目录缓存的刷新时间戳。
func (c *MetadataCache) SetDirRefreshed(dirPath string) {
	c.mu.Lock()
	if e, ok := c.dirs[c.key(dirPath)]; ok {
		e.lastRefreshedAt = time.Now()
	}
	c.mu.Unlock()
}

// NeedsRefresh 检查目录是否需要刷新（距上次刷新超过 interval）。
func (c *MetadataCache) NeedsRefresh(dirPath string, interval time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if interval <= 0 {
		return false
	}
	e, ok := c.dirs[c.key(dirPath)]
	if !ok {
		return false
	}
	// 即使目录列表已过 TTL，也要允许后台/手动刷新重新填充；
	// 否则默认 DirRefreshInterval(24h) > DirTTL(5m) 时永远刷不到。
	return time.Since(e.lastRefreshedAt) >= interval
}
