package cache

import (
	"sync"
	"time"

	"baidupcs-fuse/mount"
)

// MetadataCache 文件 stat + 目录列表的内存 TTL 缓存。
// 写操作后必须调用 Invalidate/InvalidateDir/InvalidateParent 立即失效。
type MetadataCache struct {
	mu           sync.RWMutex
	entries      map[string]*cacheEntry    // accountKey+":"+path → cacheEntry
	dirs         map[string]*dirCacheEntry // accountKey+":"+dirPath → dirCacheEntry
	fileTTL      time.Duration
	dirTTL       time.Duration
	negTTL       time.Duration
	prefix       string // accountKey 前缀
	maxFileEntries int  // 文件缓存最大条目数（0=不限）
	maxDirEntries  int  // 目录缓存最大条目数（0=不限）
}

type cacheEntry struct {
	entry    *mount.RemoteEntry
	expireAt time.Time
}

type dirCacheEntry struct {
	entries        []*mount.RemoteEntry
	expireAt       time.Time
	lastRefreshedAt time.Time // 上次刷新时间（后台刷新用）
}

// NewMetadataCache 创建缓存。accountKey 用于隔离不同账户的缓存 key。
func NewMetadataCache(accountKey string, fileTTL, dirTTL, negTTL time.Duration) *MetadataCache {
	return &MetadataCache{
		entries:      make(map[string]*cacheEntry),
		dirs:         make(map[string]*dirCacheEntry),
		fileTTL:      fileTTL,
		dirTTL:       dirTTL,
		negTTL:       negTTL,
		prefix:       accountKey,
		maxFileEntries: 10000,
		maxDirEntries:  5000,
	}
}

func (c *MetadataCache) key(path string) string {
	return c.prefix + ":" + path
}

// Get 获取文件 stat 缓存。命中且未过期返回 (entry, true)。
func (c *MetadataCache) Get(path string) (*mount.RemoteEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[c.key(path)]
	if !ok || time.Now().After(e.expireAt) {
		return nil, false
	}
	return e.entry, true
}

// Set 写入文件 stat 缓存。超过容量上限时淘汰最旧条目。
func (c *MetadataCache) Set(path string, entry *mount.RemoteEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 容量淘汰
	if c.maxFileEntries > 0 && len(c.entries) >= c.maxFileEntries {
		c.evictOldestEntries(c.maxFileEntries / 4) // 淘汰 25%
	}
	c.entries[c.key(path)] = &cacheEntry{
		entry:    entry,
		expireAt: time.Now().Add(c.fileTTL),
	}
}

// GetDir 获取目录列表缓存。
func (c *MetadataCache) GetDir(path string) ([]*mount.RemoteEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.dirs[c.key(path)]
	if !ok || time.Now().After(e.expireAt) {
		return nil, false
	}
	return e.entries, true
}

// SetDir 写入目录列表缓存。超过容量上限时淘汰最旧条目。
func (c *MetadataCache) SetDir(path string, entries []*mount.RemoteEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 容量淘汰
	if c.maxDirEntries > 0 && len(c.dirs) >= c.maxDirEntries {
		c.evictOldestDirs(c.maxDirEntries / 4)
	}
	c.dirs[c.key(path)] = &dirCacheEntry{
		entries:  entries,
		expireAt: time.Now().Add(c.dirTTL),
	}
}

// Invalidate 失效单个文件 stat 缓存。
func (c *MetadataCache) Invalidate(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, c.key(path))
}

// InvalidateDir 失效单个目录列表缓存。
func (c *MetadataCache) InvalidateDir(dirPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.dirs, c.key(dirPath))
}

// InvalidateParent 失效 parent dir 的 listing 缓存（mkdir/create/unlink/rename 后）。
func (c *MetadataCache) InvalidateParent(childPath string) {
	// /a/b/c → parent = /a/b
	for i := len(childPath) - 1; i >= 0; i-- {
		if childPath[i] == '/' {
			parent := childPath[:i]
			if parent == "" {
				parent = "/"
			}
			c.InvalidateDir(parent)
			return
		}
	}
	// 无 '/' → parent = "/"
	c.InvalidateDir("/")
}

// evictOldestEntries 淘汰最旧的 n 个文件缓存条目（简单 FIFO）。
func (c *MetadataCache) evictOldestEntries(n int) {
	oldest := make([]*cacheEntry, 0, n)
	oldestKeys := make([]string, 0, n)
	for k, e := range c.entries {
		if len(oldest) < n {
			oldest = append(oldest, e)
			oldestKeys = append(oldestKeys, k)
			continue
			// 找更旧的替换
		}
		for i, o := range oldest {
			if e.expireAt.Before(o.expireAt) {
				oldest[i] = e
				oldestKeys[i] = k
				break
			}
		}
	}
	for _, k := range oldestKeys {
		delete(c.entries, k)
	}
}

// evictOldestDirs 淘汰最旧的 n 个目录缓存条目。
func (c *MetadataCache) evictOldestDirs(n int) {
	oldest := make([]*dirCacheEntry, 0, n)
	oldestKeys := make([]string, 0, n)
	for k, e := range c.dirs {
		if len(oldest) < n {
			oldest = append(oldest, e)
			oldestKeys = append(oldestKeys, k)
			continue
		}
		for i, o := range oldest {
			if e.expireAt.Before(o.expireAt) {
				oldest[i] = e
				oldestKeys[i] = k
				break
			}
		}
	}
	for _, k := range oldestKeys {
		delete(c.dirs, k)
	}
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
	for k, e := range c.dirs {
		if time.Now().After(e.expireAt) {
			continue // 跳过已过期的
		}
		// 从 key 中提取路径：去掉 prefix 前缀
		path := k
		if len(c.prefix) > 0 && len(k) > len(c.prefix)+1 {
			path = k[len(c.prefix)+1:]
		}
		result = append(result, DirInfo{Key: k, Path: path})
	}
	return result
}

// SetDirRefreshed 更新目录缓存的刷新时间戳。
func (c *MetadataCache) SetDirRefreshed(dirPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.dirs[c.key(dirPath)]
	if ok {
		e.lastRefreshedAt = time.Now()
	}
}

// NeedsRefresh 检查目录是否需要刷新（距上次刷新超过 interval）。
func (c *MetadataCache) NeedsRefresh(dirPath string, interval time.Duration) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.dirs[c.key(dirPath)]
	if !ok {
		return false // 不在缓存中，不需要刷新
	}
	if time.Now().After(e.expireAt) {
		return false // 已过期，下次访问会自动重新拉取
	}
	return time.Since(e.lastRefreshedAt) >= interval
}
