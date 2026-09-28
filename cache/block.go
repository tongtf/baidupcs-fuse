package cache

import (
	"container/list"
	"sync"
)

// BlockKey 块缓存的唯一标识。
type BlockKey struct {
	AccountID string // 账户标识（前缀隔离）
	FSID      int64  // 远端 fs_id
	Size      int64  // pinned size（open 时锁定）
	MtimeSec  int64  // pinned mtime（版本指纹）
	Offset    int64  // 块起始偏移 = index * blockSize
}

type blockEntry struct {
	key  BlockKey
	data []byte
}

// BlockCache FIFO 块缓存。v1 单线程无竞争，Get 返回引用不 copy（PLAN-012）。
type BlockCache struct {
	mu        sync.RWMutex
	blocks    *list.List              // FIFO 队列（头=最旧）
	index     map[BlockKey]*list.Element
	totalSize int64
	maxSize   int64
	blockSize int64
	prefix    string // accountKey
}

// NewBlockCache 创建缓存。maxSize 默认 128MB，blockSize 默认 16MB。
func NewBlockCache(accountKey string, maxSize, blockSize int64) *BlockCache {
	return &BlockCache{
		blocks:    list.New(),
		index:     make(map[BlockKey]*list.Element),
		maxSize:   maxSize,
		blockSize: blockSize,
		prefix:    accountKey,
	}
}

// BlockKeyFromPath 构造 BlockKey，自动填充 AccountID。
func BlockKeyFromPath(accountKey string, fsid, size, mtime, offset int64) BlockKey {
	return BlockKey{
		AccountID: accountKey,
		FSID:      fsid,
		Size:      size,
		MtimeSec:  mtime,
		Offset:    offset,
	}
}

// Get 获取缓存块。返回 data 副本（并发安全）。
func (c *BlockCache) Get(k BlockKey) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.index[k]
	if !ok {
		return nil, false
	}
	src := e.Value.(*blockEntry).data
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst, true
}

// Put 放入缓存块。超容量时 FIFO 淘汰最旧块。
func (c *BlockCache) Put(k BlockKey, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 已存在 → 更新
	if e, ok := c.index[k]; ok {
		c.blocks.MoveToBack(e)
		e.Value.(*blockEntry).data = data
		return
	}

	// 淘汰直到有空间
	for c.totalSize+int64(len(data)) > c.maxSize && c.blocks.Len() > 0 {
		front := c.blocks.Front()
		c.blocks.Remove(front)
		removed := front.Value.(*blockEntry)
		delete(c.index, removed.key)
		c.totalSize -= int64(len(removed.data))
	}

	e := c.blocks.PushBack(&blockEntry{key: k, data: data})
	c.index[k] = e
	c.totalSize += int64(len(data))
}

// Invalidate 删除指定块。
func (c *BlockCache) Invalidate(k BlockKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.index[k]; ok {
		c.blocks.Remove(e)
		c.totalSize -= int64(len(e.Value.(*blockEntry).data))
		delete(c.index, k)
	}
}

// InvalidateByFSID 删除指定 fsID 的所有缓存块（upload 成功后调用，防读到旧版本）。
func (c *BlockCache) InvalidateByFSID(accountID string, fsID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, e := range c.index {
		if key.AccountID == accountID && key.FSID == fsID {
			c.blocks.Remove(e)
			c.totalSize -= int64(len(e.Value.(*blockEntry).data))
			delete(c.index, key)
		}
	}
}

// InvalidateAllByAccount 删除指定账户的所有缓存块（fsID 未知时全清）。
func (c *BlockCache) InvalidateAllByAccount(accountID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, e := range c.index {
		if key.AccountID == accountID {
			c.blocks.Remove(e)
			c.totalSize -= int64(len(e.Value.(*blockEntry).data))
			delete(c.index, key)
		}
	}
}
