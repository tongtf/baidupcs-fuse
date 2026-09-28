package cache

import (
	"sync"
	"testing"
)

func TestBlockCache_GetPut(t *testing.T) {
	c := NewMemoryOnlyBlockCache(128*1024*1024, 16*1024*1024)
	k := BlockKey{AccountID: "a", FSID: 1, Size: 100, MtimeSec: 10, Offset: 0}
	data := []byte("hello")
	c.Put(k, data)
	got, ok := c.Get(k)
	if !ok || string(got) != "hello" {
		t.Errorf("got %v, %v, want hello, true", got, ok)
	}
}

func TestBlockCache_Miss(t *testing.T) {
	c := NewMemoryOnlyBlockCache(128*1024*1024, 16*1024*1024)
	k := BlockKey{AccountID: "a", FSID: 1}
	_, ok := c.Get(k)
	if ok {
		t.Error("expected miss")
	}
}

func TestBlockCache_FIFO_Eviction(t *testing.T) {
	// 8 块容量，放 9 块，第 1 块被淘汰
	c := NewMemoryOnlyBlockCache(8*1024*1024, 1024*1024) // 8MB / 1MB = 8 blocks
	for i := int64(0); i < 9; i++ {
		k := BlockKey{AccountID: "a", FSID: 1, Offset: i * 1024 * 1024}
		c.Put(k, make([]byte, 1024*1024))
	}
	// 第 1 块 (offset=0) 应被淘汰
	_, ok := c.Get(BlockKey{AccountID: "a", FSID: 1, Offset: 0})
	if ok {
		t.Error("expected first block evicted")
	}
	// 第 2 块 (offset=1MB) 应还在
	_, ok = c.Get(BlockKey{AccountID: "a", FSID: 1, Offset: 1024 * 1024})
	if !ok {
		t.Error("expected second block present")
	}
}

func TestBlockCache_Invalidate(t *testing.T) {
	c := NewMemoryOnlyBlockCache(128*1024*1024, 16*1024*1024)
	k := BlockKey{AccountID: "a", FSID: 1}
	c.Put(k, []byte("data"))
	c.Invalidate(k)
	_, ok := c.Get(k)
	if ok {
		t.Error("expected invalidated")
	}
}

func TestBlockCache_Overwrite(t *testing.T) {
	c := NewMemoryOnlyBlockCache(128*1024*1024, 16*1024*1024)
	k := BlockKey{AccountID: "a", FSID: 1}
	c.Put(k, []byte("old"))
	c.Put(k, []byte("new"))
	got, _ := c.Get(k)
	if string(got) != "new" {
		t.Errorf("got %q, want %q", got, "new")
	}
}

func TestBlockCache_Concurrent(t *testing.T) {
	c := NewMemoryOnlyBlockCache(128*1024*1024, 16*1024*1024)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				k := BlockKey{AccountID: "a", FSID: int64(i), Offset: int64(j * 1024)}
				c.Put(k, []byte{byte(j)})
				c.Get(k)
			}
		}(i)
	}
	wg.Wait()
}

// NewMemoryOnlyBlockCache 创建测试用 BlockCache。
func NewMemoryOnlyBlockCache(maxSize, blockSize int64) *BlockCache {
	return NewBlockCache("test", maxSize, blockSize)
}
