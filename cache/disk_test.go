package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func TestDiskCache_PutGet(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100*1024*1024)
	defer dc.Close()

	key := BlockKey{AccountID: "test", FSID: 123, Size: 1000, MtimeSec: 100, Offset: 0}
	data := []byte("hello world")

	dc.Put(key, data)
	if dc.Size() != int64(len(data)) {
		t.Fatalf("Size=%d, want %d", dc.Size(), len(data))
	}

	got, ok := dc.Get(key)
	if !ok {
		t.Fatal("Get returned false")
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestDiskCache_Miss(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100*1024*1024)
	defer dc.Close()

	key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: 0}
	_, ok := dc.Get(key)
	if ok {
		t.Fatal("Get should return false for missing key")
	}
}

func TestDiskCache_Persistence(t *testing.T) {
	dir := t.TempDir()
	key := BlockKey{AccountID: "test", FSID: 42, Size: 500, MtimeSec: 99, Offset: 1024}
	data := []byte("persistent data")

	dc1 := NewDiskCache(dir, 100*1024*1024)
	dc1.Put(key, data)
	dc1.Close()

	dc2 := NewDiskCache(dir, 100*1024*1024)
	defer dc2.Close()

	got, ok := dc2.Get(key)
	if !ok {
		t.Fatal("Get returned false after reopen")
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestDiskCache_MultipleBlocks(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100*1024*1024)
	defer dc.Close()

	for i := int64(0); i < 10; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 1000, MtimeSec: 1, Offset: i * 100}
		data := []byte(fmt.Sprintf("block-%d-data-padding-to-make-it-longer", i))
		dc.Put(key, data)
	}

	for i := int64(0); i < 10; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 1000, MtimeSec: 1, Offset: i * 100}
		got, ok := dc.Get(key)
		if !ok {
			t.Fatalf("block %d: Get returned false", i)
		}
		if len(got) == 0 {
			t.Fatalf("block %d: empty data", i)
		}
	}
}

func TestDiskCache_DifferentFiles(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100*1024*1024)
	defer dc.Close()

	key1 := BlockKey{AccountID: "a", FSID: 1, Size: 100, MtimeSec: 1, Offset: 0}
	key2 := BlockKey{AccountID: "a", FSID: 2, Size: 200, MtimeSec: 2, Offset: 0}

	dc.Put(key1, []byte("file1"))
	dc.Put(key2, []byte("file2"))

	got1, _ := dc.Get(key1)
	got2, _ := dc.Get(key2)
	if string(got1) != "file1" || string(got2) != "file2" {
		t.Fatalf("cross-contamination: got1=%q, got2=%q", got1, got2)
	}
}

func TestDiskCache_Eviction(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100) // 100 字节上限
	defer dc.Close()

	for i := int64(0); i < 5; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 32}
		dc.Put(key, make([]byte, 32))
	}

	if dc.Size() > 100 {
		t.Fatalf("size=%d, should be <= 100 after eviction", dc.Size())
	}
}

func TestDiskCache_FileNotExist(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100*1024*1024)
	defer dc.Close()

	key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: 0}
	dc.Put(key, []byte("data"))

	os.Remove(dc.blockPathFor(key))

	_, ok := dc.Get(key)
	if ok {
		t.Fatal("Get should return false when block file is missing")
	}
}

func TestDiskCache_ConcurrentPutGet(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100*1024*1024)
	defer dc.Close()

	done := make(chan struct{})
	for i := int64(0); i < 10; i++ {
		go func(i int64) {
			key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 10}
			dc.Put(key, make([]byte, 10))
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}

	for i := int64(0); i < 10; i++ {
		go func(i int64) {
			key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 10}
			dc.Get(key)
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestDiskCache_ZeroMaxSize(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 0) // 不限制
	defer dc.Close()

	for i := int64(0); i < 100; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 1000, MtimeSec: 1, Offset: i * 100}
		dc.Put(key, make([]byte, 100))
	}

	if dc.Size() != 10000 {
		t.Fatalf("Size=%d, want 10000", dc.Size())
	}
}

// ============================================================
// 竞态与边界测试
// ============================================================

// TestDiskCache_ConcurrentPutEvict 复现 curSize 竞态导致负数的 bug。
func TestDiskCache_ConcurrentPutEvict(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 1000) // 1KB 上限
	defer dc.Close()

	var negativeDetected int32

	// 并发写入，触发频繁淘汰
	var wg sync.WaitGroup
	for i := int64(0); i < 100; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 50}
			dc.Put(key, make([]byte, 50))
			// 检查 curSize 是否为负
			if dc.Size() < 0 {
				atomic.AddInt32(&negativeDetected, 1)
			}
		}(i)
	}
	wg.Wait()

	if negativeDetected > 0 {
		t.Fatalf("curSize went negative %d times (race condition in Put)", negativeDetected)
	}

	// 最终 curSize 应该非负且合理
	if dc.Size() < 0 {
		t.Fatalf("final curSize=%d (negative!)", dc.Size())
	}
	t.Logf("final curSize=%d (max=1000)", dc.Size())
}

// TestDiskCache_ConcurrentPutGetEvict 混合读写淘汰竞态。
func TestDiskCache_ConcurrentPutGetEvict(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 500) // 500 字节上限
	defer dc.Close()

	var negativeDetected int32

	// 先填充
	for i := int64(0); i < 5; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 100}
		dc.Put(key, make([]byte, 100))
	}

	var wg sync.WaitGroup

	// 并发 Put（写新块，触发淘汰）
	for i := int64(5); i < 50; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 100}
			dc.Put(key, make([]byte, 100))
		}(i)
	}

	// 并发 Get（可能删除缺失条目）
	for i := int64(0); i < 5; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 100}
			dc.Get(key)
		}(i)
	}

	// 并发检查 curSize
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := dc.Size()
			if s < 0 {
				atomic.AddInt32(&negativeDetected, 1)
			}
		}()
	}

	wg.Wait()

	if negativeDetected > 0 {
		t.Fatalf("curSize went negative %d times", negativeDetected)
	}
}

// TestDiskCache_EvictAll 验证淘汰不超过 targetBytes。
func TestDiskCache_EvictAll(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100)
	defer dc.Close()

	// 填充到 120 字节（超过 100 上限）
	for i := int64(0); i < 4; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 30}
		dc.Put(key, make([]byte, 30))
	}

	// 再写入一块，触发淘汰
	key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: 120}
	dc.Put(key, make([]byte, 30))

	if dc.Size() < 0 {
		t.Fatalf("curSize=%d after eviction (negative!)", dc.Size())
	}
	t.Logf("curSize=%d after eviction", dc.Size())
}

// TestDiskCache_PutOverwrite Put 同一个 key 两次（覆盖写）。
func TestDiskCache_PutOverwrite(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 100*1024*1024)
	defer dc.Close()

	key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: 0}

	dc.Put(key, []byte("version1"))
	dc.Put(key, []byte("version2"))

	got, ok := dc.Get(key)
	if !ok {
		t.Fatal("Get returned false")
	}
	if string(got) != "version2" {
		t.Fatalf("got %q, want %q", got, "version2")
	}

	// curSize 应该是两块的大小之和（因为旧块没有被清理）
	// 实际上覆盖写会导致旧块变成孤儿文件，curSize 只增不减
}

// TestDiskCache_SaveManifestAtomic 验证 manifest 原子写。
func TestDiskCache_SaveManifestAtomic(t *testing.T) {
	dir := t.TempDir()
	dc := NewDiskCache(dir, 100*1024*1024)

	// 写入数据
	for i := int64(0); i < 10; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 100}
		dc.Put(key, make([]byte, 100))
	}
	dc.Flush()
	dc.Close()

	// 验证 manifest.json 存在且可解析
	manifestPath := dir + "/blocks/manifest.json"
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("manifest.json not found: %v", err)
	}

	var m DiskManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("manifest.json corrupted: %v", err)
	}

	if len(m.Blocks) != 10 {
		t.Fatalf("manifest has %d blocks, want 10", len(m.Blocks))
	}

	// 验证 .tmp 文件不存在
	if _, err := os.Stat(manifestPath + ".tmp"); err == nil {
		t.Fatal(".tmp file should not exist after Flush")
	}
}

// TestDiskCache_LargeBlocks 模拟大文件（1000块）缓存。
func TestDiskCache_LargeBlocks(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 50*1024) // 50KB 上限
	defer dc.Close()

	for i := int64(0); i < 1000; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 1000, MtimeSec: 1, Offset: i * 100}
		dc.Put(key, make([]byte, 100))
	}

	// 淘汰后 curSize 应 <= 50KB
	if dc.Size() > 50*1024 {
		t.Fatalf("curSize=%d, should be <= 50KB", dc.Size())
	}
	t.Logf("curSize=%d after 1000 puts with 50KB limit", dc.Size())
}
