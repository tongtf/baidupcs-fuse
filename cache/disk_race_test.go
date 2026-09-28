package cache

import (
	"sync"
	"testing"
	"time"
)

// TestDiskCache_SaveManifestRace 复现 saveManifest 并发 map 读写 panic。
// 在旧代码中，saveManifest RUnlock 后 json.Marshal 仍在遍历 dc.manifest，
// 并发 Put 的 evictLocked 执行 delete(dc.manifest, key) → panic。
func TestDiskCache_SaveManifestRace(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 500) // 小容量，频繁淘汰
	defer dc.Close()

	var wg sync.WaitGroup
	done := make(chan struct{})

	// 并发 Put（触发淘汰 → delete(map)）
	for i := int64(0); i < 50; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 50}
			dc.Put(key, make([]byte, 50))
		}(i)
	}

	// 并发 Flush（触发 saveManifest → json.Marshal 遍历 map）
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				dc.Flush()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// 等待所有 goroutine 完成
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// OK — 没有 panic
	case <-time.After(5 * time.Second):
		t.Fatal("timeout (possible deadlock)")
	}
}

// TestDiskCache_SaveManifestIntegrity 验证 saveManifest 保存的 manifest 与内存一致。
func TestDiskCache_SaveManifestIntegrity(t *testing.T) {
	dir := t.TempDir()
	dc := NewDiskCache(dir, 100*1024*1024)

	// 写入 20 块
	for i := int64(0); i < 20; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 100}
		dc.Put(key, make([]byte, 100))
	}

	// Flush 保存 manifest
	dc.Flush()
	dc.Close()

	// 重新加载并验证
	dc2 := NewDiskCache(dir, 100*1024*1024)
	defer dc2.Close()

	if dc2.Size() != dc.Size() {
		t.Fatalf("size mismatch: loaded=%d, original=%d", dc2.Size(), dc.Size())
	}

	// 每个块都能读取
	for i := int64(0); i < 20; i++ {
		key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: i * 100}
		_, ok := dc2.Get(key)
		if !ok {
			t.Fatalf("block %d not found after reload", i)
		}
	}
}
