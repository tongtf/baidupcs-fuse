package cache

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDiskCache_LoadManifestNilBlocks 防御：manifest JSON 无 "blocks" 字段时不应 panic。
func TestDiskCache_LoadManifestNilBlocks(t *testing.T) {
	dir := t.TempDir()
	blocksDir := filepath.Join(dir, "blocks")
	os.MkdirAll(blocksDir, 0755)

	// 写入一个没有 "blocks" 字段的 manifest
	manifest := []byte(`{"v":1}`)
	os.WriteFile(filepath.Join(blocksDir, "manifest.json"), manifest, 0644)

	// 不应 panic
	dc := NewDiskCache(dir, 100*1024*1024)
	defer dc.Close()

	// Put 应正常工作
	key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: 0}
	dc.Put(key, []byte("data"))

	got, ok := dc.Get(key)
	if !ok {
		t.Fatal("Get returned false after nil-blocks manifest recovery")
	}
	if string(got) != "data" {
		t.Fatalf("got %q, want %q", got, "data")
	}
}

// TestDiskCache_LoadManifestEmptyJson 空 JSON 对象。
func TestDiskCache_LoadManifestEmptyJson(t *testing.T) {
	dir := t.TempDir()
	blocksDir := filepath.Join(dir, "blocks")
	os.MkdirAll(blocksDir, 0755)

	os.WriteFile(filepath.Join(blocksDir, "manifest.json"), []byte(`{}`), 0644)

	dc := NewDiskCache(dir, 100*1024*1024)
	defer dc.Close()

	if dc.Size() != 0 {
		t.Fatalf("Size=%d, want 0", dc.Size())
	}

	key := BlockKey{AccountID: "test", FSID: 1, Size: 100, MtimeSec: 1, Offset: 0}
	dc.Put(key, []byte("data"))
	if dc.Size() != 4 {
		t.Fatalf("Size=%d, want 4", dc.Size())
	}
}
