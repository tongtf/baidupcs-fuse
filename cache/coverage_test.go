package cache

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"baidupcs-fuse/mount"
)

// 目的：块缓存按 (account, fsID) 精确失效，且不影响其他账户/文件；容量统计同步更新。
func TestBlockCache_HasAndInvalidateFSID(t *testing.T) {
	c := NewMemoryOnlyBlockCache(64*1024*1024, 1024)
	mk := func(fsid, off int64) BlockKey {
		return BlockKeyFromPath("acct", fsid, 100, 1, off)
	}
	k1, k2, other := mk(1, 0), mk(1, 1024), mk(2, 0)
	c.Put(k1, []byte("a"))
	c.Put(k2, []byte("b"))
	c.Put(other, []byte("c"))

	if !c.Has(k1) || c.Has(mk(3, 0)) {
		t.Fatal("Has returned wrong result")
	}

	c.InvalidateFSID("acct", 1)
	if c.Has(k1) || c.Has(k2) {
		t.Fatal("fsid=1 blocks should be evicted")
	}
	if !c.Has(other) {
		t.Fatal("other fsid must remain")
	}
	if c.totalSize != 1 {
		t.Fatalf("totalSize=%d, want 1", c.totalSize)
	}

	// 不同账户不误删。
	c.Put(BlockKeyFromPath("other-acct", 1, 100, 1, 0), []byte("x"))
	c.InvalidateFSID("acct", 1)
	if !c.Has(other) {
		t.Fatal("other account block must remain")
	}
}

// 目的：单块超过缓存容量时不得入缓存，避免撑爆容量。
func TestBlockCache_OversizeBlockIgnored(t *testing.T) {
	c := NewBlockCache("a", 4, 4)
	c.Put(BlockKey{FSID: 1}, []byte("12345"))
	if c.Has(BlockKey{FSID: 1}) {
		t.Fatal("oversize block should not be cached")
	}
}

// 目的：磁盘块文件缺失时 Has=false；按 fsID 失效不影响其他文件/账户，Size 同步。
func TestDiskCache_HasAndInvalidateFSID(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 0)
	defer dc.Close()

	k1 := BlockKeyFromPath("acct", 1, 100, 1, 0)
	k2 := BlockKeyFromPath("acct", 1, 100, 1, 1024)
	other := BlockKeyFromPath("acct", 2, 100, 1, 0)
	dc.Put(k1, []byte("aaaa"))
	dc.Put(k2, []byte("bbbb"))
	dc.Put(other, []byte("cccc"))

	if !dc.Has(k1) {
		t.Fatal("Has(k1) = false")
	}
	if dc.Has(BlockKeyFromPath("acct", 9, 1, 1, 0)) {
		t.Fatal("Has(missing) = true")
	}

	// 文件被删除后 Has 应为 false。
	meta := dc.manifest[k1.String()]
	if meta == nil {
		t.Fatal("manifest missing k1")
	}
	if err := os.Remove(dc.blockPath(meta)); err != nil {
		t.Fatal(err)
	}
	if dc.Has(k1) {
		t.Fatal("Has should be false after file removal")
	}

	dc.InvalidateFSID("acct", 1)
	if dc.Has(k2) {
		t.Fatal("k2 should be invalidated")
	}
	if !dc.Has(other) {
		t.Fatal("other fsid must remain")
	}
	if dc.Size() != 4 {
		t.Fatalf("Size=%d, want 4", dc.Size())
	}

	// 不存在 fsID 的失效是 no-op。
	dc.InvalidateFSID("acct", 99)
}

// 目的：空块与超限块不落盘，保持容量约束。
func TestDiskCache_PutSkipsEmptyAndOversize(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 2)
	defer dc.Close()
	dc.Put(BlockKey{FSID: 1}, nil)
	dc.Put(BlockKey{FSID: 1}, []byte("123"))
	if dc.Size() != 0 {
		t.Fatalf("Size=%d, want 0", dc.Size())
	}
}

// 目的：同长度但内容损坏的块必须由 SHA-256 检出并清除 manifest，不能当命中。
func TestDiskCache_GetDetectsChecksumMismatch(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 0)
	defer dc.Close()
	k := BlockKeyFromPath("a", 1, 10, 1, 0)
	dc.Put(k, []byte("abcd"))

	meta := dc.manifest[k.String()]
	if meta == nil {
		t.Fatal("missing manifest entry")
	}
	// 同长度、不同内容：必须由 SHA-256 检出，不能当命中返回。
	if err := os.WriteFile(dc.blockPath(meta), []byte("wxyz"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok := dc.Get(k); ok {
		t.Fatal("corrupted block should be a miss")
	}
	if _, ok := dc.manifest[k.String()]; ok {
		t.Fatal("corrupted block manifest entry should be removed")
	}
}

// 目的：命中刷新近似 LRU 时间，但 60s 内不重复置 dirty，避免刷盘风暴。
func TestDiskCache_GetRefreshesAccessedAtThrottled(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 0)
	defer dc.Close()
	k := BlockKeyFromPath("a", 1, 10, 1, 0)
	dc.Put(k, []byte("abcd"))

	meta := dc.manifest[k.String()]
	old := time.Now().Add(-2 * time.Minute)
	meta.AccessedAt = old
	// 第一次命中应刷新并置 dirty。
	if _, ok := dc.Get(k); !ok {
		t.Fatal("expected hit")
	}
	if !dc.manifest[k.String()].AccessedAt.After(old) {
		t.Fatal("AccessedAt not refreshed")
	}
	// 紧接着第二次命中不应重复置 dirty（近似 LRU 节流）。
	dc.dirty = false
	if _, ok := dc.Get(k); !ok {
		t.Fatal("expected hit")
	}
	if dc.dirty {
		t.Fatal("dirty should stay false within 60s throttle window")
	}
}

// 目的：manifest 损坏时安全重建，并清理孤儿块和临时文件。
func TestDiskCache_ManifestCorruptCleansOrphans(t *testing.T) {
	cacheDir := t.TempDir()
	blocks := filepath.Join(cacheDir, "blocks")
	if err := os.MkdirAll(filepath.Join(blocks, "ab"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocks, "manifest.json"), []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(blocks, "ab", "orphan.block")
	if err := os.WriteFile(orphan, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(blocks, ".tmp-orphan")
	if err := os.WriteFile(tmp, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	dc := NewDiskCache(cacheDir, 0)
	defer dc.Close()

	if len(dc.manifest) != 0 || dc.Size() != 0 {
		t.Fatalf("manifest=%d size=%d, want empty", len(dc.manifest), dc.Size())
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan not removed: %v", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("tmp orphan not removed: %v", err)
	}
}

// 目的：manifest 缺字段/坏条目不能 panic，也不能保留无效块。
func TestDiskCache_ManifestNilAndBadEntries(t *testing.T) {
	cacheDir := t.TempDir()
	blocks := filepath.Join(cacheDir, "blocks")
	if err := os.MkdirAll(blocks, 0700); err != nil {
		t.Fatal(err)
	}
	// Blocks 为 nil + 一条 size<=0 的坏记录。
	raw, _ := json.Marshal(map[string]interface{}{
		"v": 2,
		"blocks": map[string]interface{}{
			"bad": map[string]interface{}{"path": "a:1:1:1", "file": "x.block", "size": 0},
		},
	})
	if err := os.WriteFile(filepath.Join(blocks, "manifest.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	dc := NewDiskCache(cacheDir, 0)
	defer dc.Close()
	if len(dc.manifest) != 0 {
		t.Fatalf("bad entry kept: %+v", dc.manifest)
	}

	// 没有 blocks 字段也不能 panic。
	if err := os.WriteFile(filepath.Join(blocks, "manifest.json"), []byte(`{"v":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	dc2 := NewDiskCache(cacheDir, 0)
	defer dc2.Close()
	if len(dc2.manifest) != 0 {
		t.Fatalf("manifest=%d, want 0", len(dc2.manifest))
	}
}

// 目的：flush 后重启必须能恢复块数据与容量统计。
func TestDiskCache_FlushAndReload(t *testing.T) {
	cacheDir := t.TempDir()
	dc := NewDiskCache(cacheDir, 0)
	k := BlockKeyFromPath("acct", 1, 10, 2, 0)
	dc.Put(k, []byte("payload"))
	dc.Flush()
	dc.Close()

	if _, err := os.Stat(filepath.Join(cacheDir, "blocks", "manifest.json")); err != nil {
		t.Fatalf("manifest not written: %v", err)
	}

	dc2 := NewDiskCache(cacheDir, 0)
	defer dc2.Close()
	data, ok := dc2.Get(k)
	if !ok || string(data) != "payload" {
		t.Fatalf("reloaded Get=(%q,%v), want payload,true", data, ok)
	}
	if dc2.Size() != 7 {
		t.Fatalf("Size=%d, want 7", dc2.Size())
	}
}

// 目的：manifest 刷盘失败时保留 dirty，避免丢失元数据。
func TestDiskCache_FlushFailureKeepsDirty(t *testing.T) {
	cacheDir := t.TempDir()
	dc := NewDiskCache(cacheDir, 0)
	defer dc.Close()

	// 让 manifest.json 变成目录，rename 必然失败。
	manifestPath := filepath.Join(cacheDir, "blocks", "manifest.json")
	if err := os.Mkdir(manifestPath, 0700); err != nil {
		t.Fatal(err)
	}
	dc.Put(BlockKeyFromPath("a", 1, 1, 1, 0), []byte("x"))
	dc.Flush()
	if !dc.dirty {
		t.Fatal("dirty should remain true after failed flush")
	}
}

// 目的：恶意 File 字段不能逃出缓存目录，防止路径穿越写。
func TestDiskCache_BlockPathRejectsTraversal(t *testing.T) {
	dc := NewDiskCache(t.TempDir(), 0)
	defer dc.Close()
	meta := &DiskBlockMeta{File: "../escape.block", Path: "a:1:1:1", Offset: 0, Size: 1}
	got := dc.blockPath(meta)
	rel, err := filepath.Rel(dc.baseDir, got)
	if err != nil || rel == ".." || filepath.IsAbs(rel) {
		t.Fatalf("blockPath escaped baseDir: %q", got)
	}
	if filepath.Dir(got) == filepath.Dir(filepath.Join(dc.baseDir, "..")) {
		t.Fatalf("unexpected path %q", got)
	}
}

// 目的：负缓存 TTL 生效、Set 清除负缓存、TTL<=0 时禁用。
func TestMetadataCache_NegativeLifecycle(t *testing.T) {
	c := NewMetadataCache("acct", time.Minute, time.Minute, 20*time.Millisecond)
	if c.GetNegative("/missing") {
		t.Fatal("unexpected negative hit")
	}
	c.SetNegative("/missing")
	if !c.GetNegative("/missing") {
		t.Fatal("expected negative hit")
	}
	time.Sleep(25 * time.Millisecond)
	if c.GetNegative("/missing") {
		t.Fatal("negative entry should expire")
	}

	// Set 会清除负缓存。
	c.SetNegative("/file")
	c.Set("/file", &mount.RemoteEntry{FSID: 1})
	if c.GetNegative("/file") {
		t.Fatal("Set should clear negative cache")
	}

	// negTTL<=0 禁用负缓存。
	disabled := NewMetadataCache("acct", time.Minute, time.Minute, 0)
	disabled.SetNegative("/x")
	if disabled.GetNegative("/x") {
		t.Fatal("negative cache should be disabled")
	}
}

// 目的：负缓存必须有界，扫描大量不存在路径时不能无限增长。
func TestMetadataCache_NegativeCapTrim(t *testing.T) {
	c := NewMetadataCache("acct", time.Minute, time.Minute, time.Minute)
	c.maxNegative = 3
	for i := 0; i < 20; i++ {
		c.SetNegative("/missing-" + string(rune('a'+i)))
	}
	if len(c.negative) > c.maxNegative {
		t.Fatalf("negative size=%d, want <= %d", len(c.negative), c.maxNegative)
	}
}

// 目的：缓存返回副本，调用方修改不得污染缓存内容。
func TestMetadataCache_DirCopyIsolation(t *testing.T) {
	c := NewMetadataCache("acct", time.Minute, time.Minute, time.Minute)
	entry := &mount.RemoteEntry{FSID: 1, Name: "a"}
	c.SetDir("/dir", []*mount.RemoteEntry{entry})

	entry.FSID = 99 // 修改原始对象不应影响缓存副本
	got, ok := c.GetDir("/dir")
	if !ok || got[0].FSID != 1 {
		t.Fatalf("cached dir not isolated: %+v", got)
	}
	got[0].FSID = 77 // 修改返回值也不应影响缓存
	again, _ := c.GetDir("/dir")
	if again[0].FSID != 1 {
		t.Fatalf("GetDir returned shared pointer: %+v", again)
	}

	// 目录列表出现条目时清除负缓存。
	c.SetNegative("/dir/a")
	c.SetDir("/dir", []*mount.RemoteEntry{{FSID: 1, Name: "a"}})
	if c.GetNegative("/dir/a") {
		t.Fatal("SetDir should clear negative entry for child")
	}
}

// 目的：按路径前缀精确失效文件/目录/负缓存，不误伤相似前缀。
func TestMetadataCache_InvalidatePrefix(t *testing.T) {
	c := NewMetadataCache("acct", time.Minute, time.Minute, time.Minute)
	c.Set("/dir", &mount.RemoteEntry{FSID: 1})
	c.Set("/dir/a", &mount.RemoteEntry{FSID: 2})
	c.Set("/dir2", &mount.RemoteEntry{FSID: 3})
	c.SetDir("/dir", []*mount.RemoteEntry{{FSID: 1}})
	c.SetDir("/dir/sub", []*mount.RemoteEntry{{FSID: 2}})
	c.SetNegative("/dir/missing")

	c.InvalidatePrefix("/dir")
	if _, ok := c.Get("/dir"); ok {
		t.Fatal("/dir should be invalidated")
	}
	if _, ok := c.Get("/dir/a"); ok {
		t.Fatal("/dir/a should be invalidated")
	}
	if _, ok := c.GetDir("/dir"); ok {
		t.Fatal("/dir dir entry should be invalidated")
	}
	if _, ok := c.GetDir("/dir/sub"); ok {
		t.Fatal("/dir/sub dir entry should be invalidated")
	}
	if c.GetNegative("/dir/missing") {
		t.Fatal("negative entry should be invalidated")
	}
	// 前缀相似但不同目录必须保留。
	if _, ok := c.Get("/dir2"); !ok {
		t.Fatal("/dir2 must remain")
	}
}

// 目的：ListDirs 暴露可刷新目录，NeedsRefresh/SetDirRefreshed 控制刷新窗口。
func TestMetadataCache_ListDirsAndRefresh(t *testing.T) {
	c := NewMetadataCache("acct", time.Minute, time.Minute, time.Minute)
	c.SetDir("/a", nil)
	c.SetDir("/b/c", nil)

	dirs := c.ListDirs()
	if len(dirs) != 2 {
		t.Fatalf("ListDirs=%d, want 2", len(dirs))
	}
	paths := map[string]bool{}
	for _, d := range dirs {
		paths[d.Path] = true
		if d.Key == "" {
			t.Fatal("empty key")
		}
	}
	if !paths["/a"] || !paths["/b/c"] {
		t.Fatalf("unexpected paths: %+v", paths)
	}

	if c.NeedsRefresh("/a", 0) {
		t.Fatal("interval<=0 should not need refresh")
	}
	if c.NeedsRefresh("/missing", time.Second) {
		t.Fatal("unknown dir should not need refresh")
	}
	if c.NeedsRefresh("/a", time.Hour) {
		t.Fatal("fresh dir should not need refresh")
	}
	time.Sleep(10 * time.Millisecond)
	if !c.NeedsRefresh("/a", time.Millisecond) {
		t.Fatal("stale dir should need refresh")
	}
	c.SetDirRefreshed("/a")
	if c.NeedsRefresh("/a", time.Hour) {
		t.Fatal("SetDirRefreshed did not update timestamp")
	}
	// 不存在的目录调用 SetDirRefreshed 是 no-op。
	c.SetDirRefreshed("/missing")
}

// 目的：文件条目按 LRU 淘汰，最近写入的条目保留。
func TestMetadataCache_FileLRUEviction(t *testing.T) {
	c := NewMetadataCache("acct", time.Minute, time.Minute, time.Minute)
	c.maxFiles = 2
	c.Set("/a", &mount.RemoteEntry{FSID: 1})
	c.Set("/b", &mount.RemoteEntry{FSID: 2})
	c.Set("/c", &mount.RemoteEntry{FSID: 3})
	if len(c.entries) > 2 {
		t.Fatalf("entries=%d, want <=2", len(c.entries))
	}
	// 最近写入的必须保留。
	if _, ok := c.Get("/c"); !ok {
		t.Fatal("/c should survive")
	}
}

// 目的：目录条目按 LRU 淘汰，最近写入的条目保留。
func TestMetadataCache_DirLRUEviction(t *testing.T) {
	c := NewMetadataCache("acct", time.Minute, time.Minute, time.Minute)
	c.maxDirs = 2
	for _, p := range []string{"/a", "/b", "/c"} {
		c.SetDir(p, nil)
	}
	if len(c.dirs) > 2 {
		t.Fatalf("dirs=%d, want <=2", len(c.dirs))
	}
	if _, ok := c.GetDir("/c"); !ok {
		t.Fatal("/c should survive")
	}
}

// 目的：验证 D12 顺序读判定：连续/回退容忍/跳跃/跨 inode 重启的边界。
func TestStreak_SequentialStateMachine(t *testing.T) {
	s := NewStreak()
	if s.SequentialCount() != 0 || s.IsSequential(1, 0) {
		t.Fatal("empty streak should not be sequential")
	}
	s.Advance(1, 10)
	if s.SequentialCount() != 1 {
		t.Fatalf("count=%d", s.SequentialCount())
	}
	if !s.IsSequential(1, 10) {
		t.Fatal("exact continuation should be sequential")
	}
	if !s.IsSequential(1, 10-SEQ_BACKWARD) || !s.IsSequential(1, 10+SEQ_TOLERANCE) {
		t.Fatal("tolerance boundaries should be sequential")
	}
	if s.IsSequential(1, 11+SEQ_TOLERANCE) || s.IsSequential(1, 9-SEQ_BACKWARD) {
		t.Fatal("out-of-window offsets should break sequence")
	}
	if s.IsSequential(2, 10) {
		t.Fatal("inode switch should break sequence")
	}
	s.Advance(2, 20)
	if s.SequentialCount() != 1 || s.lastIno != 2 {
		t.Fatalf("inode switch should reset: count=%d ino=%d", s.SequentialCount(), s.lastIno)
	}
	s.Advance(2, 30)
	s.Advance(2, 40)
	if s.SequentialCount() != 3 {
		t.Fatalf("count=%d, want 3", s.SequentialCount())
	}
}
