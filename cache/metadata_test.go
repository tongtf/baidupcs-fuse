package cache

import (
	"baidupcs-fuse/mount"
	"sync"
	"testing"
	"time"
)

func TestMetadataCache_GetSet(t *testing.T) {
	c := NewMetadataCache("test", 30*time.Second, 60*time.Second, 5*time.Second)
	entry := &mount.RemoteEntry{FSID: 1, Path: "/a.txt", Size: 100}
	c.Set("/a.txt", entry)
	got, ok := c.Get("/a.txt")
	if !ok {
		t.Fatal("expected hit")
	}
	if got.FSID != 1 || got.Size != 100 {
		t.Errorf("entry = %+v, want FSID=1 Size=100", got)
	}
}

func TestMetadataCache_Miss(t *testing.T) {
	c := NewMetadataCache("test", 30*time.Second, 60*time.Second, 5*time.Second)
	_, ok := c.Get("/nope")
	if ok {
		t.Error("expected miss")
	}
}

func TestMetadataCache_FileTTL_Expires(t *testing.T) {
	c := NewMetadataCache("test", 10*time.Millisecond, 60*time.Second, 5*time.Second)
	c.Set("/a.txt", &mount.RemoteEntry{FSID: 1})
	time.Sleep(15 * time.Millisecond)
	_, ok := c.Get("/a.txt")
	if ok {
		t.Error("expected expired")
	}
}

func TestMetadataCache_DirTTL_Expires(t *testing.T) {
	c := NewMetadataCache("test", 30*time.Second, 10*time.Millisecond, 5*time.Second)
	c.SetDir("/docs", []*mount.RemoteEntry{{FSID: 1}})
	time.Sleep(15 * time.Millisecond)
	_, ok := c.GetDir("/docs")
	if ok {
		t.Error("expected expired")
	}
}

func TestMetadataCache_NegativeTTL(t *testing.T) {
	c := NewMetadataCache("test", 30*time.Second, 60*time.Second, 50*time.Millisecond)
	// 第一次 miss 不写负缓存（由调用方决定），连续 miss 都返回 false
	_, ok1 := c.Get("/never")
	_, ok2 := c.Get("/never")
	if ok1 || ok2 {
		t.Error("expected both misses")
	}
}

func TestMetadataCache_Invalidate(t *testing.T) {
	c := NewMetadataCache("test", 30*time.Second, 60*time.Second, 5*time.Second)
	c.Set("/a.txt", &mount.RemoteEntry{FSID: 1})
	c.Invalidate("/a.txt")
	_, ok := c.Get("/a.txt")
	if ok {
		t.Error("expected invalidation")
	}
}

func TestMetadataCache_InvalidateDir(t *testing.T) {
	c := NewMetadataCache("test", 30*time.Second, 60*time.Second, 5*time.Second)
	c.SetDir("/docs", []*mount.RemoteEntry{{FSID: 1}})
	c.InvalidateDir("/docs")
	_, ok := c.GetDir("/docs")
	if ok {
		t.Error("expected invalidation")
	}
}

func TestMetadataCache_InvalidateParent(t *testing.T) {
	c := NewMetadataCache("test", 30*time.Second, 60*time.Second, 5*time.Second)
	c.SetDir("/a/b", []*mount.RemoteEntry{{FSID: 1}})
	c.InvalidateParent("/a/b/c") // parent = /a/b
	_, ok := c.GetDir("/a/b")
	if ok {
		t.Error("expected parent invalidation")
	}
}

func TestMetadataCache_Concurrent(t *testing.T) {
	c := NewMetadataCache("test", 30*time.Second, 60*time.Second, 5*time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				path := "/file" + string(rune('A'+i)) + string(rune('0'+j%10))
				c.Set(path, &mount.RemoteEntry{FSID: int64(i*100 + j)})
				c.Get(path)
			}
		}(i)
	}
	wg.Wait()
}
