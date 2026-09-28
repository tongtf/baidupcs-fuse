package mount

import (
	"sync"
	"testing"
	"time"
)

func TestInodeManager_GetOrCreate(t *testing.T) {
	m := NewInodeManager(30 * time.Second)
	entry := &RemoteEntry{FSID: 1, Path: "/a.txt", Name: "a.txt", Size: 100}
	ino1 := m.GetOrCreate("/a.txt", entry, 0)
	ino2 := m.GetOrCreate("/a.txt", entry, 0)
	if ino1 != ino2 {
		t.Error("expected same inode pointer")
	}
	if ino1.NodeID != 1 {
		t.Errorf("NodeID = %d, want 1", ino1.NodeID)
	}
}

func TestInodeManager_TTL_Expires(t *testing.T) {
	m := NewInodeManager(10 * time.Millisecond)
	entry := &RemoteEntry{FSID: 1, Path: "/a.txt", Name: "a.txt"}
	m.GetOrCreate("/a.txt", entry, 0)
	time.Sleep(15 * time.Millisecond)
	_, ok := m.GetByPath("/a.txt")
	if ok {
		t.Error("expected expired")
	}
}

func TestInodeManager_Invalidate(t *testing.T) {
	m := NewInodeManager(30 * time.Second)
	entry := &RemoteEntry{FSID: 1, Path: "/a.txt", Name: "a.txt"}
	m.GetOrCreate("/a.txt", entry, 0)
	m.Invalidate("/a.txt")
	_, ok := m.GetByPath("/a.txt")
	if ok {
		t.Error("expected invalidated")
	}
}

func TestInodeManager_InvalidateDir(t *testing.T) {
	m := NewInodeManager(30 * time.Second)
	m.GetOrCreate("/a/b", &RemoteEntry{Path: "/a/b", Name: "b"}, 0)
	m.GetOrCreate("/a/c", &RemoteEntry{Path: "/a/c", Name: "c"}, 0)
	m.InvalidateDir("/a")
	_, ok1 := m.GetByPath("/a/b")
	_, ok2 := m.GetByPath("/a/c")
	if ok1 || ok2 {
		t.Error("expected both invalidated")
	}
}

func TestInodeManager_ByNodeID_Persists(t *testing.T) {
	m := NewInodeManager(10 * time.Millisecond)
	entry := &RemoteEntry{FSID: 1, Path: "/a.txt", Name: "a.txt"}
	ino := m.GetOrCreate("/a.txt", entry, 0)
	time.Sleep(15 * time.Millisecond)
	// byPath 过期
	_, okPath := m.GetByPath("/a.txt")
	if okPath {
		t.Error("byPath should expire")
	}
	// byNodeID 不过期
	got, okID := m.GetByNodeID(ino.NodeID)
	if !okID || got != ino {
		t.Error("byNodeID should persist")
	}
}

func TestInodeManager_NodeID_Increment(t *testing.T) {
	m := NewInodeManager(30 * time.Second)
	ino1 := m.GetOrCreate("/a", &RemoteEntry{Path: "/a", Name: "a"}, 0)
	ino2 := m.GetOrCreate("/b", &RemoteEntry{Path: "/b", Name: "b"}, 0)
	ino3 := m.GetOrCreate("/c", &RemoteEntry{Path: "/c", Name: "c"}, 0)
	if ino1.NodeID != 1 || ino2.NodeID != 2 || ino3.NodeID != 3 {
		t.Errorf("NodeIDs = %d,%d,%d, want 1,2,3", ino1.NodeID, ino2.NodeID, ino3.NodeID)
	}
}

func TestInodeManager_Concurrent(t *testing.T) {
	m := NewInodeManager(30 * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				path := "/dir" + string(rune('A'+i)) + "/file" + string(rune('0'+j%10))
				entry := &RemoteEntry{Path: path, Name: path}
				m.GetOrCreate(path, entry, 0)
				m.GetByPath(path)
			}
		}(i)
	}
	wg.Wait()
}
