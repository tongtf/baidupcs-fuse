package mount

import (
	"sync"
	"sync/atomic"
	"time"
)

// Inode 内存中的 inode 表示。
type Inode struct {
	NodeID   uint64
	Path     string
	Name     string
	IsDir    bool
	Size     int64
	MtimeSec int64
	Mode     uint32
	Parent   uint64
	expireAt time.Time
}

// InodeManager 路径 ↔ inode 映射，TTL 过期。
type InodeManager struct {
	mu       sync.RWMutex
	byPath   map[string]*Inode
	byNodeID map[uint64]*Inode
	nextID   atomic.Uint64
	ttl      time.Duration
}

func NewInodeManager(ttl time.Duration) *InodeManager {
	return &InodeManager{
		byPath:   make(map[string]*Inode),
		byNodeID: make(map[uint64]*Inode),
		ttl:      ttl,
		nextID:   atomic.Uint64{},
	}
}

func (m *InodeManager) GetByPath(path string) (*Inode, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ino, ok := m.byPath[path]
	if !ok || time.Now().After(ino.expireAt) {
		return nil, false
	}
	return ino, true
}

func (m *InodeManager) GetByNodeID(id uint64) (*Inode, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ino, ok := m.byNodeID[id]
	return ino, ok
}

func (m *InodeManager) GetOrCreate(path string, entry *RemoteEntry, parent uint64) *Inode {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ino, ok := m.byPath[path]; ok && time.Now().Before(ino.expireAt) {
		return ino
	}

	id := m.nextID.Add(1) // root=1，FUSE 规范
	ino := &Inode{
		NodeID:   id,
		Path:     path,
		Name:     entry.Name,
		IsDir:    entry.IsDir,
		Size:     entry.Size,
		MtimeSec: entry.MtimeSec,
		Mode:     entry.Mode,
		Parent:   parent,
		expireAt: time.Now().Add(m.ttl),
	}
	m.byPath[path] = ino
	m.byNodeID[id] = ino
	return ino
}

func (m *InodeManager) Invalidate(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.byPath, path)
}

func (m *InodeManager) InvalidateDir(dirPath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for path := range m.byPath {
		if len(path) > len(dirPath) && path[:len(dirPath)+1] == dirPath+"/" {
			delete(m.byPath, path)
		}
	}
}
