package adapter

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"baidupcs-fuse/mount"
)

// StagingManager 本地暂存管理器（PLAN-006: adapter/ 包，非 mount/）。
// Phase 1: sessions map 加 mutex（v2 并发安全）。
type StagingManager struct {
	mu          sync.RWMutex
	cacheDir    string // ~/.cache/baidupcs-fuse/uploads/<sha256(path)>/
	sessions    map[string]*writeSession
	enableWrite bool
}

type writeSession struct {
	path    string
	tmpFile *os.File
	tmpPath string
	mu      sync.Mutex // 保护 dirty/closed
	closed  bool
	dirty   bool
}

// NewStagingManager 创建暂存管理器。
func NewStagingManager(cacheDir string, enableWrite bool) *StagingManager {
	return &StagingManager{
		cacheDir:    cacheDir,
		sessions:    make(map[string]*writeSession),
		enableWrite: enableWrite,
	}
}

// Open 为 path 创建写 session。
// 同路径已有活跃 session → 返回 ErrStagingBusy（v1 不支持并发写同一文件）。
// CacheDir 返回 staging 文件根目录（供失败上传记录等外部使用）。
func (m *StagingManager) CacheDir() string { return m.cacheDir }

func (m *StagingManager) Open(path string) (*writeSession, error) {
	if !m.enableWrite {
		return nil, mount.ErrWriteNotEnabled
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[path]; ok {
		return nil, mount.ErrStagingBusy
	}

	// 创建 staging 目录
	dir := m.stagingDir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir staging: %w", err)
	}

	tmpPath := filepath.Join(dir, "data.tmp")
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("create staging file: %w", err)
	}

	s := &writeSession{
		path:    path,
		tmpFile: tmpFile,
		tmpPath: tmpPath,
	}
	m.sessions[path] = s
	return s, nil
}

// Write 写入数据到 session 的 staging 文件。
func (s *writeSession) Write(off int64, data []byte) error {
	n, err := s.tmpFile.WriteAt(data, off)
	if err != nil {
		return fmt.Errorf("staging write at %d: %w", off, err)
	}
	if n != len(data) {
		return fmt.Errorf("staging short write: %d != %d", n, len(data))
	}
	s.mu.Lock()
	s.dirty = true
	s.mu.Unlock()
	return nil
}

// Size 返回 staging 文件当前大小。
func (s *writeSession) Size() int64 {
	info, err := s.tmpFile.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// Close 关闭 session，返回 staging 文件路径。
func (s *writeSession) Close() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", fmt.Errorf("session already closed")
	}
	s.closed = true
	if err := s.tmpFile.Close(); err != nil {
		return "", fmt.Errorf("close staging file: %w", err)
	}
	return s.tmpPath, nil
}

// Remove 清理 session 的 staging 文件和目录。
func (s *writeSession) Remove() error {
	s.mu.Lock()
	if s.tmpFile != nil && !s.closed {
		s.tmpFile.Close()
		s.closed = true
	}
	s.mu.Unlock()
	if err := os.Remove(s.tmpPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	// 尝试清理父目录（空则删，不空则忽略）
	dir := filepath.Dir(s.tmpPath)
	os.Remove(dir)
	return nil
}

// CloseAndRemove 关闭 session 并清理 staging 文件。
func (m *StagingManager) CloseAndRemove(path string) {
	m.mu.Lock()
	s, ok := m.sessions[path]
	if ok {
		delete(m.sessions, path)
	}
	m.mu.Unlock()
	if ok {
		s.Remove()
	}
}

// ClearSession 只清 session map，不删 staging 文件（上传失败保留场景）。
func (m *StagingManager) ClearSession(path string) {
	m.mu.Lock()
	delete(m.sessions, path)
	m.mu.Unlock()
}

// ReadFrom 从 staging 文件读取 [off, off+size)。
func (s *writeSession) ReadFrom(off int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	n, err := s.tmpFile.ReadAt(buf, off)
	if n == 0 && err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// Truncate 将 staging 文件截断到指定大小。
func (s *writeSession) Truncate(size int64) error {
	return s.tmpFile.Truncate(size)
}

// Reset 重置 staging 文件（truncate 到 0），用于覆写/O_TRUNC。
func (s *writeSession) Reset() error {
	s.mu.Lock()
	s.dirty = false
	s.mu.Unlock()
	return s.tmpFile.Truncate(0)
}

// Dirty reports whether the session has uncommitted writes.
func (s *writeSession) Dirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirty
}

// ClearDirty resets the dirty flag after successful upload.
func (s *writeSession) ClearDirty() {
	s.mu.Lock()
	s.dirty = false
	s.mu.Unlock()
}

// IsDirty reports whether path has uncommitted writes.
func (m *StagingManager) IsDirty(path string) bool {
	m.mu.RLock()
	s, ok := m.sessions[path]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	return s.dirty
}

// stagingDir 返回 staging 文件的目录：CacheDir/uploads/<sha256(path)>/
func (m *StagingManager) stagingDir(path string) string {
	h := sha256.Sum256([]byte(path))
	return filepath.Join(m.cacheDir, "uploads", fmt.Sprintf("%x", h))
}

// HasSession 检查 path 是否有活跃写 session。
func (m *StagingManager) HasSession(path string) bool {
	m.mu.RLock()
	_, ok := m.sessions[path]
	m.mu.RUnlock()
	return ok
}

// TmpPath 获取 path 的 staging 文件路径。
func (m *StagingManager) TmpPath(path string) (string, bool) {
	m.mu.RLock()
	s, ok := m.sessions[path]
	m.mu.RUnlock()
	if !ok {
		return "", false
	}
	return s.tmpPath, true
}
