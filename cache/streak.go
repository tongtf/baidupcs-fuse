package cache

import "sync"

const (
	// SEQ_TOLERANCE 顺序读前进容忍窗口（256KB）。
	SEQ_TOLERANCE = 256 * 1024
	// SEQ_BACKWARD 顺序读回退容忍窗口（64KB）。
	SEQ_BACKWARD = 64 * 1024
)

// Streak per-inode 顺序读状态机。
// 判定连续：off ∈ [lastEnd - 64KB, lastEnd + 256KB]（D12 语义对齐）。
type Streak struct {
	mu      sync.Mutex
	lastIno uint64 // 上一次读的 ino
	lastEnd int64  // 上一次读的结束位置（off+size）
	count   int    // 连续计数
}

func NewStreak() *Streak {
	return &Streak{}
}

// IsSequential 判定 (ino, off) 是否与上一次读连续。
// off = 下一次读取的起始位置（D12）。
func (s *Streak) IsSequential(ino uint64, off int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.count == 0 {
		return false
	}
	if ino != s.lastIno {
		return false
	}
	// off ∈ [lastEnd - 64KB, lastEnd + 256KB]
	low := s.lastEnd - SEQ_BACKWARD
	high := s.lastEnd + SEQ_TOLERANCE
	return off >= low && off <= high
}

// Advance 更新 streak。end = off+size（本次读取的结束位置，D12）。
func (s *Streak) Advance(ino uint64, end int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.count > 0 && ino == s.lastIno {
		s.count++
	} else {
		s.count = 1
	}
	s.lastIno = ino
	s.lastEnd = end
}

// SequentialCount 返回当前连续读计数（用于预读决策）。
func (s *Streak) SequentialCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}
