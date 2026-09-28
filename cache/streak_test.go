package cache

import "testing"

func TestStreak_FirstRead_NotSequential(t *testing.T) {
	s := NewStreak()
	if s.IsSequential(1, 0) {
		t.Error("first read should not be sequential")
	}
}

func TestStreak_WithinWindow_Sequential(t *testing.T) {
	s := NewStreak()
	s.Advance(1, 1000) // end=1000
	// 精确续读: off=1000 ∈ [1000-64KB, 1000+256KB]
	if !s.IsSequential(1, 1000) {
		t.Error("exact continuation should be sequential")
	}
}

func TestStreak_BackwardWithinWindow(t *testing.T) {
	s := NewStreak()
	s.Advance(1, 1000)
	// 回退 100: off=900 ∈ [1000-64KB, 1000+256KB]
	if !s.IsSequential(1, 900) {
		t.Error("backward within 64KB should be sequential")
	}
}

func TestStreak_ForwardExceedsWindow(t *testing.T) {
	s := NewStreak()
	s.Advance(1, 1000)
	// 前进超出 256KB: off=300000 ∉ [1000-64KB, 1000+256KB≈263168]
	if s.IsSequential(1, 300000) {
		t.Error("forward exceeding 256KB should not be sequential")
	}
}

func TestStreak_DifferentIno_Reset(t *testing.T) {
	s := NewStreak()
	s.Advance(1, 100)
	if s.IsSequential(2, 100) {
		t.Error("different ino should not be sequential")
	}
}

func TestStreak_AdvanceMultipleFiles(t *testing.T) {
	s := NewStreak()
	// 连续读文件 A
	s.Advance(1, 100)
	s.Advance(1, 200)
	if !s.IsSequential(1, 200) {
		t.Error("same file sequential reads should be sequential")
	}
	// 切换到文件 B → 重置
	s.Advance(2, 300)
	if s.IsSequential(1, 300) {
		t.Error("different file should reset streak")
	}
	if !s.IsSequential(2, 300) {
		t.Error("new file first read should be tracked")
	}
}
