package download

import (
	"sync"
	"testing"
	"time"
)

// TestStuckFetchesAndActiveCount 验证活跃 fetch 计数与卡死检测。
func TestStuckFetchesAndActiveCount(t *testing.T) {
	s := NewScheduler(2, time.Second, time.Second)

	if got := s.ActiveFetchCount(); got != 0 {
		t.Fatalf("初始 ActiveFetchCount = %d，期望 0", got)
	}

	// 注册一个"旧" fetch（人工把 Started 设到 40s 前，超过 30s 阈值）
	oldID := "old-request-id"
	s.RegisterFetch(oldID, 111, 0, 33554432)
	s.fetchStats[oldID].Started = time.Now().Add(-40 * time.Second)

	// 注册一个新 fetch
	newID := "new-request-id"
	s.RegisterFetch(newID, 222, 1006632960, 33554432)

	if got := s.ActiveFetchCount(); got != 2 {
		t.Fatalf("ActiveFetchCount = %d，期望 2", got)
	}

	stucks := s.StuckFetches(30 * time.Second)
	if len(stucks) != 1 {
		t.Fatalf("StuckFetches = %d 个，期望 1（仅超过阈值的旧 fetch）", len(stucks))
	}
	if stuck := stucks[0]; stuck.FsID != 111 || stuck.Off != 0 || stuck.Length != 33554432 {
		t.Fatalf("卡死 fetch 元信息错误: %+v", stuck)
	}

	// 注销后不再计入活跃计数
	s.UnregisterFetch(oldID)
	if got := s.ActiveFetchCount(); got != 1 {
		t.Fatalf("UnregisterFetch 后 ActiveFetchCount = %d，期望 1", got)
	}
	stucks = s.StuckFetches(30 * time.Second)
	if len(stucks) != 0 {
		t.Fatalf("注销后 StuckFetches = %d，期望 0", len(stucks))
	}

	// RegisterFetch 幂等：重复注册同一 requestID 不增加计数
	s.RegisterFetch(newID, 222, 1006632960, 33554432)
	if got := s.ActiveFetchCount(); got != 1 {
		t.Fatalf("幂等注册后 ActiveFetchCount = %d，期望仍为 1", got)
	}
	s.UnregisterFetch(newID)
}

// TestStuckThresholdBoundary 验证卡死检测的阈值边界（恰好在阈值内不算卡死）。
func TestStuckThresholdBoundary(t *testing.T) {
	s := NewScheduler(1, time.Second, time.Second)

	now := time.Now()
	s.RegisterFetch("within", 1, 0, 1000)
	s.fetchStats["within"].Started = now.Add(-25 * time.Second) // 25s < 30s

	s.RegisterFetch("over", 2, 0, 1000)
	s.fetchStats["over"].Started = now.Add(-45 * time.Second)   // 45s > 30s

	stucks := s.StuckFetches(30 * time.Second)
	if len(stucks) != 1 || stucks[0].FsID != 2 {
		t.Fatalf("阈值边界测试失败: %+v", stucks)
	}
}

// TestActiveFetchCountConcurrency 验证并发注册/注销下的计数正确性。
func TestActiveFetchCountConcurrency(t *testing.T) {
	s := NewScheduler(4, time.Second, time.Second)

	const monitorTestN = 50
	var wg sync.WaitGroup
	for i := 0; i < monitorTestN; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			reqID := "req-" + string(rune('a'+id))
			s.RegisterFetch(reqID, int64(id), 0, 1000)
			time.Sleep(time.Millisecond)
			s.UnregisterFetch(reqID)
		}(i)
	}
	wg.Wait()

	if got := s.ActiveFetchCount(); got != 0 {
		t.Fatalf("并发结束后 ActiveFetchCount = %d，期望 0", got)
	}
}
