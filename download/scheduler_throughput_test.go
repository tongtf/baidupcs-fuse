package download

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tokenBucket 简易令牌桶：限制服务端整体写入速率（字节/秒），模拟百度 CDN 限速。
type tokenBucket struct {
	mu      sync.Mutex
	balance int64
	rate    int64 // bytes/sec
	last    time.Time
}

func newTokenBucket(bytesPerSec float64) *tokenBucket {
	return &tokenBucket{rate: int64(bytesPerSec)}
}

// take 按 rate（字节/秒）精确 pacing：扣除上次调用以来累积的额度后，剩余部分按全速睡眠。
// 多并发串行通过互斥，保证整体聚合速率稳定在 rate。
func (t *tokenBucket) take(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if !t.last.IsZero() {
		n -= int64(now.Sub(t.last).Seconds() * float64(t.rate))
	}
	t.last = now
	if n > 0 {
		time.Sleep(time.Duration(float64(n) / float64(t.rate) * float64(time.Second)))
	}
}

// newMockCDNServer 复刻真实两步流程：/dlink 返回 Location 指向 /data（step1，仅取头）；
// /data 带 Range → 按令牌桶限速后精确返回字节（step2）。
func newMockCDNServer(capBytesPerSec float64) (*httptest.Server, *tokenBucket) {
	bkt := newTokenBucket(capBytesPerSec)
	data := make([]byte, 1024*1024*1024)
	for i := range data {
		if i%512 == 0 {
			data[i] = byte(i)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/dlink", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		off, _ := strconv.ParseInt(q.Get("off"), 10, 64)
		length, _ := strconv.ParseInt(q.Get("len"), 10, 64)
		w.Header().Set("Location", fmt.Sprintf("http://%s/data?fsid=%s&off=%d&len=%d", r.Host, q.Get("fsid"), off, length))
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		var off, length int64
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			parts := strings.SplitN(rng[6:], "-", 2)
			off, _ = strconv.ParseInt(parts[0], 10, 64)
			if len(parts) == 2 && parts[1] != "" {
				if end, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
					length = end - off + 1
				}
			}
		} else if q := r.URL.Query(); q.Get("len") != "" {
			off, _ = strconv.ParseInt(q.Get("off"), 10, 64)
			length, _ = strconv.ParseInt(q.Get("len"), 10, 64)
		}
		if length <= 0 || off+length > int64(len(data)) {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if bkt.rate <= 0 { // 不限速
			w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
			w.WriteHeader(http.StatusOK)
			w.Write(data[off : off+length])
			return
		}
		bkt.take(length) // 服务端限速：先消耗令牌再写
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
		w.Write(data[off : off+length])
	})
	return httptest.NewServer(mux), bkt
}

// fetchBlocks 并发拉取同一文件（fsID）的多个非重叠块，返回总字节数与耗时。
func fetchBlocks(t *testing.T, sched *Scheduler, fsID int64, dlink string, blocks []int64, perFileCap int) (float64, bool) {
	t.Helper()
	sched.SetPerFileCap(perFileCap)
	ctx := context.Background()
	var total int64
	var wg sync.WaitGroup
	for _, off := range blocks {
		wg.Add(1)
		go func(off int64) {
			defer wg.Done()
			data, err := sched.FetchRange(ctx, fsID, dlink, off, 512*1024)
			if err != nil {
				t.Errorf("FetchRange fsid=%d off=%d: %v", fsID, off, err)
				return
			}
			atomic.AddInt64(&total, int64(len(data)))
		}(off)
	}
	start := time.Now()
	wg.Wait()
	if total == 0 {
		return 0, false
	}
	return float64(total) / time.Since(start).Seconds() / (1024 * 1024), true
}

// TestSchedulerThroughput 隔离客户端侧瓶颈（走真实 curl 下载路径）：
//   A. 单文件内并行度（PerFileSem=1 vs 8）—— 能否用多条连接加速单文件；
//   B. 全局并发上限（MaxParallel=2 vs 5）—— 多文件的聚合吞吐天花板；
//   C. 服务端限速 30MB/s：并行再高，聚合也被钉在服务端上限。
func TestSchedulerThroughput(t *testing.T) {
	const block = 512 * 1024

	// A. 单文件内并行度：同一文件并发拉 8 个块。
	srvA, _ := newMockCDNServer(0)
	defer srvA.Close()
	blocks := make([]int64, 0, 8)
	for i := 0; i < 8; i++ {
		blocks = append(blocks, int64(i)*block)
	}
	dlinkA := fmt.Sprintf("%s/dlink?fsid=1", srvA.URL)

	var serialBytes, parallelBytes float64
	if b, ok := fetchBlocks(t, NewScheduler(10, 30*time.Second, 15*time.Second), 1, dlinkA, blocks, 1); ok {
		serialBytes = b
	}
	if b, ok := fetchBlocks(t, NewScheduler(10, 30*time.Second, 15*time.Second), 1, dlinkA, blocks, 8); ok {
		parallelBytes = b
	}
	fmt.Printf("[A] 单文件 8 块并发: PerFileSem=1(串行)=%.1f MB/s, PerFileSem=8=%.1f MB/s\n", serialBytes, parallelBytes)

	// B. 全局并发上限：5 个独立文件各串行拉块。
	srvB, _ := newMockCDNServer(0)
	defer srvB.Close()
	multiSeq := func(maxP int) float64 {
		sched := NewScheduler(maxP, 30*time.Second, 15*time.Second)
		sched.SetPerFileCap(1)
		ctx := context.Background()
		var wg sync.WaitGroup
		start := time.Now()
		for f := 0; f < 5; f++ {
			wg.Add(1)
			go func(fsID int64) {
				defer wg.Done()
				dlink := fmt.Sprintf("%s/dlink?fsid=%d", srvB.URL, fsID)
				for b := 0; b < 8; b++ {
					if _, err := sched.FetchRange(ctx, fsID, dlink, int64(b)*block, block); err != nil {
						t.Errorf("file %d blk %d: %v", fsID, b, err)
						return
					}
				}
			}(int64(f + 1))
		}
		wg.Wait()
		want := int64(5 * 8) * block
		return float64(want) / time.Since(start).Seconds() / (1024 * 1024)
	}
	seqMax2 := multiSeq(2)
	seqMax5 := multiSeq(5)
	fmt.Printf("[B] 5 文件串行逐块聚合: MaxParallel=2=%.1f MB/s, MaxParallel=5=%.1f MB/s\n", seqMax2, seqMax5)

	// C. 服务端限速 30MB/s：单文件内并行 + 全局并发拉满，看能否突破上限。
	srvC, _ := newMockCDNServer(30 * 1024 * 1024)
	defer srvC.Close()
	schedD := NewScheduler(5, 30*time.Second, 15*time.Second)
	schedD.SetPerFileCap(8)
	dlinkD := fmt.Sprintf("%s/dlink?fsid=99", srvC.URL)
	var wg sync.WaitGroup
	start := time.Now()
	total := int64(0)
	for i := 0; i < 5*8; i++ {
		wg.Add(1)
		go func(off int64) {
			defer wg.Done()
			if _, err := schedD.FetchRange(context.Background(), 99, dlinkD, off*block, block); err != nil {
				t.Errorf("[cap] parallel: %v", err)
				return
			}
			atomic.AddInt64(&total, block)
		}(int64(i))
	}
	wg.Wait()
	capParallel := float64(total) / time.Since(start).Seconds() / (1024 * 1024)
	fmt.Printf("[C] 服务端限速 30MB/s: 单文件8并行+全局5聚合=%.1f MB/s（≈30 说明瓶颈在服务端，非客户端）\n", capParallel)
}
