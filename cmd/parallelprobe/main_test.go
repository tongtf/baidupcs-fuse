package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"baidupcs-fuse/adapter"
	"baidupcs-fuse/mount"
)

// fakePCS 记录上传/目录调用并可注入错误，用于验证秒传→分片降级与错误传播。
type fakePCS struct {
	statEntry *mount.RemoteEntry
	statErr   error
	dlink     string
	locateErr error

	mkdirs   []string
	mkdirErr error

	rapidCalls  int
	rapidErr    error
	rapidPath   string
	rapidSize   int64
	rapidMD5    string
	rapidBlocks []string

	precreateID   string
	precreateErr  error
	precreatePath string
	precreateSize int64
	precreateList []string

	sliceErr  error
	sliceCall []sliceCall

	createErr  error
	createID   string
	createPath string
	createSize int64
	checksums  map[int]string
}

type sliceCall struct {
	seq    int
	offset int64
	data   string
}

func (f *fakePCS) Stat(context.Context, string) (*mount.RemoteEntry, error) {
	return f.statEntry, f.statErr
}
func (f *fakePCS) ReadDir(context.Context, string) ([]*mount.RemoteEntry, error) { return nil, nil }
func (f *fakePCS) LocateDownload(context.Context, string) (string, int64, int64, int64, error) {
	return f.dlink, 1, 0, 0, f.locateErr
}
func (f *fakePCS) QuotaInfo(context.Context) (int64, int64, error) { return 0, 0, nil }
func (f *fakePCS) Mkdir(_ context.Context, path string) error {
	f.mkdirs = append(f.mkdirs, path)
	return f.mkdirErr
}
func (f *fakePCS) Remove(context.Context, ...string) error { return nil }
func (f *fakePCS) Rename(context.Context, string, string) error {
	return nil
}
func (f *fakePCS) RapidUpload(_ context.Context, remotePath string, size int64, contentMD5, _, _, _, _ string, blocks []string) error {
	f.rapidCalls++
	f.rapidPath = remotePath
	f.rapidSize = size
	f.rapidMD5 = contentMD5
	f.rapidBlocks = append([]string(nil), blocks...)
	return f.rapidErr
}
func (f *fakePCS) PrecreateUpload(_ context.Context, remotePath string, size int64, blocks []string) (string, error) {
	f.precreatePath = remotePath
	f.precreateSize = size
	f.precreateList = append([]string(nil), blocks...)
	return f.precreateID, f.precreateErr
}
func (f *fakePCS) UploadSlice(_ context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (string, error) {
	f.sliceCall = append(f.sliceCall, sliceCall{seq: seq, offset: offset, data: string(data)})
	return "", f.sliceErr
}
func (f *fakePCS) CreateSuperFile(_ context.Context, uploadID, remotePath string, size int64, checksums map[int]string) error {
	f.createID = uploadID
	f.createPath = remotePath
	f.createSize = size
	f.checksums = make(map[int]string, len(checksums))
	for k, v := range checksums {
		f.checksums[k] = v
	}
	return f.createErr
}
func (f *fakePCS) UpdateStoken(string) {}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(p, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

// 目的：凭据解析必须接受 BDUSS/stoken 引号形式，拒绝空 BDUSS/缺失文件，并忽略无效行。
func TestParseCreds(t *testing.T) {
	p := filepath.Join(t.TempDir(), "creds")
	if err := os.WriteFile(p, []byte("# comment\nBAD\nBDUSS \"abc\"\nstoken tok\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b, s, err := parseCreds(p)
	if err != nil || b != "abc" || s != "tok" {
		t.Fatalf("bduss=%q stoken=%q err=%v", b, s, err)
	}
	if _, _, err := parseCreds(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing creds should fail")
	}
	p2 := filepath.Join(t.TempDir(), "nobduss")
	if err := os.WriteFile(p2, []byte("stoken x\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseCreds(p2); err == nil || !strings.Contains(err.Error(), "BDUSS") {
		t.Fatalf("no bduss err=%v", err)
	}
}

// 目的：目录创建必须递归补齐父目录，根路径直接成功，API 错误向上传播。
func TestEnsureDir(t *testing.T) {
	pc := &fakePCS{}
	if err := ensureDir(context.Background(), pc, "/"); err != nil {
		t.Fatal(err)
	}
	if err := ensureDir(context.Background(), pc, ""); err != nil {
		t.Fatal(err)
	}
	if len(pc.mkdirs) != 0 {
		t.Fatalf("root created dirs: %v", pc.mkdirs)
	}
	if err := ensureDir(context.Background(), pc, "/a/b/c"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(pc.mkdirs, ",") != "/a,/a/b,/a/b/c" {
		t.Fatalf("mkdirs=%v", pc.mkdirs)
	}

	pc = &fakePCS{mkdirErr: os.ErrPermission}
	if err := ensureDir(context.Background(), pc, "/x/y"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("err=%v", err)
	}
	if len(pc.mkdirs) != 1 || pc.mkdirs[0] != "/x" {
		t.Fatalf("must stop at first error: %v", pc.mkdirs)
	}
}

// 目的：upload 先计算整文件/分片 MD5；秒传命中不再分片，未命中或异常时降级为
// precreate→slice→create，且偏移/校验表必须正确。
func TestUploadFile_RapidAndFallback(t *testing.T) {
	ctx := context.Background()
	content := "hello world"
	local := writeTempFile(t, content)

	pc := &fakePCS{}
	if err := uploadFile(ctx, pc, local, "/dir/"); err != nil {
		t.Fatal(err)
	}
	if pc.rapidCalls != 1 || pc.rapidPath != "/dir/f.txt" || pc.rapidSize != int64(len(content)) {
		t.Fatalf("rapid=%+v", pc)
	}
	if pc.rapidMD5 != md5Hex([]byte(content)) || len(pc.rapidBlocks) != 1 || pc.rapidBlocks[0] != md5Hex([]byte(content)) {
		t.Fatalf("hashes md5=%s blocks=%v", pc.rapidMD5, pc.rapidBlocks)
	}
	if strings.Join(pc.mkdirs, ",") != "/dir" {
		t.Fatalf("mkdirs=%v", pc.mkdirs)
	}
	if len(pc.sliceCall) != 0 {
		t.Fatal("rapid success must not slice upload")
	}

	for _, rapidErr := range []error{mount.ErrNeedUpload, errors.New("not exist"), errors.New("cdn boom")} {
		pc = &fakePCS{rapidErr: rapidErr, precreateID: "up-1"}
		if err := uploadFile(ctx, pc, local, "/dir"); err != nil {
			t.Fatalf("rapidErr=%v: %v", rapidErr, err)
		}
		if pc.precreateID != "up-1" || pc.precreatePath != "/dir/f.txt" || pc.precreateSize != int64(len(content)) {
			t.Fatalf("precreate=%+v", pc)
		}
		if len(pc.precreateList) != 1 || pc.precreateList[0] != md5Hex([]byte(content)) {
			t.Fatalf("precreate list=%v", pc.precreateList)
		}
		if len(pc.sliceCall) != 1 || pc.sliceCall[0].offset != 0 || pc.sliceCall[0].data != content {
			t.Fatalf("slices=%+v", pc.sliceCall)
		}
		if pc.createID != "up-1" || pc.createPath != "/dir/f.txt" || pc.createSize != int64(len(content)) {
			t.Fatalf("create=%+v", pc)
		}
		if len(pc.checksums) != 1 || pc.checksums[0] != md5Hex([]byte(content)) {
			t.Fatalf("checksums=%v", pc.checksums)
		}
	}
}

// 目的：本地文件缺失/precreate 空 ID/分片失败/create 失败必须返回带上下文的错误，
// 不得把半成品当作成功。
func TestUploadFile_ErrorPaths(t *testing.T) {
	ctx := context.Background()
	if err := uploadFile(ctx, &fakePCS{}, filepath.Join(t.TempDir(), "missing"), "/d"); err == nil || !strings.Contains(err.Error(), "open local") {
		t.Fatalf("open err=%v", err)
	}

	local := writeTempFile(t, "abcd")
	pc := &fakePCS{rapidErr: mount.ErrNeedUpload, precreateErr: os.ErrPermission}
	if err := uploadFile(ctx, pc, local, "/d"); err == nil || !strings.Contains(err.Error(), "precreate") {
		t.Fatalf("precreate err=%v", err)
	}

	pc = &fakePCS{rapidErr: mount.ErrNeedUpload}
	if err := uploadFile(ctx, pc, local, "/d"); err == nil || !strings.Contains(err.Error(), "空 uploadid") {
		t.Fatalf("empty id err=%v", err)
	}

	pc = &fakePCS{rapidErr: mount.ErrNeedUpload, precreateID: "u", sliceErr: os.ErrPermission}
	if err := uploadFile(ctx, pc, local, "/d"); err == nil || !strings.Contains(err.Error(), "upload slice 0") {
		t.Fatalf("slice err=%v", err)
	}

	pc = &fakePCS{rapidErr: mount.ErrNeedUpload, precreateID: "u", createErr: os.ErrPermission}
	if err := uploadFile(ctx, pc, local, "/d"); err == nil || !strings.Contains(err.Error(), "create") {
		t.Fatalf("create err=%v", err)
	}

	local = writeTempFile(t, "abcd")
	if err := slowUpload(ctx, &fakePCS{precreateErr: os.ErrPermission}, local, "/d/f", 4, 2, nil); err == nil || !strings.Contains(err.Error(), "precreate") {
		t.Fatalf("slow precreate err=%v", err)
	}
}

// 目的：slowUpload 必须按 sliceSize 计算偏移并逐片读取，末尾短片长度正确。
func TestSlowUpload_MultiSliceOffsets(t *testing.T) {
	local := writeTempFile(t, "abcde")
	pc := &fakePCS{precreateID: "u2"}
	if err := slowUpload(context.Background(), pc, local, "/d/f", 5, 2, []string{"h0", "h1", "h2"}); err != nil {
		t.Fatal(err)
	}
	if len(pc.sliceCall) != 3 {
		t.Fatalf("slices=%+v", pc.sliceCall)
	}
	for i, want := range []struct {
		off  int64
		data string
	}{{0, "ab"}, {2, "cd"}, {4, "e"}} {
		if pc.sliceCall[i].seq != i || pc.sliceCall[i].offset != want.off || pc.sliceCall[i].data != want.data {
			t.Fatalf("slice[%d]=%+v want off=%d data=%q", i, pc.sliceCall[i], want.off, want.data)
		}
	}
	if len(pc.checksums) != 3 || pc.checksums[2] != "h2" {
		t.Fatalf("checksums=%v", pc.checksums)
	}
}

// 目的：readFull 必须在 EOF/短读/已关闭文件时返回实际读取字节数，供分片上传使用。
func TestReadFull(t *testing.T) {
	p := writeTempFile(t, "abc")
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if n := readFull(f, buf); n != 3 || string(buf[:n]) != "abc" {
		t.Fatalf("n=%d data=%q", n, buf[:n])
	}
	if n := readFull(f, buf); n != 0 {
		t.Fatalf("eof n=%d", n)
	}
	f.Close()
	if n := readFull(f, buf); n != 0 {
		t.Fatalf("closed n=%d", n)
	}
}

// 目的：秒传未命中识别只匹配已知“文件不存在”语义，未知错误必须继续走分片。
func TestIsRapidNotFound(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("NeedUpload"), true},
		{errors.New("文件不存在"), true},
		{errors.New("not exist"), true},
		{errors.New("rate limited"), false},
	} {
		if got := isRapidNotFound(tc.err); got != tc.want {
			t.Fatalf("isRapidNotFound(%v)=%v want %v", tc.err, got, tc.want)
		}
	}
}

// 目的：diag 必须发送 Range 与 Connection:close，并对每个响应按状态码输出；
// 连接失败时继续尝试其它头组合而不是中断诊断。
func TestDiag_HTTPAndCurl(t *testing.T) {
	var requests atomic.Int32
	var sawRange atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("Range") == "bytes=0-65535" {
			sawRange.Store(true)
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abc"))
	}))
	defer srv.Close()

	if err := diag(context.Background(), srv.URL+"/file"); err != nil {
		t.Fatal(err)
	}
	if !sawRange.Load() || requests.Load() < 4 {
		t.Fatalf("requests=%d range=%v", requests.Load(), sawRange.Load())
	}

	// 不可达 dlink：HTTP 与 curl 都失败，但函数仍返回 nil。
	if err := diag(context.Background(), "http://127.0.0.1:1/file"); err != nil {
		t.Fatal(err)
	}
}

// 目的：runDiag 必须把 LocateDownload 失败包装为明确错误；成功时执行诊断。
func TestRunDiag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))
	}))
	defer srv.Close()
	pc := &fakePCS{dlink: srv.URL + "/f"}
	if err := runDiag(context.Background(), pc, "/f"); err != nil {
		t.Fatal(err)
	}
	if err := runDiag(context.Background(), &fakePCS{locateErr: os.ErrPermission}, "/f"); err == nil || !strings.Contains(err.Error(), "LocateDownload") {
		t.Fatalf("locate err=%v", err)
	}
}

// probeReader 是可注入延迟/错误的远端 reader，用于稳定验证单连接/并发统计分支。
type probeReader struct {
	data       []byte
	baseDelay  time.Duration
	chunkDelay time.Duration
	errOffset  int64
	hasErr     bool
	closed     bool
}

func (r *probeReader) ReadAt(ctx context.Context, off int64, size int) ([]byte, error) {
	isBase := off == 0 && size == len(r.data)
	if isBase {
		time.Sleep(r.baseDelay)
	} else {
		time.Sleep(r.chunkDelay)
	}
	if r.hasErr && off == r.errOffset {
		return nil, os.ErrPermission
	}
	if off < 0 || off >= int64(len(r.data)) {
		return nil, nil
	}
	end := off + int64(size)
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	return append([]byte(nil), r.data[off:end]...), nil
}
func (r *probeReader) Size() int64  { return int64(len(r.data)) }
func (r *probeReader) FSID() int64  { return 1 }
func (r *probeReader) Mtime() int64 { return 0 }
func (r *probeReader) Reload(context.Context) error {
	return nil
}
func (r *probeReader) Close() error { r.closed = true; return nil }

// 目的：runProbe 必须切分并发区间、统计单连接与并发完整性，并在 Stat/OpenRead
// 失败时返回明确错误；快慢两种比例覆盖结论分支。
func TestRunProbe_Branches(t *testing.T) {
	ctx := context.Background()
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i)
	}
	pc := &fakePCS{statEntry: &mount.RemoteEntry{FSID: 7, Size: int64(len(data))}}

	t.Run("parallel speedup", func(t *testing.T) {
		reader := &probeReader{data: data, baseDelay: 30 * time.Millisecond, chunkDelay: time.Millisecond}
		stopped := false
		open := func(context.Context, adapter.PCSClient, string, string) (mount.RemoteReader, func(), error) {
			return reader, func() { stopped = true }, nil
		}
		if err := runProbeWith(ctx, pc, "/f", 2, "", open); err != nil {
			t.Fatal(err)
		}
		if !reader.closed || !stopped {
			t.Fatalf("reader closed=%v stop=%v", reader.closed, stopped)
		}
	})

	t.Run("server limited", func(t *testing.T) {
		reader := &probeReader{data: data, baseDelay: time.Millisecond, chunkDelay: 40 * time.Millisecond}
		open := func(context.Context, adapter.PCSClient, string, string) (mount.RemoteReader, func(), error) {
			return reader, nil, nil
		}
		if err := runProbeWith(ctx, pc, "/f", 2, "", open); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("read errors", func(t *testing.T) {
		reader := &probeReader{data: data, hasErr: true, errOffset: 0}
		open := func(context.Context, adapter.PCSClient, string, string) (mount.RemoteReader, func(), error) {
			return reader, func() {}, nil
		}
		if err := runProbeWith(ctx, pc, "/f", 2, "", open); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("stat error", func(t *testing.T) {
		if err := runProbeWith(ctx, &fakePCS{statErr: os.ErrPermission}, "/f", 2, "", nil); err == nil || !strings.Contains(err.Error(), "Stat 失败") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("open error", func(t *testing.T) {
		open := func(context.Context, adapter.PCSClient, string, string) (mount.RemoteReader, func(), error) {
			return nil, nil, os.ErrPermission
		}
		if err := runProbeWith(ctx, pc, "/f", 2, "", open); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("err=%v", err)
		}
	})
}

// 目的：CLI 分发必须支持凭据路径覆盖、upload/probe/diag 子命令、PARALLEL_N，
// 并把各阶段错误加上明确上下文。
func TestRun_DispatchAndErrors(t *testing.T) {
	creds := filepath.Join(t.TempDir(), "creds")
	if err := os.WriteFile(creds, []byte("BDUSS abcdefghijk\nstoken s\n"), 0600); err != nil {
		t.Fatal(err)
	}

	makeDeps := func(env map[string]string) (probeDeps, *depsRecorder) {
		rec := &depsRecorder{}
		deps := probeDeps{
			getenv: func(k string) string { return env[k] },
			newClient: func(b, s string) adapter.PCSClient {
				rec.bduss, rec.stoken = b, s
				return &fakePCS{}
			},
			upload: func(_ context.Context, _ adapter.PCSClient, local, remote string) error {
				rec.args = []string{local, remote}
				return rec.err
			},
			diag: func(_ context.Context, _ adapter.PCSClient, remote string) error {
				rec.args = []string{remote}
				return rec.err
			},
			probe: func(_ context.Context, _ adapter.PCSClient, remote string, n int, bduss string) error {
				rec.args = []string{remote}
				rec.n = n
				rec.bduss = bduss
				return rec.err
			},
		}
		return deps, rec
	}

	t.Run("upload with creds override", func(t *testing.T) {
		deps, rec := makeDeps(nil)
		if err := run([]string{"prog", creds, "upload", "local", "remote"}, deps); err != nil {
			t.Fatal(err)
		}
		if strings.Join(rec.args, ",") != "local,remote" || rec.bduss != "abcdefghijk" || rec.stoken != "s" {
			t.Fatalf("rec=%+v", rec)
		}
	})

	t.Run("diag uses env creds", func(t *testing.T) {
		deps, rec := makeDeps(map[string]string{"BDFS_ENV": creds})
		if err := run([]string{"prog", "diag", "/remote"}, deps); err != nil {
			t.Fatal(err)
		}
		if len(rec.args) != 1 || rec.args[0] != "/remote" {
			t.Fatalf("rec=%+v", rec)
		}
	})

	t.Run("probe nconc and default path", func(t *testing.T) {
		deps, rec := makeDeps(map[string]string{"BDFS_ENV": creds, "PARALLEL_N": "7"})
		if err := run([]string{"prog", "probe", "/p"}, deps); err != nil {
			t.Fatal(err)
		}
		if rec.n != 7 || rec.args[0] != "/p" {
			t.Fatalf("rec=%+v", rec)
		}

		deps, rec = makeDeps(map[string]string{"BDFS_ENV": creds, "PARALLEL_N": "bad"})
		if err := run([]string{"prog"}, deps); err != nil {
			t.Fatal(err)
		}
		if rec.n != 4 || !strings.Contains(rec.args[0], "bdfuse-test") {
			t.Fatalf("default rec=%+v", rec)
		}
	})

	t.Run("home fallback", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		if err := os.WriteFile(filepath.Join(home, ".bdfs.env"), []byte("BDUSS homeb\n"), 0600); err != nil {
			t.Fatal(err)
		}
		deps, rec := makeDeps(nil)
		if err := run([]string{"prog"}, deps); err != nil {
			t.Fatal(err)
		}
		if rec.bduss != "homeb" {
			t.Fatalf("bduss=%q", rec.bduss)
		}
	})

	t.Run("errors carry context", func(t *testing.T) {
		deps, rec := makeDeps(map[string]string{"BDFS_ENV": creds})
		rec.err = os.ErrPermission
		if err := run([]string{"prog", "upload", "l", "r"}, deps); err == nil || !strings.Contains(err.Error(), "上传失败") {
			t.Fatalf("upload err=%v", err)
		}
		if err := run([]string{"prog", "diag", "/r"}, deps); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("diag err=%v", err)
		}
		if err := run([]string{"prog", "probe", "/r"}, deps); err == nil || !strings.Contains(err.Error(), "探测失败") {
			t.Fatalf("probe err=%v", err)
		}
		if err := run([]string{"prog", filepath.Join(t.TempDir(), "missing")}, deps); err == nil || !strings.Contains(err.Error(), "解析凭据失败") {
			t.Fatalf("creds err=%v", err)
		}
	})
}

type depsRecorder struct {
	bduss  string
	stoken string
	args   []string
	n      int
	err    error
}
