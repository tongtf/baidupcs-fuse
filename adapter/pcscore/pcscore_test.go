package pcscore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/qjfoidnh/BaiduPCS-Go/baidupcs"
	"github.com/qjfoidnh/BaiduPCS-Go/baidupcs/pcserror"
)

// fakeError 用于构造任意 errno/msg 的 pcserror.Error。
type fakeError struct {
	code int
	msg  string
	err  error
}

func (e *fakeError) Error() string          { return fmt.Sprintf("fake error %d: %s", e.code, e.msg) }
func (e *fakeError) SetJSONError(err error) { e.err = err }
func (e *fakeError) SetNetError(err error)  { e.err = err }
func (e *fakeError) SetRemoteError()        {}
func (e *fakeError) GetOperation() string   { return "test" }
func (e *fakeError) GetErrType() pcserror.ErrType {
	return pcserror.ErrTypeRemoteError
}
func (e *fakeError) GetRemoteErrCode() int   { return e.code }
func (e *fakeError) GetRemoteErrMsg() string { return e.msg }
func (e *fakeError) GetError() error         { return e.err }

// fakePCS 实现测试所需的 BaiduPCS 方法集。
type fakePCS struct {
	meta    *baidupcs.FileDirectory
	metaErr pcserror.Error

	list    baidupcs.FileDirectoryList
	listErr pcserror.Error

	locate    *baidupcs.URLInfo
	locateErr pcserror.Error

	quota, used int64
	quotaErr    pcserror.Error

	mkdirErr  pcserror.Error
	removeErr pcserror.Error
	renameErr pcserror.Error

	uploadURL string
	uploadMD5 string
	uploadErr pcserror.Error

	superErr pcserror.Error

	lastStoken string
}

func (f *fakePCS) FilesDirectoriesMeta(string) (*baidupcs.FileDirectory, pcserror.Error) {
	return f.meta, f.metaErr
}

func (f *fakePCS) FilesDirectoriesList(string, *baidupcs.OrderOptions) (baidupcs.FileDirectoryList, pcserror.Error) {
	return f.list, f.listErr
}

func (f *fakePCS) LocateDownload(string) (*baidupcs.URLInfo, pcserror.Error) {
	return f.locate, f.locateErr
}

func (f *fakePCS) QuotaInfo() (int64, int64, pcserror.Error) {
	return f.quota, f.used, f.quotaErr
}

func (f *fakePCS) Mkdir(string) pcserror.Error { return f.mkdirErr }

func (f *fakePCS) Remove(...string) pcserror.Error { return f.removeErr }

func (f *fakePCS) Rename(_, _ string) pcserror.Error { return f.renameErr }

func (f *fakePCS) UploadTmpFile(_, _ string, _ int, _ int64, fn baidupcs.UploadFunc) (string, pcserror.Error) {
	if f.uploadErr != nil {
		return "", f.uploadErr
	}
	if fn != nil {
		resp, err := fn(f.uploadURL, nil)
		if err != nil {
			return "", &fakeError{msg: "upload failed", err: err}
		}
		resp.Body.Close()
	}
	return f.uploadMD5, nil
}

func (f *fakePCS) UploadCreateSuperFile(_, _ string, _ int64, _ string, _ map[int]string) pcserror.Error {
	return f.superErr
}

func (f *fakePCS) SetStoken(token string) { f.lastStoken = token }

func urlInfo(t *testing.T, raw string) *baidupcs.URLInfo {
	t.Helper()
	var info baidupcs.URLInfo
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		t.Fatalf("unmarshal URLInfo: %v", err)
	}
	return &info
}

// 目的：New 必须把非正超时归一为默认值，并在有 STOKEN 时完成初始化，返回可用的 Library。
func TestNew(t *testing.T) {
	lib := New(1, "bduss", "", 0)
	core, ok := lib.(*coreLibrary)
	if !ok {
		t.Fatalf("New returned %T", lib)
	}
	if core.timeout != defaultHTTPTimeout {
		t.Fatalf("timeout=%v, want %v", core.timeout, defaultHTTPTimeout)
	}

	lib = New(1, "bduss", "stoken", 5*time.Second)
	core, ok = lib.(*coreLibrary)
	if !ok || core.timeout != 5*time.Second {
		t.Fatalf("timeout=%v", core.timeout)
	}
	if core.pcs == nil || core.raw == nil {
		t.Fatal("New must initialize both pcs and raw")
	}
}

// 目的：FileMeta 字段映射、目录/文件 mode 推断与 errno 错误映射。
func TestStat(t *testing.T) {
	fd := &baidupcs.FileDirectory{FsID: 7, Path: "/a", Filename: "a", Size: 9, Mtime: 11}
	c := &coreLibrary{pcs: &fakePCS{meta: fd}}
	got, err := c.Stat(context.Background(), "/a")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got.FsID != 7 || got.Path != "/a" || got.Filename != "a" || got.Size != 9 || got.Mtime != 11 || got.Mode != 0644 {
		t.Fatalf("unexpected meta: %+v", got)
	}

	fd.Isdir = true
	got, err = c.Stat(context.Background(), "/a")
	if err != nil || got.Mode != 0755 {
		t.Fatalf("dir mode=%o err=%v", got.Mode, err)
	}

	c = &coreLibrary{pcs: &fakePCS{metaErr: &fakeError{code: -9, msg: "文件不存在"}}}
	if _, err := c.Stat(context.Background(), "/missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat err=%v, want ErrNotExist", err)
	}
}

// 目的：目录列表映射、空目录返回非 nil 切片、errno 错误映射。
func TestReadDir(t *testing.T) {
	fdl := baidupcs.FileDirectoryList{
		{FsID: 1, Filename: "a", Size: 1},
		{FsID: 2, Filename: "d", Isdir: true},
	}
	c := &coreLibrary{pcs: &fakePCS{list: fdl}}
	got, err := c.ReadDir(context.Background(), "/")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(got) != 2 || got[0].FsID != 1 || got[1].Mode != 0755 {
		t.Fatalf("unexpected entries: %+v", got)
	}

	// 空目录必须返回非 nil 的零长度切片。
	c = &coreLibrary{pcs: &fakePCS{list: nil}}
	got, err = c.ReadDir(context.Background(), "/empty")
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty ReadDir = (%v,%v), want non-nil empty", got, err)
	}

	c = &coreLibrary{pcs: &fakePCS{listErr: &fakeError{code: -6, msg: "bad token"}}}
	if _, err := c.ReadDir(context.Background(), "/"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("ReadDir err=%v, want ErrPermission", err)
	}
}

// 目的：https 优先、无链接、取链错误、元信息错误与 URL 返回语义。
func TestLocateDownload(t *testing.T) {
	fd := &baidupcs.FileDirectory{FsID: 3, Size: 10, Mtime: 4}

	t.Run("prefers https", func(t *testing.T) {
		c := &coreLibrary{pcs: &fakePCS{
			locate: urlInfo(t, `{"urls":[{"url":"http://a"},{"url":"https://b"}]}`),
			meta:   fd,
		}}
		dlink, fsid, size, mtime, err := c.LocateDownload(context.Background(), "/a")
		if err != nil {
			t.Fatal(err)
		}
		if dlink != "https://b" || fsid != 3 || size != 10 || mtime != 4 {
			t.Fatalf("got %q %d %d %d", dlink, fsid, size, mtime)
		}
	})

	t.Run("no urls", func(t *testing.T) {
		c := &coreLibrary{pcs: &fakePCS{locate: urlInfo(t, `{"urls":[]}`)}}
		if _, _, _, _, err := c.LocateDownload(context.Background(), "/a"); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("locate error", func(t *testing.T) {
		c := &coreLibrary{pcs: &fakePCS{locateErr: &fakeError{code: -10, msg: "deleted"}}}
		if _, _, _, _, err := c.LocateDownload(context.Background(), "/a"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v, want ErrNotExist", err)
		}
	})

	t.Run("meta error after locate", func(t *testing.T) {
		c := &coreLibrary{pcs: &fakePCS{
			locate:  urlInfo(t, `{"urls":[{"url":"http://a"}]}`),
			metaErr: &fakeError{code: -9, msg: "gone"},
		}}
		dlink, _, _, _, err := c.LocateDownload(context.Background(), "/a")
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v, want ErrNotExist", err)
		}
		if dlink != "http://a" {
			t.Fatalf("dlink=%q, want http://a", dlink)
		}
	})

	t.Run("single http url", func(t *testing.T) {
		c := &coreLibrary{pcs: &fakePCS{
			locate: urlInfo(t, `{"urls":[{"url":"http://only"}]}`),
			meta:   fd,
		}}
		dlink, _, _, _, err := c.LocateDownload(context.Background(), "/a")
		if err != nil || dlink != "http://only" {
			t.Fatalf("dlink=%q err=%v", dlink, err)
		}
	})
}

// 目的：配额数值透传与权限错误映射。
func TestQuotaInfo(t *testing.T) {
	c := &coreLibrary{pcs: &fakePCS{quota: 100, used: 40}}
	total, used, err := c.QuotaInfo(context.Background())
	if err != nil || total != 100 || used != 40 {
		t.Fatalf("QuotaInfo = %d,%d,%v", total, used, err)
	}

	c = &coreLibrary{pcs: &fakePCS{quotaErr: &fakeError{code: -7, msg: "denied"}}}
	if _, _, err := c.QuotaInfo(context.Background()); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("err=%v, want ErrPermission", err)
	}
}

// 目的：Mkdir/Remove/Rename 正确委托并把 errno 映射为标准错误。
func TestMutationWrappers(t *testing.T) {
	ok := &coreLibrary{pcs: &fakePCS{}}
	if err := ok.Mkdir(context.Background(), "/d"); err != nil {
		t.Error(err)
	}
	if err := ok.Remove(context.Background(), "/d"); err != nil {
		t.Error(err)
	}
	if err := ok.Rename(context.Background(), "/a", "/b"); err != nil {
		t.Error(err)
	}

	boom := &fakeError{code: -11, msg: "exists"}
	bad := &coreLibrary{pcs: &fakePCS{mkdirErr: boom, removeErr: boom, renameErr: boom}}
	if err := bad.Mkdir(context.Background(), "/d"); !errors.Is(err, os.ErrExist) {
		t.Errorf("Mkdir err=%v", err)
	}
	if err := bad.Remove(context.Background(), "/d"); !errors.Is(err, os.ErrExist) {
		t.Errorf("Remove err=%v", err)
	}
	if err := bad.Rename(context.Background(), "/a", "/b"); !errors.Is(err, os.ErrExist) {
		t.Errorf("Rename err=%v", err)
	}
}

// 目的：分片上传构造 PUT 请求、透传内容与 MD5，并传播 API 错误。
func TestUploadSlice(t *testing.T) {
	var gotBody, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := &fakePCS{uploadURL: srv.URL, uploadMD5: "abc"}
	c := &coreLibrary{pcs: f, timeout: 2 * time.Second}
	md5, err := c.UploadSlice(context.Background(), "uid", "/a", 1, 0, []byte("data"))
	if err != nil || md5 != "abc" {
		t.Fatalf("UploadSlice = %q,%v", md5, err)
	}
	if gotMethod != http.MethodPut || gotBody != "data" {
		t.Fatalf("request method=%q body=%q", gotMethod, gotBody)
	}

	f = &fakePCS{uploadErr: &fakeError{code: -9, msg: "gone"}}
	c = &coreLibrary{pcs: f}
	if _, err := c.UploadSlice(context.Background(), "uid", "/a", 1, 0, []byte("x")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err=%v, want ErrNotExist", err)
	}
}

// 目的：合并请求参数正确委托，并透传 errno。
func TestCreateSuperFile(t *testing.T) {
	c := &coreLibrary{pcs: &fakePCS{}}
	if err := c.CreateSuperFile(context.Background(), "uid", "/a", 10, map[int]string{1: "m"}); err != nil {
		t.Fatal(err)
	}
	c = &coreLibrary{pcs: &fakePCS{superErr: &fakeError{code: -11, msg: "exists"}}}
	if err := c.CreateSuperFile(context.Background(), "uid", "/a", 10, nil); !errors.Is(err, os.ErrExist) {
		t.Fatalf("err=%v, want ErrExist", err)
	}
}

// 目的：空 STOKEN 被忽略，非空值透传给底层客户端。
func TestUpdateStoken(t *testing.T) {
	f := &fakePCS{}
	c := &coreLibrary{pcs: f}
	c.UpdateStoken("")
	if f.lastStoken != "" {
		t.Fatalf("empty token should be ignored, got %q", f.lastStoken)
	}
	c.UpdateStoken("new")
	if f.lastStoken != "new" {
		t.Fatalf("lastStoken=%q", f.lastStoken)
	}
}

// 目的：errno/消息/底层错误到标准 error 的完整映射。
func TestToError(t *testing.T) {
	if err := toError(nil); err != nil {
		t.Fatalf("nil => %v", err)
	}

	cases := []struct {
		code int
		want error
	}{
		{-6, os.ErrPermission},
		{-7, os.ErrPermission},
		{-12, os.ErrPermission},
		{-14, os.ErrPermission},
		{-9, os.ErrNotExist},
		{-10, os.ErrNotExist},
		{-11, os.ErrExist},
	}
	for _, tt := range cases {
		err := toError(&fakeError{code: tt.code, msg: "remote"})
		if !errors.Is(err, tt.want) {
			t.Errorf("code %d => %v, want %v", tt.code, err, tt.want)
		}
	}

	// 无 errno 但 msg 含 Permission。
	if err := toError(&fakeError{msg: "Permission denied"}); !errors.Is(err, os.ErrPermission) {
		t.Errorf("permission msg => %v", err)
	}

	// errMsg 为空时回退到底层 error。
	err := toError(&fakeError{code: 42, err: errors.New("network down")})
	if err == nil || err.Error() != "pcs error 42: network down" {
		t.Errorf("fallback => %v", err)
	}
}
