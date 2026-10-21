package adapter

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"baidupcs-fuse/mount"
)

// uploadRecorder 记录上传会话的调用序列，并可注入各阶段错误。
type uploadRecorder struct {
	*MockPCSClient
	mu sync.Mutex

	rapidCalls     int32
	precreateCalls int32
	sliceCalls     int32
	createCalls    int32

	rapidErr     error
	precreateID  string
	precreateErr error
	sliceErr     error
	createErr    error

	blocks    []string
	offsets   []int64
	checksums map[int]string
}

func (u *uploadRecorder) RapidUpload(_ context.Context, _ string, _ int64, _, _, _, _, _ string, blocks []string) error {
	atomic.AddInt32(&u.rapidCalls, 1)
	u.mu.Lock()
	u.blocks = append([]string(nil), blocks...)
	u.mu.Unlock()
	return u.rapidErr
}

func (u *uploadRecorder) PrecreateUpload(context.Context, string, int64, []string) (string, error) {
	atomic.AddInt32(&u.precreateCalls, 1)
	return u.precreateID, u.precreateErr
}

func (u *uploadRecorder) UploadSlice(_ context.Context, _, _ string, seq int, off int64, data []byte) (string, error) {
	atomic.AddInt32(&u.sliceCalls, 1)
	if u.sliceErr != nil {
		return "", u.sliceErr
	}
	sum := md5.Sum(data)
	u.mu.Lock()
	u.offsets = append(u.offsets, off)
	u.mu.Unlock()
	return hex.EncodeToString(sum[:]), nil
}

func (u *uploadRecorder) CreateSuperFile(_ context.Context, _, _ string, _ int64, checksums map[int]string) error {
	atomic.AddInt32(&u.createCalls, 1)
	u.mu.Lock()
	u.checksums = make(map[int]string, len(checksums))
	for k, v := range checksums {
		u.checksums[k] = v
	}
	u.mu.Unlock()
	return u.createErr
}

func newUploadRecorder() *uploadRecorder {
	return &uploadRecorder{MockPCSClient: &MockPCSClient{}}
}

// 目的：空文件上传必须走“秒传→precreate→合并”三条正确路径，且不触发分片上传。
func TestUploadSession_EmptyFileFlows(t *testing.T) {
	t.Run("rapid upload hit", func(t *testing.T) {
		client := newUploadRecorder()
		u := NewUploadSession(client, NewStagingManager(t.TempDir(), true))
		res, err := u.Upload(context.Background(), "", "/empty", 0)
		if err != nil || res == nil || res.Size != 0 {
			t.Fatalf("res=%+v err=%v", res, err)
		}
		if atomic.LoadInt32(&client.rapidCalls) != 1 || atomic.LoadInt32(&client.precreateCalls) != 0 {
			t.Fatalf("rapid=%d precreate=%d", client.rapidCalls, client.precreateCalls)
		}
		if len(client.blocks) != 1 || client.blocks[0] != "d41d8cd98f00b204e9800998ecf8427e" {
			t.Fatalf("empty block_list=%v", client.blocks)
		}
	})

	t.Run("rapid miss precreate merge", func(t *testing.T) {
		client := newUploadRecorder()
		client.rapidErr = mount.ErrNeedUpload
		client.precreateID = "uid-empty"
		u := NewUploadSession(client, NewStagingManager(t.TempDir(), true))
		if _, err := u.Upload(context.Background(), "", "/empty", 0); err != nil {
			t.Fatal(err)
		}
		if atomic.LoadInt32(&client.createCalls) != 1 {
			t.Fatalf("create calls=%d", client.createCalls)
		}
		if got := client.checksums[0]; got != "d41d8cd98f00b204e9800998ecf8427e" {
			t.Fatalf("checksum=%q", got)
		}
	})

	t.Run("precreate says rapid success", func(t *testing.T) {
		client := newUploadRecorder()
		client.rapidErr = mount.ErrNeedUpload
		client.precreateID = ""
		u := NewUploadSession(client, NewStagingManager(t.TempDir(), true))
		if _, err := u.Upload(context.Background(), "", "/empty", 0); err != nil {
			t.Fatal(err)
		}
		if atomic.LoadInt32(&client.createCalls) != 0 {
			t.Fatal("no merge request expected when precreate returns empty uploadid")
		}
	})

	t.Run("precreate and merge errors", func(t *testing.T) {
		client := newUploadRecorder()
		client.rapidErr = mount.ErrNeedUpload
		client.precreateErr = os.ErrPermission
		u := NewUploadSession(client, NewStagingManager(t.TempDir(), true))
		if _, err := u.Upload(context.Background(), "", "/empty", 0); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("err=%v", err)
		}

		client = newUploadRecorder()
		client.rapidErr = mount.ErrNeedUpload
		client.precreateID = "uid"
		client.createErr = os.ErrExist
		u = NewUploadSession(client, NewStagingManager(t.TempDir(), true))
		if _, err := u.Upload(context.Background(), "", "/empty", 0); !errors.Is(err, os.ErrExist) {
			t.Fatalf("err=%v", err)
		}
	})
}

// 目的：非空文件秒传失败后，按分片并发上传并合并；校验分片偏移、MD5 汇总、进度清理与统计。
func TestUploadSession_SliceUploadFlow(t *testing.T) {
	dir := t.TempDir()
	data := []byte("abcdefgh")
	staging := filepath.Join(dir, "data.tmp")
	if err := os.WriteFile(staging, data, 0600); err != nil {
		t.Fatal(err)
	}

	client := newUploadRecorder()
	client.rapidErr = mount.ErrNeedUpload
	client.precreateID = "uid-1"
	stats := NewTransferStats()
	u := NewUploadSession(client, NewStagingManager(dir, true))
	u.SetSliceSize(4)
	u.SetMaxParallel(2)
	u.SetTransferStats(stats)

	res, err := u.Upload(context.Background(), staging, "/dir/file.bin", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if res.Path != "/dir/file.bin" || res.Size != 8 {
		t.Fatalf("result=%+v", res)
	}
	if atomic.LoadInt32(&client.sliceCalls) != 2 || atomic.LoadInt32(&client.createCalls) != 1 {
		t.Fatalf("slice=%d create=%d", client.sliceCalls, client.createCalls)
	}
	if len(client.checksums) != 2 {
		t.Fatalf("checksums=%v", client.checksums)
	}
	if len(client.offsets) != 2 || !((client.offsets[0] == 0 && client.offsets[1] == 4) || (client.offsets[0] == 4 && client.offsets[1] == 0)) {
		t.Fatalf("offsets=%v", client.offsets)
	}
	if got := stats.UploadBytes.Load(); got != 8 {
		t.Fatalf("uploaded bytes=%d", got)
	}
	if len(stats.ActiveTasks()) != 0 {
		t.Fatal("upload task not ended")
	}
	if progress, err := ListProgress(dir); err != nil || len(progress) != 0 {
		t.Fatalf("progress=%v err=%v, want cleaned", progress, err)
	}
}

// 目的：上传各阶段失败必须返回原始错误，且 success-only 的合并请求不得发生。
func TestUploadSession_SliceUploadErrors(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "data.tmp")
	if err := os.WriteFile(staging, []byte("abcdefgh"), 0600); err != nil {
		t.Fatal(err)
	}
	rapidMiss := func() *uploadRecorder {
		c := newUploadRecorder()
		c.rapidErr = mount.ErrNeedUpload
		c.precreateID = "uid"
		return c
	}

	t.Run("precreate error", func(t *testing.T) {
		client := rapidMiss()
		client.precreateErr = os.ErrPermission
		u := NewUploadSession(client, NewStagingManager(dir, true))
		u.SetSliceSize(4)
		if _, err := u.Upload(context.Background(), staging, "/f", 8); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("precreate rapid success", func(t *testing.T) {
		client := rapidMiss()
		client.precreateID = ""
		u := NewUploadSession(client, NewStagingManager(dir, true))
		u.SetSliceSize(4)
		res, err := u.Upload(context.Background(), staging, "/f", 8)
		if err != nil || res.Size != 8 {
			t.Fatalf("res=%+v err=%v", res, err)
		}
		if atomic.LoadInt32(&client.sliceCalls) != 0 {
			t.Fatal("slices must not upload after server-side rapid success")
		}
	})

	t.Run("slice error", func(t *testing.T) {
		client := rapidMiss()
		client.sliceErr = os.ErrNotExist
		u := NewUploadSession(client, NewStagingManager(dir, true))
		u.SetSliceSize(4)
		u.maxRetries = 0 // 避免指数退避拖慢用例
		if _, err := u.Upload(context.Background(), staging, "/f", 8); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err=%v", err)
		}
		if atomic.LoadInt32(&client.createCalls) != 0 {
			t.Fatal("merge must not run after slice failure")
		}
	})

	t.Run("merge error", func(t *testing.T) {
		client := rapidMiss()
		client.createErr = os.ErrExist
		u := NewUploadSession(client, NewStagingManager(dir, true))
		u.SetSliceSize(4)
		if _, err := u.Upload(context.Background(), staging, "/f", 8); !errors.Is(err, os.ErrExist) {
			t.Fatalf("err=%v", err)
		}
	})
}

// 目的：失败清单必须支持新增/去重替换/删除/损坏拒绝覆盖，并兼容旧版 pipe 格式。
func TestFailedUploadManifest_Lifecycle(t *testing.T) {
	dir := t.TempDir()
	if entries, err := LoadFailedUploads(dir); err != nil || entries != nil {
		t.Fatalf("missing manifest => %v %v", entries, err)
	}

	oldLocal := filepath.Join(dir, "old.tmp")
	if err := os.WriteFile(oldLocal, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SaveFailedUpload(dir, FailedUploadEntry{RemotePath: "/a", LocalPath: oldLocal, Size: 3, Error: "first"}); err != nil {
		t.Fatal(err)
	}
	newLocal := filepath.Join(dir, "new.tmp")
	if err := os.WriteFile(newLocal, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	// 同一远端路径应替换旧记录，并删除旧 staging，避免 recover 重传旧内容。
	if err := SaveFailedUpload(dir, FailedUploadEntry{RemotePath: "/a", LocalPath: newLocal, Size: 4, Error: "second"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldLocal); !os.IsNotExist(err) {
		t.Fatalf("old staging not removed: %v", err)
	}
	if err := SaveFailedUpload(dir, FailedUploadEntry{RemotePath: "/b", LocalPath: newLocal, Size: 5}); err != nil {
		t.Fatal(err)
	}
	entries, err := LoadFailedUploads(dir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
	if entries[0].RemotePath != "/a" || entries[0].Size != 4 || entries[0].Error != "second" {
		t.Fatalf("replace failed: %+v", entries[0])
	}

	if err := RemoveFailedUpload(dir, "/a"); err != nil {
		t.Fatal(err)
	}
	if entries, _ = LoadFailedUploads(dir); len(entries) != 1 || entries[0].RemotePath != "/b" {
		t.Fatalf("after remove: %+v", entries)
	}
	if err := RemoveFailedUpload(dir, "/b"); err != nil {
		t.Fatal(err)
	}
	if entries, _ = LoadFailedUploads(dir); len(entries) != 0 {
		t.Fatalf("manifest should be removed: %+v", entries)
	}
	// 删除不存在的记录是 no-op。
	if err := RemoveFailedUpload(dir, "/missing"); err != nil {
		t.Fatal(err)
	}
}

// 目的：损坏清单必须拒绝覆盖/重写，防止丢失失败上传记录。
func TestFailedUploadManifest_CorruptRefusal(t *testing.T) {
	dir := t.TempDir()
	manifestDir := filepath.Join(dir, "failed-uploads")
	if err := os.MkdirAll(manifestDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(manifestDir, "manifest.json")
	corrupt := []byte("legacy-line-without-pipe")
	if err := os.WriteFile(path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if err := SaveFailedUpload(dir, FailedUploadEntry{RemotePath: "/a"}); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("err=%v", err)
	}
	if data, _ := os.ReadFile(path); string(data) != string(corrupt) {
		t.Fatalf("corrupt manifest overwritten: %q", data)
	}
	if _, err := LoadFailedUploads(dir); err == nil {
		t.Fatal("Load should report corrupt manifest")
	}
	if err := RemoveFailedUpload(dir, "/a"); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("Remove err=%v", err)
	}
}

// 目的：兼容旧版 pipe 清单、JSON 数组与空文件，非法行必须明确报错。
func TestParseFailedManifest_Formats(t *testing.T) {
	entries, err := parseFailedManifest([]byte("  \n\t "))
	if err != nil || entries != nil {
		t.Fatalf("empty => %+v %v", entries, err)
	}

	entries, err = parseFailedManifest([]byte("/r|/l|12|boom\n/r2|/l2"))
	if err != nil || len(entries) != 2 {
		t.Fatalf("legacy => %+v %v", entries, err)
	}
	if entries[0].Size != 12 || entries[0].Error != "boom" || entries[1].Size != 0 {
		t.Fatalf("legacy fields: %+v", entries)
	}

	if _, err := parseFailedManifest([]byte("/r|/l|not-a-size")); err == nil {
		t.Fatal("invalid legacy size should fail")
	}

	raw, _ := json.Marshal([]FailedUploadEntry{{RemotePath: "/arr", LocalPath: "/tmp/x"}})
	entries, err = parseFailedManifest(raw)
	if err != nil || len(entries) != 1 || entries[0].RemotePath != "/arr" {
		t.Fatalf("array => %+v %v", entries, err)
	}

	if _, err := parseFailedManifest([]byte("{bad")); err == nil {
		t.Fatal("bad object should fail")
	}
	if _, err := parseFailedManifest([]byte("[bad")); err == nil {
		t.Fatal("bad array should fail")
	}
}

// 目的：进度文件可被 status 读取/更新/清理，并能识别静默超时的疑似卡死上传。
func TestUploadProgress_CRUDAndStuckDetection(t *testing.T) {
	dir := t.TempDir()
	if list, err := ListProgress(dir); err != nil || list != nil {
		t.Fatalf("missing dir => %v %v", list, err)
	}

	old := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
	fresh := time.Now().Format(time.RFC3339)
	entries := []UploadProgress{
		{Path: "/old", UploadID: "u1", TotalSize: 100, TotalSlices: 10, StartedAt: old, LastActivityAt: old},
		{Path: "/fresh", UploadID: "u2", TotalSize: 10, TotalSlices: 1, StartedAt: fresh, LastActivityAt: fresh},
		{Path: "/fallback", UploadID: "u3", StartedAt: old, LastActivityAt: "invalid-time"},
	}
	for i := range entries {
		if err := WriteProgress(dir, &entries[i]); err != nil {
			t.Fatal(err)
		}
	}

	list, err := ListProgress(dir)
	if err != nil || len(list) != 3 {
		t.Fatalf("list=%+v err=%v", list, err)
	}

	if err := UpdateProgress(dir, "/fresh", 1, 10, 123.5); err != nil {
		t.Fatal(err)
	}
	list, _ = ListProgress(dir)
	var found *UploadProgress
	for i := range list {
		if list[i].Path == "/fresh" {
			found = &list[i]
		}
	}
	if found == nil || found.UploadedSlices != 1 || found.UploadedBytes != 10 || found.SpeedBps != 123.5 {
		t.Fatalf("updated progress=%+v", found)
	}
	// 不存在的进度文件更新应返回错误（调用方据此忽略）。
	if err := UpdateProgress(dir, "/missing", 1, 1, 0); err == nil {
		t.Fatal("expected error for missing progress")
	}

	// 非 .json、目录、坏 JSON 都应被忽略而不是让整个 status 失败。
	progressDir := progressDir(dir)
	if err := os.Mkdir(filepath.Join(progressDir, "subdir"), 0700); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(progressDir, "note.txt"), []byte("x"), 0600)
	_ = os.WriteFile(filepath.Join(progressDir, "bad.json"), []byte("{bad"), 0600)
	if list, err := ListProgress(dir); err != nil || len(list) != 3 {
		t.Fatalf("garbage entries should be skipped: %d %v", len(list), err)
	}

	stuck, err := ListStuckUploads(dir)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	for _, p := range stuck {
		paths[p.Path] = true
	}
	if !paths["/old"] || !paths["/fallback"] || paths["/fresh"] {
		t.Fatalf("stuck=%v", paths)
	}

	RemoveProgress(dir, "/old")
	if list, _ := ListProgress(dir); len(list) != 2 {
		t.Fatalf("RemoveProgress failed: %d", len(list))
	}
}
