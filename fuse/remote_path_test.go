package fuse

import (
	"context"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
)

// TestRemotePath_FollowsInodeRename: 节点创建时缓存的 path 会过时；
// rename 后必须沿内核 inode 树重新解析，否则改名后的目录/文件访问旧路径。
func TestRemotePath_FollowsInodeRename(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	ctx := newContext(opts, newMockFS())
	root := &BaiduFS{fs: ctx, rootPath: "/remote"}
	_ = gofuse.NewNodeFS(root, &gofuse.Options{})
	ctx.rootInode = root.EmbeddedInode()
	ctx.rootRemote = "/remote"

	dir := &dirNode{fs: ctx, path: "/remote/old"}
	dirInode := root.NewInode(context.Background(), dir, gofuse.StableAttr{Ino: 2, Mode: syscall.S_IFDIR})
	if !root.AddChild("old", dirInode, false) {
		t.Fatal("AddChild old failed")
	}

	file := &fileNode{fs: ctx, path: "/remote/old/file"}
	fileInode := root.NewInode(context.Background(), file, gofuse.StableAttr{Ino: 3, Mode: syscall.S_IFREG})
	if !dirInode.AddChild("file", fileInode, false) {
		t.Fatal("AddChild file failed")
	}

	if got := dir.remotePath(); got != "/remote/old" {
		t.Fatalf("dir before rename = %q", got)
	}
	if got := file.remotePath(); got != "/remote/old/file" {
		t.Fatalf("file before rename = %q", got)
	}

	if !root.MvChild("old", root.EmbeddedInode(), "new", false) {
		t.Fatal("MvChild failed")
	}
	if got := dir.remotePath(); got != "/remote/new" {
		t.Fatalf("dir after rename = %q, want /remote/new", got)
	}
	if got := file.remotePath(); got != "/remote/new/file" {
		t.Fatalf("file after rename = %q, want /remote/new/file", got)
	}
}

// TestRemotePath_UnlinkedFileIsNotLinked: unlink 后 inode 成为孤立节点，
// 写路径必须知道它已不在目录树中，不能按旧路径重建文件。
func TestRemotePath_UnlinkedFileIsNotLinked(t *testing.T) {
	opts := defaultOpts()
	opts.CacheDir = t.TempDir()
	ctx := newContext(opts, newMockFS())
	root := &BaiduFS{fs: ctx, rootPath: "/remote"}
	_ = gofuse.NewNodeFS(root, &gofuse.Options{})
	ctx.rootInode = root.EmbeddedInode()
	ctx.rootRemote = "/remote"

	file := &fileNode{fs: ctx, path: "/remote/old"}
	fileInode := root.NewInode(context.Background(), file, gofuse.StableAttr{Ino: 2, Mode: syscall.S_IFREG})
	if !root.AddChild("old", fileInode, false) {
		t.Fatal("AddChild failed")
	}
	p, linked := file.remotePathStatus()
	if !linked || p != "/remote/old" {
		t.Fatalf("before unlink=(%q,%v), want /remote/old,true", p, linked)
	}
	if ok, _ := root.RmChild("old"); !ok {
		t.Fatal("RmChild failed")
	}
	p, linked = file.remotePathStatus()
	if linked {
		t.Fatalf("after unlink linked=true path=%q, want false", p)
	}
	if p != "/remote/old" {
		t.Fatalf("fallback path=%q, want /remote/old", p)
	}
}
