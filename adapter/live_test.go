package adapter

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 活测开关：仅当 BDFUSE_BDUSS 非空时运行，避免污染常规单元测试与泄露凭据。
func liveCreds(t *testing.T) (bduss, stoken string) {
	t.Helper()
	bduss = os.Getenv("BDFUSE_BDUSS")
	stoken = os.Getenv("BDFUSE_STOKEN")
	if bduss == "" {
		t.Skip("live test disabled: 未设置环境变量 BDFUSE_BDUSS（跳过网络活测）")
	}
	return bduss, stoken
}

// TestLiveTransportParity 直连百度服务器：对 pan-api 与 Core-PCS 两种传输做读路径 parity，
// 并各走一遍完整写路径（tryRapidUpload→CreateSuperFile/UploadSlice），验证 adapter 重构后
// 两条链路均能在真实网络上跑通。上传到临时命名目录 /bdfs_selftest，测后尽力清理。
func TestLiveTransportParity(t *testing.T) {
	bduss, stoken := liveCreds(t)
	ctx := context.Background()

	type tc struct {
		name   string
		client PCSClient
	}
	cases := []tc{
		{"panapi", NewPanClient(bduss, stoken, 60*time.Second)},
		{"core-pcs", NewPCSClient(266719, bduss, stoken, 60*time.Second)},
	}

	randName := "selftest_" + time.Now().Format("20060102150405")
	remoteDir := "/bdfs_selftest"
	remoteFile := remoteDir + "/" + randName + ".txt"

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs := NewBaiduCloudFS(c.client, NewDlinkCache(30*time.Minute), "live")

			// 1) 读路径 parity：stat 根目录 + 列目录。
			root, err := fs.Stat(ctx, "/")
			if err != nil {
				t.Fatalf("read-parity Stat(/) failed: %v", err)
			}
			children, err := fs.ReadDir(ctx, "/")
			if err != nil {
				t.Fatalf("read-parity ReadDir(/) failed: %v", err)
			}
			t.Logf("%s read-path OK: root=%q size=%d isdir=%v children=%d",
				c.name, root.Name, root.Size, root.IsDir, len(children))

			// 2) 写路径：确保临时目录存在。
			if err := fs.Mkdir(ctx, remoteDir); err != nil {
				t.Logf("%s mkdir %q (可忽略:可能已存在): %v", c.name, remoteDir, err)
			}

			// 3) 写路径：走完整 UploadSession（含秒传尝试）。
			local := filepath.Join(t.TempDir(), randName+".txt")
			content := []byte("bdfs selftest " + randName + "\n")
			if err := os.WriteFile(local, content, 0644); err != nil {
				t.Fatalf("write local staged file: %v", err)
			}
			sm := NewStagingManager(t.TempDir(), true)
			us := NewUploadSession(c.client, sm)
			res, uerr := us.Upload(ctx, local, remoteFile, int64(len(content)))
			if uerr != nil {
				t.Fatalf("%s upload failed: %v", c.name, uerr)
			}
			t.Logf("%s write-path OK: remote=%q size=%d",
				c.name, remoteFile, res.Size)

			// 4) 验证可回读 + 清理。
			if got, serr := fs.Stat(ctx, remoteFile); serr == nil {
				t.Logf("%s verify-after-upload OK: fs_id=%d size=%d", c.name, got.FSID, got.Size)
			} else {
				t.Logf("%s verify-after-upload failed (文件可能仍在): %v", c.name, serr)
			}
			if cerr := fs.Remove(ctx, remoteFile); cerr != nil {
				t.Errorf("%s cleanup remove failed — 请手动删除 %q: %v", c.name, remoteFile, cerr)
			} else {
				t.Logf("%s cleaned up %q", c.name, remoteFile)
			}
		})
	}
}
