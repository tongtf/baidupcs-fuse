package adapter

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"baidupcs-fuse/mount"
)

// defaultHTTPTimeout 是客户端无超时兜底值（httpTimeout<=0 时生效）。
// panClient 默认用裸 &http.Client{Jar: jar}，缺省此值会让 stalled TCP 连接把 FUSE op 永远卡在 request_wait_answer。
const defaultHTTPTimeout = 60 * time.Second

func normalizeHTTPTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultHTTPTimeout
	}
	return d
}

// panClient Pan API 直连客户端（仅需 BDUSS）。
// Pan API 域名：pan.baidu.com，不依赖 STOKEN。
type panClient struct {
	bduss    string
	stoken   string // 由 stokenMu 保护（STOKEN 自动刷新会更新）
	stokenMu sync.RWMutex
	client   *http.Client
	ua       string
	fetcher  FetchFunc // 注入 Scheduler.FetchRange（可选，nil 时降级为直连）
	uid      uint64    // 百度 UID（locatedownload 签名需要）
	uidMu    sync.RWMutex
	uidAt    time.Time
	uidErr   error
	panBase  string // 测试可覆盖；空=生产域名
	pcsBase  string
}

func (c *panClient) panBaseURL() string {
	if c.panBase != "" {
		return c.panBase
	}
	return "https://pan.baidu.com"
}

func (c *panClient) pcsBaseURL() string {
	if c.pcsBase != "" {
		return c.pcsBase
	}
	return "https://pcs.baidu.com"
}

// NewPanClient 创建 Pan API 客户端。stoken 可选（空则只用 Pan API）。
func NewPanClient(bduss, stoken string, httpTimeout time.Duration) PCSClient {
	httpTimeout = normalizeHTTPTimeout(httpTimeout)
	jar, _ := cookiejar.New(nil)
	// 标准 cookiejar 仅用于 get/post 辅助方法
	// precreate/upload 等需要手动设置 Cookie header（百度 API 域名匹配特殊）
	panURL, _ := url.Parse("https://pan.baidu.com")
	pcsURL, _ := url.Parse("https://pcs.baidu.com")
	dpcsURL, _ := url.Parse("https://d.pcs.baidu.com")
	cookies := []*http.Cookie{
		{Name: "BDUSS", Value: bduss},
	}
	if stoken != "" {
		cookies = append(cookies, &http.Cookie{Name: "STOKEN", Value: stoken})
	}
	jar.SetCookies(panURL, cookies)
	jar.SetCookies(pcsURL, cookies)
	jar.SetCookies(dpcsURL, cookies)

	c := &panClient{
		bduss:  bduss,
		stoken: stoken,
		client: &http.Client{Jar: jar, Timeout: httpTimeout},
		ua:     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}

	// UID 异步获取：失败不阻塞挂载；locatedownload 首次使用时再懒重试。
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if uid, err := c.fetchUIDContext(ctx); err == nil {
			c.uidMu.Lock()
			c.uid, c.uidAt, c.uidErr = uid, time.Now(), nil
			c.uidMu.Unlock()
			log.Printf("panClient: uid=%d", uid)
		} else {
			c.uidMu.Lock()
			c.uidAt, c.uidErr = time.Now(), err
			c.uidMu.Unlock()
			log.Printf("panClient: UID 异步获取失败（locatedownload 将按需重试）: %v", err)
		}
	}()

	return c
}

// getStoken 返回当前 STOKEN（线程安全）。
func (c *panClient) getStoken() string {
	c.stokenMu.RLock()
	defer c.stokenMu.RUnlock()
	return c.stoken
}

// UpdateStoken 更新 STOKEN（供 StokenRefresher 回调）。
func (c *panClient) UpdateStoken(newToken string) {
	c.stokenMu.Lock()
	c.stoken = newToken
	c.stokenMu.Unlock()
	log.Printf("panClient: STOKEN 已更新")
}

// SetBaseURL 用于测试覆盖基础 URL。
func (c *panClient) SetBaseURL(url string) {
	c.panBase = url
	c.pcsBase = url
}

// ============================================================
// UID 获取 + locatedownload 签名
// ============================================================

// fetchUIDContext 通过 pan API 获取百度 UID（locatedownload 签名需要）。
func (c *panClient) fetchUIDContext(ctx context.Context) (uint64, error) {
	apiURL := c.panBaseURL() + "/api/gettemplatevariable?clienttype=0&app_id=250528&fields=%5B%22uk%22%5D"
	resp, err := c.get(ctx, apiURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Errno  int `json:"errno"`
		Result struct {
			UK uint64 `json:"uk"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode uid: %w", err)
	}
	if result.Errno != 0 {
		return 0, &mount.BaiduError{Errno: result.Errno, Method: "gettemplatevariable"}
	}
	if result.Result.UK == 0 {
		return 0, fmt.Errorf("uid is zero")
	}
	return result.Result.UK, nil
}

// getUID 返回已缓存 UID；没有则做一次短超时懒加载（30s 内失败不重复打 API）。
func (c *panClient) getUID(ctx context.Context) (uint64, error) {
	c.uidMu.RLock()
	uid, at, lastErr := c.uid, c.uidAt, c.uidErr
	c.uidMu.RUnlock()
	if uid != 0 {
		return uid, nil
	}
	if !at.IsZero() && time.Since(at) < 30*time.Second {
		if lastErr != nil {
			return 0, lastErr
		}
		return 0, fmt.Errorf("uid not ready")
	}

	lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	uid, err := c.fetchUIDContext(lctx)
	c.uidMu.Lock()
	if err == nil {
		c.uid, c.uidAt, c.uidErr = uid, time.Now(), nil
	} else if ctx.Err() == nil {
		// 调用方取消导致的失败不缓存：否则 30s 内所有新请求都会被旧的
		// context.Canceled 挡住，locatedownload 会持续失败。
		c.uidAt, c.uidErr = time.Now(), err
	}
	c.uidMu.Unlock()
	if err != nil {
		return 0, err
	}
	return uid, nil
}

// devUID 计算设备标识：MD5(BDUSS) + "|0"（大写）。
func devUID(bduss string) string {
	h := md5.Sum([]byte(bduss))
	s := hex.EncodeToString(h[:]) + "|0"
	return strings.ToUpper(s)
}

// locateDownloadSign 计算 locatedownload 签名参数。
// 算法：SHA1(SHA1(BDUSS) + UID + magic + timestamp + devUID)
func locateDownloadSign(uid uint64, bduss string) (timestamp, rand, devUIDStr string) {
	timestamp = strconv.FormatInt(time.Now().Unix(), 10)
	devUIDStr = devUID(bduss)

	// SHA1(BDUSS)
	bdussHash := sha1.Sum([]byte(bduss))

	// SHA1(SHA1(BDUSS) + UID + magic + timestamp + devUID)
	randHash := sha1.New()
	randHash.Write(bdussHash[:])
	randHash.Write([]byte(strconv.FormatUint(uid, 10)))
	// 魔术字节（BaiduPCS-Go netdisksign 包固定值）
	randHash.Write([]byte{
		0x65, 0x62, 0x72, 0x63, 0x55, 0x59, 0x69, 0x75,
		0x78, 0x61, 0x5a, 0x76, 0x32, 0x58, 0x47, 0x75,
		0x37, 0x4b, 0x49, 0x59, 0x4b, 0x78, 0x55, 0x72,
		0x71, 0x66, 0x6e, 0x4f, 0x66, 0x70, 0x44, 0x46,
	})
	randHash.Write([]byte(timestamp))
	randHash.Write([]byte(devUIDStr))
	rand = hex.EncodeToString(randHash.Sum(nil))

	return
}

// getDlinkLocateDownload 使用 PCS API locatedownload 获取下载链接（带网盘签名）。
// 替代原来的 filemetas dlink（CDN 403）。
func (c *panClient) getDlinkLocateDownload(ctx context.Context, path string) (string, error) {
	uid, err := c.getUID(ctx)
	if err != nil {
		return "", fmt.Errorf("UID not available: %w", err)
	}

	timestamp, randVal, devUIDStr := locateDownloadSign(uid, c.bduss)

	apiURL := fmt.Sprintf(
		"%s/rest/2.0/pcs/file?ant=1&check_blue=1&es=1&esl=1&app_id=250528&method=locatedownload&path=%s&ver=4.0&clienttype=17&channel=0&apn_id=1_0&freeisp=0&queryfree=0&use=0&time=%s&rand=%s&devuid=%s&cuid=%s",
		c.pcsBaseURL(),
		url.QueryEscape(path),
		timestamp,
		randVal,
		devUIDStr,
		devUIDStr,
	)

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, nil)
	if err != nil {
		return "", err
	}
	// BaiduPCS-Go 使用 netdisk 客户端 UA（浏览器 UA 会被拒绝）
	req.Header.Set("User-Agent", "netdisk;P2SP;3.0.0.8;netdisk;11.12.3;ANG-AN00;android-android;10.0;JSbridge4.4.0;jointBridge;1.1.0;")
	req.Header.Set("Cookie", "BDUSS="+c.bduss)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("locatedownload HTTP %d: %s", resp.StatusCode, string(body[:minInt(len(body), 200)]))
	}

	var result struct {
		Errno int `json:"errno"`
		Infos []struct {
			URL     string `json:"url"`
			Encrypt int    `json:"encrypt"`
		} `json:"urls"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("decode locatedownload: %w (body=%s)", err, string(body[:minInt(len(body), 200)]))
	}
	if result.Errno != 0 {
		return "", &mount.BaiduError{Errno: result.Errno, Method: "locatedownload"}
	}
	if len(result.Infos) == 0 {
		return "", fmt.Errorf("locatedownload: no URLs returned")
	}

	// 选非加密链接
	for _, info := range result.Infos {
		if info.Encrypt == 0 && info.URL != "" {
			return info.URL, nil
		}
	}
	return "", fmt.Errorf("locatedownload: all URLs encrypted")
}

// getDlinkDownload 返回 PCS method=download URL。
// Scheduler 的 doFetchCDN 用 curl -L 跟随重定向直接下载（CDN URL 是一次性的，不可复用）。
func (c *panClient) getDlinkDownload(ctx context.Context, path string) (string, error) {
	// 只构造 PCS method=download URL，不在取链阶段额外发一次 curl。
	// Scheduler 的第一次 GET 会解析 302 Location；若 403 由 remoteReader.Reload 刷新。
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	apiURL := fmt.Sprintf(
		"%s/rest/2.0/pcs/file?app_id=250528&method=download&path=%s",
		c.pcsBaseURL(), url.QueryEscape(path),
	)
	return apiURL, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ============================================================
// Pan API 适配 — Stat / ReadDir / LocateDownload / QuotaInfo
// ============================================================

func (c *panClient) Stat(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	if path == "/" || path == "" {
		return &mount.RemoteEntry{FSID: 0, Path: "/", Name: "/", IsDir: true, Mode: 0755}, nil
	}
	// 优先 filemetas 按路径直查（1 次请求）；不可用时回退父目录分页 list。
	if e, err := c.statByFilemetas(ctx, path); err == nil && e != nil {
		return e, nil
	}
	return c.statByList(ctx, path)
}

func (c *panClient) statByFilemetas(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	target, _ := json.Marshal([]string{path})
	apiURL := fmt.Sprintf("%s/api/filemetas?target=%s&dlink=0", c.panBaseURL(), url.QueryEscape(string(target)))
	resp, err := c.get(ctx, apiURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Errno int `json:"errno"`
		Info  []struct {
			FsID           int64  `json:"fs_id"`
			ServerFilename string `json:"server_filename"`
			Path           string `json:"path"`
			Isdir          int    `json:"isdir"`
			Size           int64  `json:"size"`
			ServerMtime    int64  `json:"server_mtime"`
		} `json:"info"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if result.Errno != 0 {
		return nil, &mount.BaiduError{Errno: result.Errno, Method: "filemetas"}
	}
	if len(result.Info) == 0 {
		return nil, os.ErrNotExist
	}
	it := result.Info[0]
	mode := uint32(0644)
	if it.Isdir == 1 {
		mode = 0755
	}
	return &mount.RemoteEntry{
		FSID:     it.FsID,
		Path:     it.Path,
		Name:     it.ServerFilename,
		IsDir:    it.Isdir == 1,
		Size:     it.Size,
		MtimeSec: it.ServerMtime,
		Mode:     mode,
	}, nil
}

func (c *panClient) ReadDir(ctx context.Context, path string) ([]*mount.RemoteEntry, error) {
	const pageSize = 1000
	const maxEntries = 200000

	entries := make([]*mount.RemoteEntry, 0, pageSize)
	seen := make(map[int64]struct{}, pageSize)
	for start := 0; start < maxEntries; start += pageSize {
		apiURL := fmt.Sprintf(
			"%s/api/list?dir=%s&order=name&start=%d&limit=%d",
			c.panBaseURL(), url.QueryEscape(path), start, pageSize,
		)
		resp, err := c.get(ctx, apiURL)
		if err != nil {
			return nil, err
		}

		var result struct {
			Errno int `json:"errno"`
			List  []struct {
				FsID           int64  `json:"fs_id"`
				ServerFilename string `json:"server_filename"`
				Path           string `json:"path"`
				Isdir          int    `json:"isdir"`
				Size           int64  `json:"size"`
				ServerMtime    int64  `json:"server_mtime"`
			} `json:"list"`
		}
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("decode list response: %w", err)
		}
		if result.Errno != 0 {
			return nil, &mount.BaiduError{Errno: result.Errno, Method: "list"}
		}

		added := 0
		for _, item := range result.List {
			if item.FsID != 0 {
				if _, dup := seen[item.FsID]; dup {
					continue
				}
				seen[item.FsID] = struct{}{}
			}
			added++
			mode := uint32(0644)
			if item.Isdir == 1 {
				mode = 0755
			}
			entries = append(entries, &mount.RemoteEntry{
				FSID:     item.FsID,
				Path:     item.Path,
				Name:     item.ServerFilename,
				IsDir:    item.Isdir == 1,
				Size:     item.Size,
				MtimeSec: item.ServerMtime,
				Mode:     mode,
			})
		}
		if len(result.List) < pageSize || added == 0 {
			// added==0 防御服务端忽略 start 参数导致同一页被反复返回。
			break
		}
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

func (c *panClient) LocateDownload(ctx context.Context, path string) (dlink string, fsID int64, size int64, mtimeSec int64, err error) {
	entry, err := c.Stat(ctx, path)
	if err != nil {
		return "", 0, 0, 0, err
	}
	dlink, err = c.getDlinkLocateDownload(ctx, path)
	if err != nil {
		return "", 0, 0, 0, err
	}
	return dlink, entry.FSID, entry.Size, entry.MtimeSec, nil
}

func (c *panClient) QuotaInfo(ctx context.Context) (total, used int64, err error) {
	// Pan API: /api/quota?checkexpire=1
	apiURL := c.panBaseURL() + "/api/quota?checkexpire=1"
	resp, err := c.get(ctx, apiURL)
	if err != nil {
		// 降级：返回合理默认值（不影响 df 显示，只是不准）
		return 5 * 1024 * 1024 * 1024 * 1024, 0, nil
	}
	defer resp.Body.Close()

	var result struct {
		Errno int `json:"errno"`
		Quota struct {
			Total int64 `json:"total"`
			Used  int64 `json:"used"`
		} `json:"quota"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 5 * 1024 * 1024 * 1024 * 1024, 0, nil
	}
	if result.Errno != 0 {
		// 权限不足等非致命错误，返回默认值
		return 5 * 1024 * 1024 * 1024 * 1024, 0, nil
	}
	return result.Quota.Total, result.Quota.Used, nil
}

func (c *panClient) Mkdir(ctx context.Context, path string) error {
	// PCS API: method=mkdir（app_id=266719 是 PCS 专用，250528 会报 "pcs token not exist"）
	apiURL := fmt.Sprintf(
		"%s/rest/2.0/pcs/file?app_id=266719&method=mkdir&path=%s",
		c.pcsBaseURL(), url.QueryEscape(path),
	)
	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", c.ua)
	cookieVal := "BDUSS=" + c.bduss
	if st := c.getStoken(); st != "" {
		cookieVal += "; STOKEN=" + st
	}
	req.Header.Set("Cookie", cookieVal)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("mkdir HTTP %d: %s", resp.StatusCode, string(body[:minInt(len(body), 200)]))
	}

	var result struct {
		Errno int `json:"errno"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("decode mkdir response: %w, body: %s", err, string(body[:minInt(len(body), 200)]))
	}
	if result.Errno != 0 {
		be := &mount.BaiduError{Errno: result.Errno, Method: "mkdir"}
		if mount.BaiduErrorToErrno(be) == syscall.EEXIST {
			return nil
		}
		return be
	}
	return nil
}

func (c *panClient) Remove(ctx context.Context, paths ...string) error {
	// PCS API: method=delete（Pan API filemanager?op=delete 返回 errno=2）
	type pathItem struct {
		Path string `json:"path"`
	}
	type paramJSON struct {
		List []*pathItem `json:"list"`
	}
	list := make([]*pathItem, len(paths))
	for i, p := range paths {
		list[i] = &pathItem{Path: p}
	}
	paramData, _ := json.Marshal(&paramJSON{List: list})

	apiURL := c.pcsBaseURL() + "/rest/2.0/pcs/file?app_id=266719&method=delete"

	// multipart/form-data（BaiduPCS-Go 方式）
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	writer.WriteField("param", string(paramData))
	writer.Close()

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("User-Agent", c.ua)
	cookieVal := "BDUSS=" + c.bduss
	if st := c.getStoken(); st != "" {
		cookieVal += "; STOKEN=" + st
	}
	req.Header.Set("Cookie", cookieVal)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("remove HTTP %d: %s", resp.StatusCode, string(respBody[:minInt(len(respBody), 200)]))
	}

	var result struct {
		Errno int `json:"errno"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("decode remove response: %w, body: %s", err, string(respBody[:minInt(len(respBody), 200)]))
	}
	if result.Errno != 0 {
		be := &mount.BaiduError{Errno: result.Errno, Method: "remove"}
		if mount.BaiduErrorToErrno(be) == syscall.ENOENT {
			return nil
		}
		return be
	}
	return nil
}

func (c *panClient) Rename(ctx context.Context, from, to string) error {
	// Pan API: /api/filemanager?op=rename
	apiURL := c.panBaseURL() + "/api/filemanager?op=rename"
	fileList := fmt.Sprintf("[[%q,%q]]", from, to)
	body := "filelist=" + url.QueryEscape(fileList)
	resp, err := c.post(ctx, apiURL, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result struct {
		Errno int `json:"errno"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode rename response: %w", err)
	}
	if result.Errno != 0 {
		return &mount.BaiduError{Errno: result.Errno, Method: "rename"}
	}
	return nil
}

// ============================================================
// 写路径 — Pan API 实现
// 参考 BaiduPCS-Go baidupcs/upload.go + prepare.go
// ============================================================

func (c *panClient) RapidUpload(ctx context.Context, remotePath string, size int64, contentMD5, sliceMD5, dataContent, policy, uploadID string, blockListMD5 []string) error {
	blockListJSON := mergeBlockList(blockListMD5...)
	rtype := policyToRtype(policy)

	post := fmt.Sprintf(
		"path=%s&size=%d&isdir=0&autoinit=1&rtype=%s&uploadid=%s&content-md5=%s&slice-md5=%s&block_list=%s&mode=1",
		url.QueryEscape(remotePath), size, rtype, url.QueryEscape(uploadID),
		contentMD5, sliceMD5, url.QueryEscape(blockListJSON),
	)
	if uploadID == "" {
		post = fmt.Sprintf(
			"path=%s&size=%d&isdir=0&autoinit=1&rtype=%s&content-md5=%s&slice-md5=%s&block_list=%s&mode=1",
			url.QueryEscape(remotePath), size, rtype,
			contentMD5, sliceMD5, url.QueryEscape(blockListJSON),
		)
	}

	resp, err := c.post(ctx, c.panBaseURL()+"/api/precreate", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result struct {
		Errno      int    `json:"errno"`
		ReturnType int    `json:"return_type"` // 1=上传, 2=秒传
		UploadID   string `json:"uploadid"`
		Info       struct {
			FsID int64 `json:"fs_id"`
		} `json:"info"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode precreate response: %w", err)
	}
	if result.Errno != 0 {
		return &mount.BaiduError{Errno: result.Errno, Method: "rapid_upload"}
	}
	if result.ReturnType != 2 {
		return mount.ErrNeedUpload
	}
	// ReturnType == 2 表示秒传成功
	return nil
}

func (c *panClient) PrecreateUpload(ctx context.Context, remotePath string, size int64, blockListMD5 []string) (string, error) {
	blockListJSON := mergeBlockList(blockListMD5...)
	// rtype=2 覆盖同名文件
	post := fmt.Sprintf(
		"path=%s&size=%d&isdir=0&autoinit=1&rtype=2&block_list=%s",
		url.QueryEscape(remotePath), size, url.QueryEscape(blockListJSON),
	)

	resp, err := c.post(ctx, c.panBaseURL()+"/api/precreate", post)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var result struct {
		Errno      int    `json:"errno"`
		ReturnType int    `json:"return_type"` // 1=上传, 2=秒传
		UploadID   string `json:"uploadid"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("decode precreate response: %w, body: %s", err, string(respBody))
	}
	if result.Errno != 0 {
		return "", &mount.BaiduError{Errno: result.Errno, Method: "precreate"}
	}
	if result.ReturnType == 2 {
		// 服务端判定秒传成功（常见于 RapidUpload 瞬时失败但内容已存在）。
		// 返回空 uploadID 表示文件已创建，调用方不要再上传分片。
		return "", nil
	}
	if result.UploadID == "" {
		return "", fmt.Errorf("precreate returned empty uploadid")
	}
	return result.UploadID, nil
}

func (c *panClient) UploadSlice(ctx context.Context, uploadID, remotePath string, seq int, offset int64, data []byte) (string, error) {
	// 计算 data 的 MD5 仅用于调试和幂等性确认，生产日志不逐片打印。
	dataMD5 := md5.Sum(data)
	if os.Getenv("BDFS_DEBUG") != "" {
		log.Printf("UploadSlice: uploadID=%s seq=%d offset=%d size=%d md5=%x",
			uploadID, seq, offset, len(data), dataMD5)
	}

	// PCS API 分片上传：pcs.baidu.com/rest/2.0/pcs/superfile2
	// 幂等性：uploadid+partseq 唯一标识一个分片，重复上传会覆盖写
	uploadURL := fmt.Sprintf(
		"%s/rest/2.0/pcs/superfile2?app_id=250528&method=upload&type=tmpfile&path=%s&uploadid=%s&partseq=%d&partoffset=%d&vip=1",
		c.pcsBaseURL(), url.QueryEscape(remotePath), url.QueryEscape(uploadID), seq, offset,
	)

	body, contentType := buildMultipartBody(data)
	req, err := http.NewRequestWithContext(ctx, "POST", uploadURL, body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", c.ua)
	cookieVal := "BDUSS=" + c.bduss
	if st := c.getStoken(); st != "" {
		cookieVal += "; STOKEN=" + st
	}
	req.Header.Set("Cookie", cookieVal)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		// 尝试解析 Baidu JSON errno
		var apiErr struct {
			Errno int    `json:"errno"`
			Msg   string `json:"errmsg"`
		}
		if json.Unmarshal(respBody, &apiErr) == nil && apiErr.Errno != 0 {
			return "", &mount.BaiduError{Errno: apiErr.Errno, Method: "upload_slice", Msg: apiErr.Msg}
		}
		return "", fmt.Errorf("upload slice HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Errno int    `json:"error_code"`
		MD5   string `json:"md5"`
		Error string `json:"error_msg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}
	if result.Errno != 0 {
		return "", &mount.BaiduError{Errno: result.Errno, Method: "upload_slice", Msg: result.Error}
	}
	if result.MD5 == "" {
		return "", fmt.Errorf("upload slice: no md5 returned")
	}
	if want := hex.EncodeToString(dataMD5[:]); !strings.EqualFold(result.MD5, want) {
		return "", fmt.Errorf("upload slice md5 mismatch: got %s, want %s", result.MD5, want)
	}
	return result.MD5, nil
}

func (c *panClient) CreateSuperFile(ctx context.Context, uploadID, remotePath string, size int64, checksumMap map[int]string) error {
	blockList := sortBlockList(checksumMap)
	blockListJSON := mergeBlockList(blockList...)

	targetDir := parentPath(remotePath)
	if targetDir != "/" {
		targetDir += "/"
	}

	post := fmt.Sprintf(
		"uploadid=%s&path=%s&size=%d&isdir=0&rtype=2&block_list=%s&target_path=%s",
		url.QueryEscape(uploadID), url.QueryEscape(remotePath), size,
		url.QueryEscape(blockListJSON), url.QueryEscape(targetDir),
	)

	resp, err := c.post(ctx, c.panBaseURL()+"/api/create", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	var result struct {
		Errno int   `json:"errno"`
		FsID  int64 `json:"fs_id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("decode create response: %w, body: %s", err, string(respBody))
	}
	if result.Errno != 0 {
		return &mount.BaiduError{Errno: result.Errno, Method: "create"}
	}
	return nil
}

// ============================================================
// 内部辅助方法
// ============================================================

// mergeBlockList 将 blockList 序列化为 JSON 数组字符串
func mergeBlockList(blocks ...string) string {
	if len(blocks) == 0 {
		return "[]"
	}
	result := "["
	for i, b := range blocks {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("%q", b)
	}
	result += "]"
	return result
}

// policyToRtype 将 policy 字符串转为 rtype 参数
// overwrite=2, skip=1, rename=3
func policyToRtype(policy string) string {
	switch policy {
	case "overwrite", "rsync":
		return "2"
	case "skip":
		return "1"
	case "rename":
		return "3"
	default:
		return "2"
	}
}

// sortBlockList 按 seq 排序 blockList
func sortBlockList(checksumMap map[int]string) []string {
	if len(checksumMap) == 0 {
		return nil
	}
	maxSeq := 0
	for seq := range checksumMap {
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	result := make([]string, maxSeq+1)
	for seq, md5 := range checksumMap {
		result[seq] = md5
	}
	return result
}

// buildMultipartBody 构建 multipart/form-data 请求体。
// 使用随机 boundary，避免固定 boundary 恰好出现在二进制分片数据中导致服务端解析错位。
func buildMultipartBody(data []byte) (io.Reader, string) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", "blob")
	if err != nil {
		return bytes.NewReader(nil), w.FormDataContentType()
	}
	_, _ = part.Write(data)
	_ = w.Close()
	return bytes.NewReader(buf.Bytes()), w.FormDataContentType()
}

// statByList 通过 list API 获取文件元信息。
func (c *panClient) statByList(ctx context.Context, path string) (*mount.RemoteEntry, error) {
	// 对于根目录
	if path == "/" || path == "" {
		return &mount.RemoteEntry{
			FSID: 0, Path: "/", Name: "/",
			IsDir: true, Size: 0, Mode: 0755,
		}, nil
	}

	// 获取父目录
	parent := parentPath(path)
	name := baseName(path)

	entries, err := c.ReadDir(ctx, parent)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		if e.Name == name {
			return e, nil
		}
	}
	return nil, os.ErrNotExist
}

func (c *panClient) get(ctx context.Context, urlStr string) (*http.Response, error) {
	return c.doWithRetry(ctx, http.MethodGet, urlStr, "", false)
}

func (c *panClient) post(ctx context.Context, urlStr, body string) (*http.Response, error) {
	return c.doWithRetry(ctx, http.MethodPost, urlStr, body, true)
}

// doWithRetry 对元数据 GET/POST 做有限重试：仅网络错误、429、5xx；
// 4xx（除 429）立即返回，避免把鉴权/参数错误放大成重试风暴。
func (c *panClient) doWithRetry(ctx context.Context, method, urlStr, body string, form bool) (*http.Response, error) {
	const attempts = 3
	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			delay := time.Duration(200*(1<<uint(attempt-1)))*time.Millisecond +
				time.Duration(rand.Int63n(int64(150*time.Millisecond)))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		var rdr io.Reader
		if form {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, urlStr, rdr)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", c.ua)
		if form {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		// 手动注入 Cookie（百度 API 域名匹配特殊，标准 cookiejar 不可靠）
		cookieVal := "BDUSS=" + c.bduss
		if st := c.getStoken(); st != "" {
			cookieVal += "; STOKEN=" + st
		}
		req.Header.Set("Cookie", cookieVal)

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}

		respBody, _ := io.ReadAll(resp.Body)
		retryAfter := resp.Header.Get("Retry-After")
		resp.Body.Close()

		var apiErr struct {
			Errno int    `json:"errno"`
			Msg   string `json:"errmsg"`
		}
		if json.Unmarshal(respBody, &apiErr) == nil && apiErr.Errno != 0 {
			lastErr = &mount.BaiduError{Errno: apiErr.Errno, Method: "api_" + strings.ToLower(method), Msg: apiErr.Msg}
		} else {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody[:minInt(len(respBody), 200)]))
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			if retryAfter != "" {
				if secs, perr := strconv.Atoi(retryAfter); perr == nil && secs > 0 && secs <= 10 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(time.Duration(secs) * time.Second):
					}
				}
			}
			continue
		}
		return nil, lastErr
	}
	return nil, lastErr
}

// ============================================================

func baseName(path string) string {
	path = strings.TrimRight(path, "/")
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}
