package config

import (
	"os"
	"path/filepath"
	"time"
)

// MountOptions 挂载配置，flags 对齐 pcsconfig 现有配置。
type MountOptions struct {
	MountPoint  string // 本地挂载点路径
	RemotePath  string // 网盘远程路径，默认 "/"
	EnableWrite bool   // 启用写支持（Phase 5）
	AllowOther  bool   // 允许其他用户访问（需 /etc/fuse.conf user_allow_other）

	CacheDir string // 缓存目录，默认 ~/.cache/baidupcs-fuse

	ReadBlockSize   int64         // 读缓存块大小，默认 32MB（VIP 优化）
	UploadSliceSize int64         // 上传分片大小，默认 8MB（VIP 优化）
	MetadataTTL     time.Duration // 文件 stat 缓存 TTL，默认 30s
	DirTTL          time.Duration // 目录列表缓存 TTL，默认 24h
	DlinkTTL        time.Duration // dlink 缓存 TTL，默认 30min

	MaxParallel int // 全局最大并发下载/上传数，默认 10（VIP 优化）
	PerFileSem  int // 单文件最大并发下载块数（默认1=串行；>1启用单文件并行，需配合全局 max-parallel）

	BlockCacheSize int64         // 读缓存总容量，默认 512MB（VIP 优化）
	CurlTimeout    time.Duration // curl 单次下载超时，默认 60s
	HttpTimeout    time.Duration // HTTP 客户端超时，默认 120s

	DirRefreshInterval time.Duration // 后台目录刷新间隔，默认 24h（0=禁用）

	DiskCacheSize int64 // 磁盘缓存总容量，默认 10GB（0=不限）

	AccountKey string // 账户标识，用于 cache key 前缀
	BDUSS      string // 百度 BDUSS（CDN 下载需要 Cookie 认证）
}

// DefaultOptions 返回默认配置（VIP 优化）。
func DefaultOptions() MountOptions {
	home, _ := os.UserHomeDir()
	return MountOptions{
		RemotePath:         "/",
		CacheDir:           filepath.Join(home, ".cache", "baidupcs-fuse"),
		ReadBlockSize:      32 * 1024 * 1024,
		UploadSliceSize:    8 * 1024 * 1024,
		MetadataTTL:        30 * time.Second,
		DirTTL:             24 * time.Hour,
		DlinkTTL:           30 * time.Minute,
		MaxParallel:        10,
		PerFileSem:         1,
		BlockCacheSize:     512 * 1024 * 1024,
		CurlTimeout:        60 * time.Second,
		HttpTimeout:        120 * time.Second,
		DirRefreshInterval: 24 * time.Hour,
		DiskCacheSize:      10 * 1024 * 1024 * 1024, // 10GB
	}
}
