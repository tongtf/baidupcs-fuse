package config

import (
	"path/filepath"
	"testing"
	"time"
)

// 目的：默认配置是 CLI 未显式传参时的生产契约，防止容量/TTL/并发默认值漂移。
func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.RemotePath != "/" {
		t.Errorf("RemotePath = %q, want /", opts.RemotePath)
	}
	if opts.CacheDir == "" || !filepath.IsAbs(opts.CacheDir) {
		t.Errorf("CacheDir = %q, want absolute path", opts.CacheDir)
	}
	if filepath.Base(opts.CacheDir) != "baidupcs-fuse" {
		t.Errorf("CacheDir base = %q, want baidupcs-fuse", filepath.Base(opts.CacheDir))
	}

	int64s := []struct {
		name string
		got  int64
		want int64
	}{
		{"ReadBlockSize", opts.ReadBlockSize, 32 * 1024 * 1024},
		{"UploadSliceSize", opts.UploadSliceSize, 8 * 1024 * 1024},
		{"BlockCacheSize", opts.BlockCacheSize, 512 * 1024 * 1024},
		{"DiskCacheSize", opts.DiskCacheSize, 10 * 1024 * 1024 * 1024},
	}
	for _, tt := range int64s {
		if tt.got != tt.want {
			t.Errorf("%s = %d, want %d", tt.name, tt.got, tt.want)
		}
	}

	durations := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"MetadataTTL", opts.MetadataTTL, 30 * time.Second},
		{"DirTTL", opts.DirTTL, 5 * time.Minute},
		{"DlinkTTL", opts.DlinkTTL, 30 * time.Minute},
		{"CurlTimeout", opts.CurlTimeout, 60 * time.Second},
		{"HttpTimeout", opts.HttpTimeout, 120 * time.Second},
		{"CommitTimeout", opts.CommitTimeout, 30 * time.Minute},
		{"DirRefreshInterval", opts.DirRefreshInterval, 24 * time.Hour},
	}
	for _, tt := range durations {
		if tt.got != tt.want {
			t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.want)
		}
	}

	ints := []struct {
		name string
		got  int
		want int
	}{
		{"MaxParallel", opts.MaxParallel, 10},
		{"MaxUploadParallel", opts.MaxUploadParallel, 2},
		{"PerFileSem", opts.PerFileSem, 1},
	}
	for _, tt := range ints {
		if tt.got != tt.want {
			t.Errorf("%s = %d, want %d", tt.name, tt.got, tt.want)
		}
	}

	if opts.EnableWrite || opts.AllowOther || opts.AsyncClose {
		t.Errorf("bool defaults should be false: enableWrite=%v allowOther=%v asyncClose=%v",
			opts.EnableWrite, opts.AllowOther, opts.AsyncClose)
	}
	if opts.AccountKey != "" || opts.BDUSS != "" {
		t.Errorf("credentials should default empty: key=%q bduss=%q", opts.AccountKey, opts.BDUSS)
	}
}
