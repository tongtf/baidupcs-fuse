package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCreds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds")
	content := "# 注释行\nbduss=BDUSS_VALUE\n  STOKEN = stoken_value \n\n无关行\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	b, s, err := loadCreds(path)
	if err != nil {
		t.Fatalf("loadCreds: %v", err)
	}
	if b != "BDUSS_VALUE" {
		t.Errorf("bduss = %q, want BDUSS_VALUE", b)
	}
	if s != "stoken_value" {
		t.Errorf("stoken = %q, want stoken_value（需容错首尾空格与大小写键）", s)
	}
}

func TestLoadCredsFromFile(t *testing.T) {
	if b, s := loadCredsFromFile(nil); b != "" || s != "" {
		t.Errorf("nil env-file 应返回空，got bduss=%q stoken=%q", b, s)
	}
	b, s := loadCredsFromFile(newStrPtr("/nonexistent/creds"))
	if b != "" || s != "" {
		t.Errorf("不存在的文件应返回空且不打断启动，got bduss=%q stoken=%q", b, s)
	}
}

func newStrPtr(s string) *string { return &s }
