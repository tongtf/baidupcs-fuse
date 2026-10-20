package adapter

import (
	"net/http"
	"testing"
)

func TestExtractSToken_RawCookieValueNotTruncated(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Add("Set-Cookie", "STOKEN=abcdef123456; Path=/; Domain=baidu.com")
	if got := extractSToken(resp); got != "abcdef123456" {
		t.Fatalf("extractSToken=%q, want abcdef123456", got)
	}
}

func TestExtractSToken_CookieNameBoundary(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{"prefix-other-cookie", "XSTOKEN=bad; STOKEN=good; Path=/", "good"},
		{"cookie-not-first", "BDUSS=x; STOKEN=good; Path=/", "good"},
		{"empty-value", "STOKEN=; Path=/", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{Header: http.Header{}}
			resp.Header.Add("Set-Cookie", tt.header)
			if got := extractSToken(resp); got != tt.want {
				t.Fatalf("extractSToken(%q)=%q, want %q", tt.header, got, tt.want)
			}
		})
	}
}
