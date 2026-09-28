package download

import "errors"

var (
	// ErrCDNForbidden HTTP 403 — CDN 限流/dlink 失效（D1）。
	ErrCDNForbidden = errors.New("cdn forbidden (403)")

	// ErrRangeNotSatisfiable HTTP 416 — Range 不满足（D1）。
	ErrRangeNotSatisfiable = errors.New("range not satisfiable (416)")

	// ErrShortRead 块拉取返回字节数 ≠ 请求 size（AUDIT-001）。
	ErrShortRead = errors.New("block fetch returned fewer bytes than requested")

	// ErrShuttingDown Server 正在关闭，拒绝新下载请求。
	ErrShuttingDown = errors.New("scheduler shutting down")
)
