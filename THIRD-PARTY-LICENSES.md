# Third-Party Licenses & Attribution

baidupcs-fuse is distributed under the [Apache License 2.0](LICENSE). It links
against several third-party libraries. Their respective licenses are noted
below; full license texts live in each module's repository (and in the Go
module cache under `$GOPATH/pkg/mod`).

## Core dependency

| Library | Version | License | Copyright |
|---|---|---|---|
| [qjfoidnh/BaiduPCS-Go](https://github.com/qjfoidnh/BaiduPCS-Go) | v4.0.x | Apache-2.0 | BaiduPCS-Go contributors |

`adapter/pcscore` is the only place in this project that imports
`github.com/qjfoidnh/BaiduPCS-Go/baidupcs`, so the above attribution covers the
entire upstream dependency surface (per ADR-0001). At build time BaiduPCS-Go is
resolved as a network dependency via go.mod's `require`, with no local source
copy needed; see `README.md`.

## Runtime dependencies

| Library | Version | License | Copyright |
|---|---|---|---|
| hanwen/go-fuse/v2 | v2.5.1 | BSD-2-Clause | Han Wen |
| cloudflare/circl | v1.3.6 | Apache-2.0 | Cloudflare, Inc. |
| quic-go/quic-go | v0.37.4 | BSD-3-Clause | The QUIC Go Authors |
| bogdanfinn/utls | v1.6.1 | MIT | Bogdan Finik |
| bogdanfinn/fhttp | v0.5.27 | MIT | Bogdan Finik (fork of valyala/fasthttp, BSD-2) |
| bogdanfinn/tls-client | v1.7.2 | MIT | Bogdan Finik |
| klauspost/compress | v1.16.7 | MIT | Klaus Post |
| andybalholm/brotli | v1.0.5 | BSD-3-Clause | The Brotlly Authors / Google |
| tam7t/hpkp | v0.0.0-20160821 | MIT | Tam7t |
| golang.org/x/crypto | v0.14.0 | BSD-3-Clause | The Go Project Authors |
| golang.org/x/net | v0.17.0 | BSD-3-Clause | The Go Project Authors |
| golang.org/x/sys | v0.13.0 | BSD-3-Clause | The Go Project Authors |
| golang.org/x/text | v0.13.0 | BSD-3-Clause | The Go Project Authors |

## Notes

- Apache License 2.0 §4(d)/§5 require preservation of attribution notices; the
  notices above are provided in good faith. If a holder or license differs from
  what is listed here, please open an issue and we will correct it.
- baidupcs-fuse itself does not redistribute any of the above source; these
  are compiled Go module dependencies resolved at build time.
