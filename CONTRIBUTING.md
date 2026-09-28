# 参与贡献

感谢为 baidupcs-fuse 贡献力量!本仓库采用 **Apache-2.0** 协议,无论你是修复 bug、补充测试还是改进文档,都欢迎。

## 开发环境

- Go 1.23+
- Linux / macOS(FUSE 运行时)
- 网络可访问模块代理(国内 `export GOPROXY=https://goproxy.cn,direct`)

首次开发与构建:

```bash
git clone <your-fork-url>
cd baidupcs-fuse
go build -o bdfs ./cmd/bdfs/
```

## 测试

提交前请确保本地单测通过(无需真实账号):

```bash
go test ./... -count=1
# 推荐带竞态检查
go test ./cache/... ./fuse/... ./mount/... ./download/... -count=1 -race
```

涉及网络挂载的端到端测试需真实凭据:

```bash
BDUSS='xxx' REMOTE_PATH=/ ./scripts/e2e-test.sh
```

## 代码规范

- 遵循 `gofmt` / `goimports`;新增/修改文件后运行 `gofmt -w .`。
- 提交前跑 `go vet ./...`,确保编译期接口检查通过。
- 公开接口变更需同步更新 [`specs/`](specs/) 与相关 ADR。
- 不要将凭据(`BDUSS`/`STOKEN`)或编译产物(`bdfs`、`blocks/`)提交入库。

## 提交信息

采用中文 conventional-commit 风格,保持「单一职责」:

```
feat(adapter): 新增 xxx
fix(download): 修复并发调度竞态
test(cache): 补充磁盘缓存越界用例
docs: 更新安装指南
```

`feat/fix/test/docs/refactor/…` 前缀 + 简洁描述;避免 `fix` / `wip` / `asdf` 类无信息量提交。

## 提交流程

1. Fork 本仓库,从 `master` 创建你的分支(`git checkout -b feat/xxx`)。
2. 改动 + 补测试,本地验证通过。
3. 推送并开 Pull Request,填写 PR 模板说明改动与动机。
4. 维护者审查;CI(构建 + 单测)通过后合并。

## 报告问题

发现 bug 请开 Issue 并选择 **Bug report** 模板,附上可复现步骤、`bdfs status` 输出与环境信息。
