# 并发读写模式 Spec

**状态**: ✅ 已完成（v1.1）
**日期**: 2026-09-19 创建，2026-09-20 完成

## 目标
v1 SingleThreaded → v2 MultiThreaded，支持并发读写。

## 审计结果（已完成）

### ✅ 已线程安全（有 mutex）
| 组件 | 锁类型 | 文件 |
|------|--------|------|
| BlockCache | RWMutex | cache/block.go |
| MetadataCache | RWMutex | cache/metadata.go |
| Streak | Mutex | cache/streak.go |
| DlinkCache | RWMutex | adapter/baidu.go |
| quotaCache | RWMutex | fuse/filesystem.go |
| inodeManager | RWMutex | fuse/filesystem.go |
| Scheduler.activeCmds | Mutex | download/scheduler.go |
| StagingManager.sessions | RWMutex | adapter/staging.go |
| fileNode.entry | RWMutex | fuse/filesystem.go |
| fileNode.mu | RWMutex | fuse/filesystem.go |

### ✅ 架构问题（已解决）
| 问题 | 解决方案 |
|------|---------|
| per-inode reader/writer | → per-handle（readFileHandle/writeFileHandle） |
| SingleThreaded: true | → MultiThreaded（SingleThreaded: false） |
| fileNode.writeRefs 无锁 | → 不再需要（per-handle 无共享状态） |

## 实现记录

### Phase 1: 基础设施 ✅
- [x] StagingManager 加 `sync.RWMutex`
- [x] fileNode.entry 改为 `sync.RWMutex` 保护
- [x] 验证所有现有测试通过

### Phase 2: Per-Handle 状态 ✅
```go
// 最终实现
type readFileHandle struct {
    node   *fileNode
    reader mount.RemoteReader
    streak *cache.Streak
}

type writeFileHandle struct {
    node   *fileNode
    writer mount.RemoteWriter
    reader mount.RemoteReader  // 写洞回源用（per-handle，不共享）
}
```
- Open 返回 FileHandle → Read/Write 通过 FileHandle 分发 → 无竞争
- 同一文件多 fd 并发读：每个 fd 独立 reader + streak

### Phase 3: MultiThreaded FUSE ✅
```go
fsOpts := &gofuse.Options{
    MountOptions: fuse.MountOptions{
        SingleThreaded: false,  // 启用并发
        MaxWrite:      1 << 20,
    },
}
```

### Phase 4: 并发写保护 ✅
- 同一文件多 fd 写：EBUSY（Open 时检查 `len(n.writers) > 0`）
- 不同文件并发写：无竞争（per-handle staging）
- flush/close 上传：per-handle dirty flag

### Phase 5: FUSE 接口签名修复 ✅
- `readFileHandle.Read`: 去掉 `f gofuse.FileHandle` 参数（匹配 `FileReader`）
- `writeFileHandle.Read`: 同上
- `writeFileHandle.Write`: 去掉 `f gofuse.FileHandle` 参数（匹配 `FileWriter`）
- `fileNode.Fsync`: `datasync bool` → `flags uint32`（匹配 `NodeFsyncer`）
- 32 个编译期接口检查防止签名变更回归

## 验收标准
- [x] `go test -race ./...` 全过（104 tests）
- [x] 两个终端同时 `cat` 不同文件不阻塞
- [x] 一个文件读 + 另一个文件写不阻塞
- [x] 同一文件并发写返回 EBUSY
- [x] Ctrl+C 仍能正常退出

## 并发模型

```
┌─────────────────────────────────────────────┐
│  FUSE Kernel                                │
│  (MultiThreaded, 并发分发 Read/Write)        │
└──────────┬──────────────────────────────────┘
           │
    ┌──────▼──────┐
    │ rawBridge   │  go-fuse 类型断言分发
    │ .Read()     │  FileReader / NodeReader
    │ .Write()    │  FileWriter / NodeWriter
    └──────┬──────┘
           │
    ┌──────▼──────────────────────────┐
    │ Per-Handle State                │
    │ readFileHandle: reader + streak │
    │ writeFileHandle: writer + reader│
    └──────┬──────────────────────────┘
           │
    ┌──────▼──────┐
    │ BlockCache  │  LRU, per-account, RWMutex
    │ DiskCache   │  LRU, manifest 持久化
    │ MetadataCache│ RWMutex
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │ Scheduler   │  globalSem + per-fileSem
    │ curl 两步    │  PCS redirect → CDN download
    └─────────────┘
```
