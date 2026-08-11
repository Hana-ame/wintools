# Examples — 示例与辅助库

本目录包含项目的示例程序和辅助库，按用途组织为子目录：

## 目录结构

```
examples/
├── readme.md              # 本文档
├── ech-test/              # ECH 域前置手动测试工具
├── ipc-example/           # 同步 IPC 示例（stdin/stdout JSON 行协议）
├── ipc-async/             # 异步 IPC 示例（支持 Worker 主动上报）
├── peer-client/           # WebRTC P2P 聊天客户端
└── webrtc/                # WebRTC Peer/Listener 封装库
```

## 使用方式

每个子目录中的程序都可以直接使用 `go run` 运行：

```bash
# ECH 测试
go run ./examples/ech-test/

# 同步 IPC（需要同时运行 manager 和 worker）
go run ./examples/ipc-example/manager/
# 或在另一个终端中单独运行 worker
go run ./examples/ipc-example/worker/

# 异步 IPC
go run ./examples/ipc-async/manager/

# WebRTC P2P 聊天（需要信令服务器）
go run ./examples/peer-client/ --mode p1
```

## 设计原则

1. **零额外依赖**：每个示例尽量只依赖 Go 标准库和项目内的核心包，避免引入额外的第三方依赖
2. **独立可运行**：每个示例都能独立编译运行，不需要特定环境配置
3. **完整文档**：每个子目录都有 `readme.md` 详细说明设计思路、架构和使用方法
4. **非生产代码**：示例代码侧重教学和演示，不追求生产环境的鲁棒性
