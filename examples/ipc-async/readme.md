# 异步 IPC 示例 — Manager-Worker 进程间通信（异步版）

## 概述

本示例演示了基于标准输入输出（stdin/stdout）的行分隔 JSON 协议实现的**异步**进程间通信模式。与同步版（`examples/ipc-example/`）的核心区别在于：Manager 发送命令后立即返回，不等待回复；Worker 通过**独立的后台协程**处理写入，同时支持**主动上报状态**。这种非阻塞的通信模式使得 Manager 可以同时管控多个 Worker，而不会被任何一个 Worker 的处理时间阻塞。

本示例包含两个 Go 程序：
- `manager/main.go`：管理端，启动 Worker，发送命令，接收响应（包括 Worker 主动上报）
- `worker/main.go`：子进程，处理命令，定期主动上报状态

## 核心设计差异

### 异步 vs 同步的对比

在同步 IPC 中，每次通信都是一个完整的往返：

```
Manager:   Send   →  [等待响应...阻塞...]  →  得到回复   →  发送下一条
时间线:     ──────┬────────────────────────┬────────────┬─────────▶
                发送                     收到回复     发送下一条
```

在异步 IPC 中，发送和接收是分离的：

```
Manager:   Send   →  立即返回   →  发送下一条  →  处理后台收到的回复
时间线:     ──────┬────────────┬────────────┬─────────────────────▶
                 发送        发送下一条    后台 goroutine 处理回复
```

这种分离带来了以下优势：

1. **不阻塞**：Manager 的主流程不会被 Worker 的处理时间影响。即使 Worker 需要 10 秒处理一条命令，Manager 也可以在这 10 秒内做其他事情或发送更多命令。

2. **并发控制多个 Worker**：Manager 可以启动多个 Worker，所有 Worker 的响应都汇入同一个 recv channel，Manager 只需一个 goroutine 就能处理所有响应。

3. **反向推送**：Worker 可以在没有任何 Manager 请求的情况下主动推送数据（如状态报告、心跳、告警等）。这是同步模型无法天然支持的。

4. **消息吞吐量**：在高延迟场景下（如 Worker 在处理 sleep 命令时），异步模型可以保持 Manager 的持续工作。

## 通信架构

### Manager 端架构

Manager 端使用以下架构实现异步通信：

```go
type WorkerAsync struct {
    cmd    *exec.Cmd          // 子进程句柄
    stdin  io.WriteCloser     // 子进程的标准输入（Manager 写入）
    enc    *json.Encoder      // JSON 编码器（写入 stdin）
    recv   chan Response      // 所有收到的响应（包括主动上报）都送到这里
}
```

关键组件：

**`recv chan Response`**：这是一个带缓冲的 channel（容量 64），是所有来自 Worker 的响应和主动上报的统一入口。Manager 可以从这个 channel 中阻塞读取（`Recv()`）或非阻塞批量读取（`RecvAll()`）。

**后台读取 goroutine**：Worker 启动时，Manager 启动一个独立的 goroutine，通过 `bufio.Scanner` 持续读取 Worker 的 stdout：

```go
go func() {
    scanner := bufio.NewScanner(stdout)
    scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
    for scanner.Scan() {
        var resp Response
        if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
            continue  // 跳过无效 JSON（可能是日志误写入 stdout）
        }
        w.recv <- resp  // 所有解析成功的响应都送入 channel
    }
}()
```

这个 goroutine 的生命周期与 Worker 子进程绑定：当 Worker 退出后，stdout 管道的读取端收到 EOF，`scanner.Scan()` 返回 false，goroutine 退出。

### Worker 端架构

Worker 端使用两个独立的 goroutine 实现读写分离：

```
标准输入 goroutine:
    for scanner.Scan() {
        → 解析 JSON Command
        → switch cmd.Cmd 处理
        → enc.Encode(Response)  写入 stdout
    }

主动上报 goroutine:
    for {
        time.Sleep(2 * time.Second)
        → enc.Encode(Response{
            Ok: true,
            ID: "report",
            Data: {
                uptime_sec: <当前运行时长>,
                req_count:  <已处理请求数>,
                cpu_percent: 3.2,
            },
        })
    }
```

这种设计的关键在于：两个 goroutine 共享同一个 `json.Encoder`（写入同一个 stdout 管道），Go 的 `json.Encoder` 内部使用 `sync.Mutex` 保证并发安全，因此不会出现数据竞态。

### 消息路由

由于异步模型中多个 Worker 的响应可能交错到达，Manager 需要一种方式来关联请求和响应。本示例使用 `Command.ID` 和 `Response.ID` 字段来实现：

- Manager 发送命令时可以设置一个唯一的 `ID`
- Worker 在回复时将该 `ID` 原样设置在 `Response.ID` 中
- Manager 的接收端根据 `ID` 判断是哪个请求的回复
- 主动上报的消息使用固定的 `ID: "report"`，Manager 据此区分命令回复和主动上报

## 主动上报机制

### 上报内容

Worker 每 2 秒主动上报一次系统状态，上报内容包括：

```json
{
    "ok": true,
    "id": "report",
    "data": {
        "uptime_sec": 10,
        "req_count": 3,
        "cpu_percent": 3.2
    }
}
```

- **uptime_sec**：Worker 已经运行的秒数
- **req_count**：Worker 已经处理的请求总数（来自原子计数器）
- **cpu_percent**：模拟的 CPU 使用率（本示例中为固定值 3.2，实际实现中可以调用系统 API 获取真实值）

### 上报价值

主动上报机制的价值在于：

1. **不需要轮询**：Manager 不需要定期发送状态查询命令，Worker 自动推送状态信息。减少了不必要的网络/管道通信。

2. **实时性**：状态信息在上报时点即时到达 Manager，延迟仅取决于上报间隔（本示例为 2 秒）。

3. **解耦**：Manager 不需要知道 Worker 应该上报什么内容、多长时间上报一次。Worker 可以独立决定上报周期和内容。

## API 设计

### 发送命令

```go
func (w *WorkerAsync) Send(cmd Command) error
```

发送命令是纯异步的：`json.NewEncoder.Encode()` 将命令序列化为 JSON 写入 stdin 管道后立即返回。如果 Worker 暂时无法读取（管道缓冲区满），`Encode()` 会阻塞直到数据被写入管道缓冲区。但对于正常运行的 Worker，这种情况很少发生。

### 接收响应

```go
// 阻塞等待，支持超时
func (w *WorkerAsync) Recv(timeout time.Duration) (*Response, error)

// 非阻塞批量读取所有未读响应
func (w *WorkerAsync) RecvAll() []Response
```

`Recv()` 的行为：
- `timeout <= 0`：无限期阻塞直到收到响应
- `timeout > 0`：在指定时间内等待，超时返回 error

`RecvAll()` 的行为：
- 尝试从 `recv` channel 中非阻塞读取所有消息
- 如果 channel 中没有消息，返回空 slice
- 如果 channel 中有多条消息，一次性全部读取

### 使用模式

典型的使用模式是主 goroutine 负责发送命令，一个独立的 goroutine 负责处理收到的所有响应：

```go
// 后台处理响应
go func() {
    for resp := range worker.recv {
        if resp.ID == "report" {
            fmt.Printf("[上报] %+v\n", resp.Data)
        } else {
            fmt.Printf("[回复] id=%s ok=%v\n", resp.ID, resp.Ok)
        }
    }
}()

// 主 goroutine 只管发送
worker.Send(Command{Cmd: "ping"})
worker.Send(Command{Cmd: "echo", Data: "hello"})
worker.Send(Command{Cmd: "sleep", Data: "1s"})
```

## 同步 vs 异步的详细比较

### 代码复杂度

异步版本的代码比同步版本更复杂，主要体现在：

1. **需要管理 channel**：Manager 需要创建和关闭 recv channel，需要决定是使用有缓冲还是无缓冲 channel。

2. **需要处理并发**：发送和接收在不同的 goroutine 中执行，需要考虑并发安全。

3. **需要消息关联**：由于响应可能乱序到达，需要 ID 来关联请求和响应。

4. **需要超时机制**：如果 Worker 崩溃，Manager 的 `Recv()` 可能会永久阻塞。需要实现超时或心跳检测机制。

### 适用场景

**异步版本更适合**：
- 需要同时管控多个子进程的管理工具
- 需要实时监控子进程状态的场景
- 子进程需要主动上报数据（指标、日志等）
- 管理端不能因为某个 Worker 处理慢而阻塞

**同步版本更适合**：
- 严格顺序控制的配置下发
- 事务性操作（必须等前一步完成才能继续）
- 简单的请求-响应，不需要并发
- 代码可读性和简单性优先

### 错误处理差异

在同步模型中，如果 Worker 崩溃，Manager 的 `Send()` 方法会在尝试写入已关闭的管道时失败，或者 `scanner.Scan()` 返回 false（EOF），Manager 能够立即检测到异常。

在异步模型中，如果 Worker 崩溃，Manager 的 `Send()` 可能成功将最后一条命令写入管道缓冲区，但 Worker 永远不会处理它。Manager 只能通过以下方式检测：
1. `Recv()` 超时（说明 Worker 在指定时间内没有回复）
2. 在后台读取 goroutine 中检查 `scanner.Scan()` 的返回值（EOF 表示 Worker 已退出）
3. 独立的健康检查协程定期发送 `ping` 命令验证 Worker 存活

当前实现中，后台读取 goroutine 会在 Worker 退出时检测到 `scanner.Err()` 并打印错误日志，但 Manager 的主流程不会收到通知。这是一个有意为之的设计决策 — 保持 API 简单，让用户自行决定如何检测 Worker 故障。

## 进程生命周期

### Worker 启动

```go
func StartWorkerAsync(exe string, args ...string) (*WorkerAsync, error) {
    cmd := exec.Command(exe, args...)
    stdin, _ := cmd.StdinPipe()
    stdout, _ := cmd.StdoutPipe()
    cmd.Stderr = os.Stderr
    cmd.Start()

    w := &WorkerAsync{
        cmd:   cmd,
        stdin: stdin,
        enc:   json.NewEncoder(stdin),
        recv:  make(chan Response, 64),
    }

    go func() {
        scanner := bufio.NewScanner(stdout)
        // ...循环读 stdout，送入 recv channel
    }()

    return w, nil
}
```

启动过程与同步版类似，但多了两个关键差异：
1. `recv` channel 的缓冲区大小为 64，这意味着 Manager 可以累积最多 64 条未处理的响应而不阻塞 Worker 的写入。
2. 后台读取 goroutine 在 `StartWorkerAsync` 内部启动，Manager 不需要手动管理。

### Worker 停止

```go
func (w *WorkerAsync) Close() {
    w.stdin.Close()  // 关闭 stdin → Worker 的 Scan() 返回 false
    w.cmd.Wait()     // 等待子进程退出
}
```

停止逻辑与同步版相同。关闭 stdin 后，Worker 的 stdin goroutine 中的 `scanner.Scan()` 返回 false，goroutine 退出。之后 Worker 的主动上报 goroutine 仍然在运行，但由于 stdout 管道也即将关闭，`enc.Encode()` 会失败。最终 Worker 进程退出。Manager 的 `cmd.Wait()` 返回。

需要注意的是，关闭过程不是瞬时的。如果 Worker 正在 sleep 期间收到了 EOF，sleep 仍然会继续执行，直到循环回到 `scanner.Scan()` 时才退出。如果需要在关闭时中断正在执行的操作，需要使用 context 或信号机制。

## 通信协议

与同步版相同的 JSON 行协议，但增加了主动上报的消息类型：

### Manager 发送

```json
{"cmd":"ping","id":"","data":""}
{"cmd":"echo","id":"req-001","data":"hello"}
{"cmd":"sleep","id":"req-002","data":"1s"}
{"cmd":"stop","id":"","data":""}
```

### Worker 回复

```json
{"ok":true,"id":"","data":"pong"}
{"ok":true,"id":"req-001","data":"hello"}
{"ok":true,"id":"req-002","data":"slept 1s"}
{"ok":true,"id":"","data":"bye"}
```

### Worker 主动上报

```json
{"ok":true,"id":"report","data":{"uptime_sec":2,"req_count":1,"cpu_percent":3.2}}
{"ok":true,"id":"report","data":{"uptime_sec":4,"req_count":2,"cpu_percent":3.2}}
```

主动上报使用 `"id":"report"` 来标识自己不是命令回复。Manager 通过检查 `resp.ID == "report"` 来区分这两类消息。

## 线程安全设计

### Manager 端

Manager 端的 `Send()` 方法直接调用 `enc.Encode()`，而 `json.Encoder` 在 Go 标准库中被设计为线程安全的（内部持有 `sync.Mutex`）。因此 Manager 可以在多个 goroutine 中同时调用 `Send()`。

`recv` channel 是 goroutine 安全的（channel 本身就是并发安全的）。多个 goroutine 可以同时从 channel 中读取，但本示例中通常只有一个后台 goroutine 往 channel 中写入，一个主 goroutine 从 channel 中读取。

### Worker 端

Worker 端的两个 goroutine 共享同一个 `enc *json.Encoder`。`json.Encoder.Encode()` 是线程安全的，因此不会出现数据竞态。

原子计数器 `reqID` 使用 `sync/atomic.Int64` 实现，在多个 goroutine 中调用 `Load()` 和 `Add()` 是安全的。

## 代码结构

```
examples/ipc-async/
├── readme.md              # 本文档
├── manager/
│   └── main.go            # Manager 管理端
│       ├── Command        # 命令数据结构
│       ├── Response       # 响应数据结构
│       ├── WorkerAsync    # 异步 Worker 封装
│       │   ├── StartWorkerAsync  # 启动 Worker
│       │   ├── Send              # 异步发送命令
│       │   ├── Recv              # 阻塞接收响应
│       │   ├── RecvAll           # 非阻塞批量接收
│       │   └── Close             # 关闭 Worker
│       └── main           # 演示流程
└── worker/
    └── main.go            # Worker 子进程
        ├── Command        # 命令数据结构
        ├── Response       # 响应数据结构
        ├── 读 goroutine   # 处理 stdin 命令
        │   ├── ping → pong
        │   ├── echo → 原样返回
        │   ├── sleep → 等待后回复
        │   ├── count → 返回请求计数
        │   └── stop → 退出
        └── 写 goroutine   # 主动上报状态
            └── 每 2 秒上报一次系统状态
```
