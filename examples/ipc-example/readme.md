# 同步 IPC 示例 — Manager-Worker 进程间通信

## 概述

本示例演示了基于标准输入输出（stdin/stdout）的行分隔 JSON 协议实现的同步进程间通信（IPC，Inter-Process Communication）模式。它包含两个 Go 程序：一个 Manager（管理端）和一个 Worker（子进程），两者通过管道（pipe）通信，采用严格的一问一答同步模式。

本项目不依赖任何第三方 IPC 库，完全基于 Go 标准库的 `os/exec`、`encoding/json`、`bufio` 实现。这种设计模式适用于需要强顺序控制的场景，如配置下发、服务启停、远程命令执行等。

## 通信协议

### 协议定义

Manager 和 Worker 之间通过 JSON 行协议通信。每条消息是**一行完整的 JSON 对象**，以换行符 `\n` 分隔。协议建立在双向管道之上：

```
Manager                                         Worker
  │                                               │
  │  ── stdin ──→ {"cmd":"ping","id":"","data":""} │
  │  ← stdout ── {"ok":true,"id":"","data":"pong"} │
  │                                               │
```

### 数据类型

```
Command                                      Response
├── cmd: string    命令名                     ├── ok: bool           是否成功
├── id: string     请求标识（可选）            ├── id: string         请求标识（回显）
└── data: string   命令参数                    ├── data: interface{}  返回数据
                                              └── err: string        错误描述
```

### 支持的命令

| 命令 | 参数 | 行为 | 响应示例 |
|------|------|------|----------|
| `ping` | 无 | 心跳检测，立即回复 pong | `{"ok":true,"data":"pong"}` |
| `echo` | 任意字符串 | 原样返回参数，测试通信是否正常 | `{"ok":true,"data":"Hello World!"}` |
| `sleep` | 持续时间（如 "800ms"） | 模拟耗时操作，等待指定时长后回复 | `{"ok":true,"data":"slept 800ms"}` |
| `count` | 无 | 返回 Worker 已处理的请求总数（从 Worker 启动开始累计） | `{"ok":true,"data":5}` |
| `stop` | 无 | Worker 回复后立即退出 | `{"ok":true,"data":"bye"}` |

### 未知命令处理

当 Manager 发送的 `cmd` 不在上面的支持列表中时，Worker 会回复一个错误：

```json
{"ok":false,"err":"unknown: <cmd>"}
```

这种设计确保 Manager 可以安全地探测 Worker 的能力，或者避免静默吞掉错误命令导致死锁。

## 架构设计

### Manager 端

Manager 的核心是 `Worker` 结构体，它封装了子进程的全部管道操作：

```go
type Worker struct {
    cmd     *exec.Cmd         // 子进程句柄
    stdin   io.WriteCloser    // 子进程的标准输入（Manager 写入）
    scanner *bufio.Scanner    // 子进程的标准输出（Manager 读取）
    enc     *json.Encoder     // JSON 编码器（写入 stdin）
}
```

关键设计决策：

1. **`bufio.Scanner` 而非 `bufio.Reader`**：Scanner 天然按行分割，恰好匹配我们的行分隔 JSON 协议。每次调用 `Scan()` 会阻塞直到读取到完整的换行符，然后通过 `Bytes()` 获取这一行的字节切片。使用 Scanner 避免了手动处理缓冲区和行分割的逻辑。

2. **超大行支持**：默认的 Scanner 缓冲区只有 64KB，对于可能包含大块数据的 JSON 响应（如 WebRTC SDP）是不够的。通过 `scanner.Buffer(make([]byte, 1024*1024), 1024*1024)` 将最大行大小设置为 1MB，缓冲区也是 1MB。

3. **同步阻塞模型**：`Send()` 方法是严格同步的 — 写入命令后必须等待 Worker 回复才能返回。这意味着 Manager 在等待期间不能做其他事情（除非使用协程）。对于需要并发管理多个 Worker 的场景，应为每个 Worker 启动独立的 goroutine。

4. **错误处理**：如果 `scanner.Scan()` 返回 false，表示子进程的 stdout 流已结束（可能子进程已退出）。此时检查 `scanner.Err()` 区分是正常 EOF 还是 I/O 错误。如果返回 `io.EOF` 表示子进程正常退出。

5. **重定向 stderr**：通过 `cmd.Stderr = os.Stderr` 将 Worker 的 stderr 重定向到 Manager 的 stderr，确保 Worker 的日志输出不会污染 JSON 数据流。

### Worker 端

Worker 的实现更加简单：一个主循环不断从 stdin 读取 JSON 命令，处理，然后写入 JSON 响应到 stdout。Worker 的架构可以概括为：

```
main loop:
    scanner.Scan()  →  阻塞等待 stdin 的行
    json.Unmarshal  →  解析 JSON Command
    reqID.Add(1)    →  请求计数器 +1
    switch cmd.Cmd  →  根据命令分发
        ping   →  enc.Encode(Response{Data: "pong"})
        echo   →  enc.Encode(Response{Data: cmd.Data})
        sleep  →  time.Sleep(d); enc.Encode(Response{Data: "slept <d>"})
        count  →  enc.Encode(Response{Data: reqID})
        stop   →  enc.Encode(Response{Data: "bye"}); os.Exit(0)
        default→  enc.Encode(Response{Err: "unknown: <cmd>"})
```

关键实现细节：

1. **原子计数器**：`reqID` 使用 `sync/atomic.Int64` 实现，确保并发安全。每收到一个请求就 +1，用于 `count` 命令返回当前已处理的请求总数。

2. **空行跳过**：如果 Manager 端发送了空行（可能由于管道缓冲等原因），Worker 跳过不处理，避免 JSON 解析错误。

3. **`stop` 命令的立即退出**：`stop` 在回复后调用 `os.Exit(0)` 立即退出，不等待主循环的下一次迭代。这样可以确保 Worker 在收到停止命令后快速释放资源。

4. **JSON 解析错误处理**：如果 Manager 发送的 JSON 格式有误，Worker 回复一个错误响应而不是崩溃。这使得 Manager 可以继续发送下一条命令，而不是整个 IPC 通道失效。错误消息中包含具体的解析错误信息，便于调试。

### 错误的日志输出

Worker 的任何日志输出都应该写入 stderr，而不是 stdout。原因如下：

- stdout 被用作 JSON 数据通道，任何非 JSON 格式的输出都会导致 Manager 端的 JSON 解析失败
- stderr 的内容默认显示在终端上，便于调试，但不会被 Manager 的程序逻辑处理
- 使用 `cmd.Stderr = os.Stderr` 将子进程的 stderr 重定向到父进程的 stderr

如果 Worker 误将日志写入 stdout，会导致 Manager 端 `json.Unmarshal` 失败，Manager 会返回一个错误，但更严重的情况是，如果日志中包含换行符，会破坏行分隔协议，导致后续的 JSON 消息全部错位。

## 进程生命周期

### Worker 启动

Manager 通过 `exec.Command` 启动 Worker 子进程。在启动前，通过 `StdinPipe()` 和 `StdoutPipe()` 创建管道：

```
Manager 进程空间                    Worker 进程空间
┌─────────────────┐               ┌────────────────┐
│  exec.Command    │──stdin pipe──▶│  os.Stdin       │
│  StdinPipe()     │               │  json.Decoder   │
│                  │               │                │
│  StdoutPipe()    │◀─stdout pipe─│  os.Stdout      │
│  bufio.Scanner   │               │  json.Encoder   │
│  json.Decoder    │               │                │
│                  │               │  os.Stderr      │
│  os.Stderr       │◀─stderr─────│  (日志输出)      │
└─────────────────┘               └────────────────┘
```

### Worker 停止

Worker 的停止有两种方式：

1. **正常停止**：Manager 发送 `stop` 命令，Worker 回复后立即 `os.Exit(0)`。Manager 随后调用 `worker.cmd.Wait()` 等待进程实际终止并回收资源。

2. **Manager 主动关闭**：Manager 调用 `stdin.Close()` 关闭管道，Worker 的 `scanner.Scan()` 返回 false（EOF），Worker 退出主循环。不过，当前 Worker 的实现没有处理 EOF 时的逻辑，它会直接退出 `for` 循环并结束进程。

3. **defer Close()**：在 Manager 的 `main()` 函数末尾，通过 `defer worker.Close()` 确保函数退出时关闭子进程。`Close()` 先关闭 stdin，然后调用 `cmd.Wait()` 等待子进程退出。

### 进程清理

正确清理子进程非常重要，否则会留下僵尸进程：

```go
func (w *Worker) Close() {
    w.stdin.Close()  // 关闭 stdin → 子进程的 Scan() 返回 false
    w.cmd.Wait()     // 等待子进程实际终止
}
```

`cmd.Wait()` 会等待子进程退出并释放所有关联的系统资源。如果不调用 `Wait()`，子进程在终止后会成为僵尸进程，占用进程表项。

## 执行流程详解

Manager 的 `main()` 函数执行以下流程：

### 步骤 1：启动 Worker

```go
worker, err := StartWorker("./worker")
if err != nil {
    worker, err = StartWorker("go", "run", "./cmd/ipc-example/worker/")
}
```

优先尝试已编译的 `./worker` 二进制（开发时已编译好的），如果没有则回退到 `go run` 直接从源码运行。这种双重尝试模式确保开发和部署阶段都能正常工作。

### 步骤 2：心跳检测 (ping)

Manager 发送 `ping` 命令，期待 Worker 立即回复 `pong`。这是验证 IPC 通道是否正常的最简单方式。`ping` 不需要额外参数，处理逻辑也是常数时间复杂度，因此响应几乎是即时的。

### 步骤 3：回声测试 (echo)

Manager 发送 `echo` 命令，附加参数 `"Hello World!"`。Worker 将参数原样返回。这验证了双向数据传输的正确性 — 如果 `echo` 返回的数据与发送的数据不一致，说明编码或解码过程中出现了数据损坏。

### 步骤 4：模拟耗时操作 (sleep)

Manager 发送 `sleep` 命令，参数为 `"800ms"`。Worker 会调用 `time.Sleep(800 * time.Millisecond)` 等待 800ms 后回复。Manager 端记录从发送到收到响应的时间：

```go
start := time.Now()
r, _ = worker.Send(Command{Cmd: "sleep", Data: "800ms"})
fmt.Printf("sleep → ok=%v data=%v (耗时 %v)\n", r.Ok, r.Data, time.Since(start).Round(time.Millisecond))
```

这个测试验证了同步模型的核心特征：Manager 在 Worker 处理耗时操作期间被阻塞，无法发送下一条命令。如果输出显示的耗时接近 800ms，则说明同步机制工作正常。

### 步骤 5：请求计数 (count)

Manager 发送 `count` 命令，要求 Worker 返回当前已经处理了多少个请求。由于在此之前已经发送了 sleep/ping/echo 等 4 个请求，`count` 应该返回 4（从 Worker 启动开始累计，包含自身）。

这个测试验证了 Worker 的原子计数器的正确性，也间接验证了 Worker 的进程空间是隔离的 — 每次重新启动 Worker，计数器都从零开始。

### 步骤 6：批量命令确认

Manager 连续发送 3 条 `echo` 命令，每条附加不同的参数。这验证了 IPC 通道在连续通信时的稳定性。如果某条命令丢失或错位，后续的所有命令都会错位，导致非常明显的故障。

### 步骤 7：未知命令处理

Manager 发送一个不存在的命令 `explode`。Worker 回复一个错误，但 IPC 通道不会中断。这个测试验证了 Worker 的容错机制 — 即使 Manager 发送了错误的命令，Worker 不会崩溃，Manager 可以继续发送后续命令。

### 步骤 8：优雅停止

Manager 发送 `stop` 命令，Worker 回复后立即退出。Manager 随后调用 `worker.cmd.Wait()` 等待子进程退出。

## 适用场景

同步 IPC 模式适用于以下场景：

1. **严格顺序控制**：每条命令的执行依赖上一条命令的结果，Manager 必须等待 Worker 完成后才能进行下一步。例如，配置下发时需要先创建目录、再写入文件、再重启服务，每一步都必须成功才能继续。

2. **事务性操作**：多个操作需要作为一个原子单元执行，如果某一步失败需要回滚全部。通过同步模式，Manager 可以精确控制每一步的执行时机和结果验证。

3. **简单的请求-响应**：Worker 执行的计算相对轻量，不需要长时间维持连接。例如，文件哈希计算、数据格式转换、批处理拆分等。

4. **资源受限环境**：Worker 被限制在独立的子进程中运行，资源使用（内存、CPU）受操作系统隔离。如果某个 Worker 崩溃，不会影响 Manager 或其他 Worker。

## 与异步 IPC 的对比

与本示例对应的异步版本（`examples/ipc-async/`）相比：

| 特性 | 同步 (本示例) | 异步 (ipc-async) |
|------|--------------|------------------|
| 通信模式 | 一问一答，先发先回 | 多问多答，可以乱序 |
| Manager 阻塞 | 发送后阻塞直到回复 | 发送立即返回，独立接收 |
| Worker 状态上报 | 不支持 | 支持（通过独立的 report goroutine） |
| 并发管理多个 Worker | 需要为每个 Worker 开 goroutine | 天然支持，一个 recv channel 混合所有 Worker |
| 代码复杂度 | 较低 | 较高（需要管理 recv channel） |
| 适合场景 | 配置下发、启停控制 | 实时监控、数据采集 |

## 通信协议设计原则

### 为什么使用 JSON 而非二进制协议？

1. **可读性**：JSON 是文本格式，可以直接用 `cat`、`echo` 或 `curl` 手动测试，不需要专门的协议分析工具。

2. **自描述**：每条 JSON 消息都包含足够的字段名来推断其含义，不需要额外的协议文档。

3. **Go 原生支持**：`encoding/json` 是 Go 标准库的一部分，`json.NewEncoder` 和 `json.NewDecoder` 可以直接在管道上工作，无需中间缓冲区。

4. **兼容性**：JSON 几乎被所有编程语言支持，Manager 和 Worker 可以分别用不同语言实现。

### 为什么使用行分隔而非长度前缀？

1. **简单性**：行分隔协议天然匹配 Scanner 的按行读取模式，不需要手动管理消息边界。

2. **容错性**：如果某条 JSON 消息损坏或丢失，后续消息的行边界不会受影响（而长度前缀协议中的长度字段损坏会导致整条流错位）。

3. **流式处理**：Worker 可以边处理边输出，不需要预先知道每条消息的最终大小。

### 限制

1. **不能含换行符**：JSON 数据中不能包含未转义的换行符（`\n`），否则会破坏行分隔。所有应用数据中的换行符必须转义为 `\n` 或 `\\n`。

2. **最大行大小**：Scanner 有固定的最大行大小（本示例设置为 1MB），超过限制的行会被丢弃。如果需要在 JSON 中包含很大块的数据（如超过 1MB 的 WebRTC SDP），应考虑使用其他编码方式（如 gzip 压缩后 Base64 编码）。

3. **单工通信**：同一根管道不能同时双向读写。本示例使用两根独立管道（stdin/stdout）实现双向通信。

## 总结

本示例展示了一个功能完整、逻辑清晰的同步 IPC 实现。它的设计围绕"简单可靠"这一核心原则展开：使用 JSON 行协议避免了复杂的序列化代码，使用 `bufio.Scanner` 简化了消息边界管理，使用同步阻塞模型消除了并发控制的需求（代价是吞吐量受限）。这种模式非常适合配置文件下发、命令行工具链、批处理作业控制等不需要高吞吐但要求强顺序和确定性的场景。
