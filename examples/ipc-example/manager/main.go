// AI: generated with assistance from AI (2026-06-23)
//
// Manager 管理端 (同步版)
//
// =====================================================
// 功能概述
// =====================================================
// 本文件实现 Manager 管理端，启动 Worker 子进程并通过
// stdin/stdout JSON 行协议通信。采用同步一问一答模式：
// 发送命令后阻塞等待响应，收到响应后才能发下一条。
// 适用于严格顺序控制的场景（配置下发、启停控制）。
//
// =====================================================
// 工作流程
// =====================================================
//   1. exec.Command + StdinPipe/StdoutPipe 启动子进程
//   2. json.NewEncoder(stdin) 编码并写入 JSON 命令
//   3. bufio.Scanner(stdout) 阻塞读取 JSON 响应行
//   4. 解析 Response，返回给调用方
//   5. Close() 关闭 stdin → 子进程 for 循环退出
//
//   管理端可以通过并行创建多个 Worker 实例来管控多个子组件，
//   每个实例有独立的进程和管道，互不干扰。
//
// =====================================================
// 编译与运行
// =====================================================
// go build -o manager ./cmd/ipc-example/manager/
// ./manager
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

// ============================================================================
// 通信协议定义 (与 worker 端保持一致)
// ============================================================================

// Command 发送给子进程的命令
type Command struct {
	Cmd  string `json:"cmd"`            // 命令名
	ID   string `json:"id,omitempty"`   // 请求标识（可选）
	Data string `json:"data,omitempty"` // 参数
}

// Response 子进程返回的响应
type Response struct {
	Ok   bool        `json:"ok"`            // 是否成功
	ID   string      `json:"id,omitempty"`  // 请求标识
	Data interface{} `json:"data,omitempty"` // 返回数据
	Err  string      `json:"err,omitempty"` // 错误描述
}

// ============================================================================
// Worker 子进程管理器
// ============================================================================

// Worker 封装了一个子进程及其 stdin/stdout 管道
type Worker struct {
	cmd     *exec.Cmd        // 子进程句柄
	stdin   io.WriteCloser   // 子进程的标准输入（我们写，它读）
	scanner *bufio.Scanner   // 子进程的标准输出（它写，我们读）
	enc     *json.Encoder    // 基于 stdin 的 JSON 编码器
}

// StartWorker 启动一个子进程并建立 JSON 通信管道
//
// exe:  可执行文件路径
// args: 启动参数
//
// 底层原理:
//   exec.Command 创建子进程
//   StdinPipe()   → 返回一个 io.WriteCloser，写入的内容成为子进程的 stdin
//   StdoutPipe()  → 返回一个 io.Reader，读取的内容来自子进程的 stdout
//   Stderr 重定向到 os.Stderr，日志不会污染 JSON 数据流
func StartWorker(exe string, args ...string) (*Worker, error) {
	cmd := exec.Command(exe, args...)

	// 获取子进程标准输入管道（管理端写入）
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	// 获取子进程标准输出管道（管理端读取）
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	// 子进程的日志走 stderr，不影响 stdout 的 JSON 数据流
	cmd.Stderr = os.Stderr

	// 启动子进程
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// 创建带缓冲的 Scanner，读取 stdout 中的 JSON 行
	scanner := bufio.NewScanner(stdout)
	// 设置最大行大小: 1MB 缓冲区，1MB 最大行（防止大 JSON 被截断）
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	return &Worker{
		cmd:     cmd,
		stdin:   stdin,
		scanner: scanner,
		enc:     json.NewEncoder(stdin),
	}, nil
}

// Send 发送一条命令并等待响应
//
// 工作流程:
//   1. enc.Encode(cmd) → 将 Command 编码为 JSON + 换行符，写入 stdin
//   2. scanner.Scan()  → 从 stdout 读取一行（阻塞等待子进程回复）
//   3. json.Unmarshal  → 解析 JSON 响应
//
// 这是同步的: 发送后必须等子进程回复完才能发下一条
// 如果需要并发，可以为每个子进程启动独立的 goroutine
func (w *Worker) Send(cmd Command) (*Response, error) {
	// 写入 JSON 命令到子进程 stdin
	if err := w.enc.Encode(cmd); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	// 等待并从子进程 stdout 读取一行响应
	if !w.scanner.Scan() {
		// 子进程可能已退出或管道已关闭
		if err := w.scanner.Err(); err != nil {
			return nil, fmt.Errorf("read: %w", err)
		}
		return nil, io.EOF
	}

	// 解析 JSON 响应
	var resp Response
	if err := json.Unmarshal(w.scanner.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("bad json: %w (raw: %s)", err, w.scanner.Bytes())
	}
	return &resp, nil
}

// Close 关闭子进程的 stdin（通知子进程退出），等待进程结束
func (w *Worker) Close() {
	w.stdin.Close()  // 关闭 stdin → 子进程的 scanner.Scan() 返回 false → for 循环退出
	w.cmd.Wait()     // 等待子进程实际终止，回收资源
}

// ============================================================================
// 主流程: 演示管理端如何控制 Worker
// ============================================================================

func main() {
	// 优先尝试已编译好的 ./worker 二进制
	// 如果没有，回退到 go run 直接运行源码
	worker, err := StartWorker("./worker")
	if err != nil {
		worker, err = StartWorker("go", "run", "./cmd/ipc-example/worker/")
		if err != nil {
			fmt.Fprintf(os.Stderr, "启动 worker 失败: %v\n", err)
			os.Exit(1)
		}
	}
	// 函数退出前关闭子进程
	defer worker.Close()

	// --- 测试 1: 心跳检测 ---
	// 发送 ping，期待立即回复 pong
	r, _ := worker.Send(Command{Cmd: "ping"})
	fmt.Printf("ping → ok=%v data=%v\n", r.Ok, r.Data)

	// --- 测试 2: 回声测试 ---
	// 发送一段文本，子进程原样返回
	r, _ = worker.Send(Command{Cmd: "echo", Data: "Hello World!"})
	fmt.Printf("echo → ok=%v data=%v\n", r.Ok, r.Data)

	// --- 测试 3: 模拟耗时操作 ---
	// 子进程 sleep 800ms 后回复，验证同步等待机制
	start := time.Now()
	r, _ = worker.Send(Command{Cmd: "sleep", Data: "800ms"})
	fmt.Printf("sleep → ok=%v data=%v (耗时 %v)\n", r.Ok, r.Data, time.Since(start).Round(time.Millisecond))

	// --- 测试 4: 请求计数 ---
	// 子进程内部维护一个原子计数器，每收到一个请求 +1
	r, _ = worker.Send(Command{Cmd: "count"})
	fmt.Printf("count → ok=%v data=%v\n", r.Ok, r.Data)

	// --- 测试 5: 批量命令 ---
	// 连续发送多条 echo，验证通信稳定
	for i := 0; i < 3; i++ {
		r, _ = worker.Send(Command{Cmd: "echo", Data: fmt.Sprintf("batch %d", i)})
		fmt.Printf("batch %d → %v\n", i, r.Data)
	}

	// --- 测试 6: 未知命令处理 ---
	// 发送不存在的命令，验证错误回复机制
	r, _ = worker.Send(Command{Cmd: "explode"})
	fmt.Printf("未知命令 → ok=%v err=%v\n", r.Ok, r.Err)

	// --- 测试 7: 停止子进程 ---
	// 发送 stop，子进程回复后立即 os.Exit(0)
	r, _ = worker.Send(Command{Cmd: "stop"})
	fmt.Printf("stop → ok=%v data=%v\n", r.Ok, r.Data)

	// 等待子进程实际退出
	worker.cmd.Wait()
	fmt.Println("worker 已退出。")
}
