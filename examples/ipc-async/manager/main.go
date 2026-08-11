// AI: generated with assistance from AI (2026-06-23)
//
// Manager 管理端 (异步版)
//
// =====================================================
// 功能概述
// =====================================================
// 本文件实现 Manager 管理端的异步版本。与同步版的核心区别：
//   1. Send() 只写不读，发送后立即返回，不等待回复
//   2. 后台 goroutine 持续读 stdout，收到的所有响应
//      （包括命令回复和主动上报）都送入 recv channel
//   3. 支持 Recv() 阻塞读取、RecvAll() 非阻塞批量读取
//
// =====================================================
// 适用场景
// =====================================================
// 管理端需要同时管控多个子进程，且子进程会上报状态时。
// 管理端可以随时发命令，不会因为等待回复而阻塞后续操作。
// 典型场景：同时管控多个 worker、实时监控面板。
//
// =====================================================
// 数据流
// =====================================================
//   发送: Send(cmd) → json.Encoder(stdin) → 子进程
//   接收: goroutine → bufio.Scanner(stdout) → recv chan
//   读取: Recv() / RecvAll() ← recv chan
//
// 注意: async worker 每次主动上报都使用 "report" 作为 id，
// 管理端可根据 id 区分是命令回复还是主动上报。
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

type Command struct {
	Cmd  string `json:"cmd"`
	ID   string `json:"id,omitempty"`
	Data string `json:"data,omitempty"`
}

type Response struct {
	Ok   bool        `json:"ok"`
	ID   string      `json:"id,omitempty"`
	Data interface{} `json:"data,omitempty"`
	Err  string      `json:"err,omitempty"`
}

type WorkerAsync struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	enc    *json.Encoder
	recv   chan Response // 所有收到的响应（包括主动上报）都送到这里
}

func StartWorkerAsync(exe string, args ...string) (*WorkerAsync, error) {
	cmd := exec.Command(exe, args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	w := &WorkerAsync{
		cmd:  cmd,
		stdin: stdin,
		enc:  json.NewEncoder(stdin),
		recv: make(chan Response, 64),
	}

	// 后台 goroutine：持续读 stdout，收到的响应全送到 recv 通道
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			var resp Response
			if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
				continue
			}
			w.recv <- resp
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "worker stdout read error: %v\n", err)
		}
	}()

	return w, nil
}

// Send 发送命令，不等待回复（异步）
func (w *WorkerAsync) Send(cmd Command) error {
	return w.enc.Encode(cmd)
}

// Recv 读取一条响应（阻塞，支持超时）
func (w *WorkerAsync) Recv(timeout time.Duration) (*Response, error) {
	if timeout <= 0 {
		resp := <-w.recv
		return &resp, nil
	}
	select {
	case resp := <-w.recv:
		return &resp, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout")
	}
}

// RecvAll 读取当前所有未读的响应（非阻塞）
func (w *WorkerAsync) RecvAll() []Response {
	var out []Response
	for {
		select {
		case resp := <-w.recv:
			out = append(out, resp)
		default:
			return out
		}
	}
}

func (w *WorkerAsync) Close() {
	w.stdin.Close()
	w.cmd.Wait()
}

func main() {
	worker, err := StartWorkerAsync("./worker")
	if err != nil {
		worker, err = StartWorkerAsync("go", "run", "./cmd/ipc-async/worker/")
		if err != nil {
			fmt.Fprintf(os.Stderr, "start worker: %v\n", err)
			os.Exit(1)
		}
	}
	defer worker.Close()

	// 后台打印所有收到的响应
	go func() {
		for resp := range worker.recv {
			if resp.ID == "report" {
				// 主动上报
				fmt.Printf("[上报] %+v\n", resp.Data)
			} else {
				// 命令回复
				fmt.Printf("[回复] id=%s ok=%v data=%v err=%v\n", resp.ID, resp.Ok, resp.Data, resp.Err)
			}
		}
	}()

	// 主 goroutine 只管发命令，不用等回复
	fmt.Println("=== 发 ping ===")
	worker.Send(Command{Cmd: "ping"})

	time.Sleep(500 * time.Millisecond)

	fmt.Println("=== 发 echo ===")
	worker.Send(Command{Cmd: "echo", Data: "hello"})

	time.Sleep(2500 * time.Millisecond)

	fmt.Println("=== 发 sleep 1s ===")
	worker.Send(Command{Cmd: "sleep", Data: "1s"})

	time.Sleep(3500 * time.Millisecond)

	fmt.Println("=== 发 stop ===")
	worker.Send(Command{Cmd: "stop"})

	time.Sleep(500 * time.Millisecond)
}
