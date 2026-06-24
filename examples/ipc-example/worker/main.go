// AI: generated with assistance from AI (2026-06-23)
//
// Worker 子进程 (同步版)
//
// =====================================================
// 功能概述
// =====================================================
// 本文件实现 Worker 子进程，通过 stdin/stdout JSON 行协议
// 与管理端通信。采用一问一答的同步模式：
//   1. 从 stdin 读取一行 JSON Command
//   2. 解析命令并执行（ping/echo/sleep/count/stop）
//   3. 向 stdout 写入一行 JSON Response
//   4. 循环等待下一条命令
// 日志输出走 stderr，不干扰 stdout 的数据通道。
//
// =====================================================
// 通信协议
// =====================================================
// 管理端 ── stdin ──→ {"cmd":"ping","id":"","data":""}
// Worker  ── stdout ─→ {"ok":true,"id":"","data":"pong"}
// 每条消息为单行 JSON，以 \n 分隔。
//
// =====================================================
// 支持命令
// =====================================================
// ping         心跳检测                      → pong
// echo <text>  回声测试，原样返回 text        → <text>
// sleep <d>    模拟耗时操作（如 "800ms"）     → slept <d>
// count        返回已处理请求计数             → int64
// stop         优雅退出                       → bye
//
// =====================================================
// 编译
// =====================================================
// go build -o worker ./cmd/ipc-example/worker/
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// Command 管理端发来的命令
type Command struct {
	Cmd  string `json:"cmd"`            // 命令名: ping/echo/sleep/count/stop
	ID   string `json:"id,omitempty"`   // 可选的请求标识，响应中原样返回
	Data string `json:"data,omitempty"` // 命令参数，如 sleep 的持续时长 "800ms"
}

// Response 回复给管理端的响应
type Response struct {
	Ok   bool        `json:"ok"`            // 是否成功
	ID   string      `json:"id,omitempty"`  // 对应请求的 ID
	Data interface{} `json:"data,omitempty"` // 响应数据
	Err  string      `json:"err,omitempty"` // 错误信息 (Ok=false 时填写)
}

var reqID atomic.Int64 // 自增请求序号，用于 count 命令

func main() {
	// 从标准输入读取 JSON 命令
	scanner := bufio.NewScanner(os.Stdin)
	// 向标准输出写入 JSON 响应
	enc := json.NewEncoder(os.Stdout)

	// 主循环: 逐行读取 stdin，每行是一条 JSON 命令
	for scanner.Scan() {
		b := scanner.Bytes()
		if len(b) == 0 {
			continue // 跳过空行
		}

		// 解析 JSON 命令
		var cmd Command
		if err := json.Unmarshal(b, &cmd); err != nil {
			// JSON 格式错误，回复错误信息
			enc.Encode(Response{
				Ok:  false,
				Err: fmt.Sprintf("bad json: %v", err),
			})
			continue
		}

		// 计数器 +1，用于 count 命令
		n := reqID.Add(1)

		// 根据命令名分发处理
		switch cmd.Cmd {
		case "ping":
			// 心跳检测，回复 pong
			enc.Encode(Response{Ok: true, ID: cmd.ID, Data: "pong"})

		case "echo":
			// 原样返回 Data，用于测试通信是否正常
			enc.Encode(Response{Ok: true, ID: cmd.ID, Data: cmd.Data})

		case "sleep":
			// 模拟耗时操作，Data 是持续时间如 "800ms"
			d, _ := time.ParseDuration(cmd.Data)
			time.Sleep(d)
			enc.Encode(Response{
				Ok:   true,
				ID:   cmd.ID,
				Data: fmt.Sprintf("slept %s", d),
			})

		case "count":
			// 返回当前已经处理了多少个请求
			enc.Encode(Response{Ok: true, ID: cmd.ID, Data: n})

		case "stop":
			// 回复 bye 后立即退出
			enc.Encode(Response{Ok: true, ID: cmd.ID, Data: "bye"})
			os.Exit(0)

		default:
			// 未知命令，回复错误
			enc.Encode(Response{
				Ok:  false,
				ID:  cmd.ID,
				Err: fmt.Sprintf("unknown: %s", cmd.Cmd),
			})
		}
	}
	// scanner.Scan() 返回 false → 可能是错误或 EOF
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "stdin read error: %v\n", err)
	}
}
