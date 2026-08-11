// AI: generated with assistance from AI (2026-06-23)
//
// Worker 子进程 (异步版)
//
// =====================================================
// 功能概述
// =====================================================
// 本文件实现 Worker 子进程的异步版本。与同步版的区别：
// 使用两个独立 goroutine 实现读写分离，支持：
//   1. 管理端随时发命令，Worker 立即处理并回复
//   2. Worker 主动上报状态（无需管理端轮询）
//
// =====================================================
// 架构
// =====================================================
//   goroutine 1 (读): for scanner.Scan() 循环读 stdin，
//                      解析 JSON Command，switch 分发执行
//   goroutine 2 (写): 定时主动上报系统状态到 stdout
//
//   写 goroutine 每 2 秒上报一次:
//   {"ok":true,"id":"report","data":{"uptime_sec":N,"req_count":N,"cpu_percent":3.2}}
//
// =====================================================
// 适用场景
// =====================================================
// 管理工具需要实时监控子进程状态时使用异步版。
// 子进程可以主动推送指标数据，管理端被动接收。
// 典型场景：进程监控、状态看板、实时日志流。
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
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

var reqID atomic.Int64

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	enc := json.NewEncoder(os.Stdout)

	// 读 goroutine：不断读 stdin，处理命令
	go func() {
		for scanner.Scan() {
			b := scanner.Bytes()
			if len(b) == 0 {
				continue
			}
			var cmd Command
			if err := json.Unmarshal(b, &cmd); err != nil {
				enc.Encode(Response{Ok: false, Data: fmt.Sprintf("bad json: %v", err)})
				continue
			}
			n := reqID.Add(1)
			switch cmd.Cmd {
			case "ping":
				enc.Encode(Response{Ok: true, ID: cmd.ID, Data: "pong"})
			case "echo":
				enc.Encode(Response{Ok: true, ID: cmd.ID, Data: cmd.Data})
			case "sleep":
				d, _ := time.ParseDuration(cmd.Data)
				time.Sleep(d)
				enc.Encode(Response{Ok: true, ID: cmd.ID, Data: fmt.Sprintf("slept %s", d)})
			case "count":
				enc.Encode(Response{Ok: true, ID: cmd.ID, Data: n})
			case "stop":
				enc.Encode(Response{Ok: true, ID: cmd.ID, Data: "bye"})
				os.Exit(0)
			default:
				enc.Encode(Response{Ok: false, ID: cmd.ID, Err: fmt.Sprintf("unknown: %s", cmd.Cmd)})
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "stdin read error: %v\n", err)
		}
	}()

	// 写 goroutine：主动上报状态（模拟每秒主动上报一次）
	for i := 1; ; i++ {
		time.Sleep(2 * time.Second)
		enc.Encode(Response{
			Ok:   true,
			ID:   "report",
			Data: map[string]interface{}{
				"uptime_sec":  i * 2,
				"req_count":   reqID.Load(),
				"cpu_percent": 3.2,
			},
		})
	}
}
