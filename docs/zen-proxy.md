# Zen Proxy Stack (Go)

opencode.ai 免费 zen 端点的代理栈，纯 Go 实现，替代原先的 Python 版
（`zen_proxy.py` / `zen_multi.py` / `local_proxy.py`）。

## 架构

```
pi / opencode ──> local zen-multi :8443 ──> zen-proxy (vps / bwh / cloudcone) :8443 ──> opencode.ai/zen/v1
                                                       │
                  local local-proxy :11434 (Ollama 端口, OpenAI 协议) ── 直接转发多源 ──┘
```

- **zen-proxy**（远程，每台服务器一个）：直接连 `opencode.ai`，负责上游转发。
  v6/v4 双栈 failover、FreeUsageLimitError cooldown（到 UTC 午夜）、
  keep-alive 注释过滤、stall 检测（30s；工具调用期间 180s）、每客户端限流/封禁。
- **zen-multi**（本地）：聚合 bwh/vps/cloudcone 三个 zen-proxy，
  失败自动 failover + cooldown，并提供 "inf loop" 工具调用注入
  （`deepseek-v4-flash-inf` 模型）。提供 `/v1/models`、`/status`。
- **local-proxy**（本地）：Ollama 兼容端口 `11434` 的简单转发器，
  直接透传流、多源 failover，无注入/无 keep-alive 处理。

## 构建

```bash
# 全部
go build ./cmd/zen-proxy/ ./cmd/zen-multi/ ./cmd/local-proxy/

# 交叉编译 local-proxy（Windows / Termux / Linux）
GOOS=windows GOARCH=amd64 go build -o local_proxy_windows_amd64.exe ./cmd/local-proxy/
GOOS=linux   GOARCH=arm64  go build -o local_proxy_termux_arm64 ./cmd/local-proxy/
GOOS=linux   GOARCH=amd64  go build -o local_proxy_linux_amd64 ./cmd/local-proxy/
```

所有代理用 `CGO_ENABLED=0` 静态编译，可跑在旧 glibc 服务器 / Termux 上。

## 部署

### zen-proxy（远程三台）

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o zen_proxy_go ./cmd/zen-proxy/
scp zen_proxy_go <server>:/root/zen_proxy_go
```

systemd `zen.service`：

```
[Service]
Type=simple
ExecStart=/root/zen_proxy_go 0.0.0.0 8443 \
  /root/.acme.sh/*.moonchan.xyz_ecc/fullchain.cer \
  /root/.acme.sh/*.moonchan.xyz_ecc/*.moonchan.xyz.key 30 vps
Restart=always
RestartSec=3
```

位置参数：`[addr] [port] [cert] [key] [timeout] [server_id]`。
证书目录名含字面 `*`（acme.sh 产物），systemd 直接按字面解析。

### zen-multi（本地）

```bash
CGO_ENABLED=0 go build -o /usr/local/bin/zen_multi_go ./cmd/zen-multi/
```

systemd `zen-multi.service`：`ExecStart=/usr/local/bin/zen_multi_go 127.0.0.1 8443`

上游源在 `cmd/zen-multi/main.go` 的 `upList` 中配置
（`https://{bwh,vps,cloudcone}.moonchan.xyz:8443`）。

### local-proxy（可选）

```bash
./local_proxy_linux_amd64          # 默认 127.0.0.1:11434
./local_proxy_linux_amd64 0.0.0.0 11434
```

## 端点

| 路径 | 方法 | 说明 |
|------|------|------|
| zen-proxy `/chat/completions`、`/v1/chat/completions` | POST | 转发到 opencode.ai |
| zen-proxy `/status` | GET | cooldown / banned / goroutines / 分 v4/v6 用量统计 |
| zen-multi `/v1/chat/completions` | POST | 多源 failover + inf 注入 |
| zen-multi `/v1/models` | GET | 模型列表 |
| zen-multi `/status` | GET | 各源 cooldown / reqs |
| local-proxy `/v1/chat/completions`、`/v1/models` | POST/GET | 简单多源转发 |

### 用量统计（zen-proxy `/status`）

`stats` 字段按协议栈分组，进程存活期间累计：

```json
"stats": {
  "since": "2026-08-05T00:00:00Z",
  "total": { "reqs": 12, "ok": 10, "streams": 9, "bytes": 40960,
             "tool_calls": 3, "stalls": 1, "errs": 2, "free_limit": 0 },
  "per": { "v6": { "...": 0 }, "v4": { "...": 0 } }
}
```

| 字段 | 含义 |
|------|------|
| `reqs` | 尝试连接上游的次数 |
| `ok` | 上游返回 200 的次数 |
| `streams` | 已提交（commit）的流式响应次数 |
| `bytes` | 转发给客户端的 SSE 字节数 |
| `tool_calls` | 转发的 tool_call delta 次数 |
| `stalls` | stall 超时中断次数（工具流/普通流同计） |
| `errs` | 传输错误 / 非 200 / 预读无数据（上游 200 但流空） |
| `free_limit` | `FreeUsageLimitError` 命中次数（触发午夜 cooldown） |

## 与 Python 版的差异

Go 版消除了 Python 栈的三个核心问题：

1. **`resp.close()` 阻塞**：Python `http.client` 关闭未读完的 keep-alive 流时会
   drain 阻塞（实测 70s+）。Go 直接 `resp.Body.Close()` 即时中断，reader goroutine
   自动退出。
2. **生成器并发冲突**：Python 生成器在 GIL 下不能跨线程 `next()`，超时后残留
   daemon 线程与下一次迭代撞车（`generator already executing`）。Go 用单 goroutine
   读 + channel，天然无竞争。
3. **超时无法取消**：Python 线程卡在 socket read 无法真正取消。Go 用
   `select` + timer + 关闭 body 干净中断。

语义保持：keep-alive 注释不转发、工具调用流（`saw_tool`）不误判死、
FreeUsageLimitError 冷却到 UTC 午夜、长工具参数完整转发不截断。

## inf 注入的 SSE 协议（zen-multi）

`deepseek-v4-flash-inf` 模型在流式中途/结尾/断流/idle 时若一直没产出 tool_call，
zen-multi 会注入一条假的 `bash` tool_call，驱动 opencode 自动继续。

注入的完整序列（三条事件，逐条 `\n\n` 分隔）必须是 OpenAI 兼容的合法 tool_call 收尾：

```
data: {"id":"inf",...,"choices":[{"index":0,"delta":{"tool_calls":[
      {"index":0,"id":"call_inf_N","type":"function",
       "function":{"name":"bash","arguments":"{\"command\":\"echo 请继续完善当前项目...\"}"}}]},
      "finish_reason":null}]}

data: {"id":"inf",...,"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]
```

- `idleInject()`（idle 超时路径）自带 `data: [DONE]`。
- `infInject()`（finish 改写 / 上游 `[DONE]` / EOF 断流三条路径）**不带** `[DONE]`：
  finish 改写与上游 `[DONE]` 路径的上游本来就会转发自己的 `[DONE]`，再加会双 DONE；
  EOF 断流路径会在 `infInject()` 之后**单独补发** `data: [DONE]`，否则客户端认为流被截断。
- 收敛规则：四条注入路径最终都保证终端为 `finish_reason:"tool_calls"` 事件 + `[DONE]`，
  改动这里时保持此收尾不变。
- 已注入 tool_call 但上游 `[DONE]` 未到就 stall 的角落：只补一个裸 `data: [DONE]`，
  不再重复收尾事件。

## 实现约束

三个 Go 代理共享以下实现约束（改动时需保持一致）：

- **SSE 提交后禁止 failover**：`forwardStream` 一旦 `WriteHeader(200)` + flush 首包，
  后续任何失败（stall / mid-stream 错误 / 客户端断开）都不允许再切换到下一个源，
  否则会对已提交的响应双写。实现上用 `(ok, committed)` 返回值约定：只有
  `committed=false`（预读阶段失败）时才可 failover。
- **请求体上限**：`io.ReadAll` 一律经 `io.LimitReader`，超过 10MB 返回 413。
- **工具流 stall 收尾**：工具调用流（`saw_tool`）超过 180s 无真实数据时，三个代理
  都补发 `finish_reason:"length"` 终止事件 + `data: [DONE]` 再断开，客户端能区分
  「完成」与「截断」；非工具流行为不变（zen-proxy / local-proxy 写 `UpstreamStall`
  错误，zen-multi 静默断开）。`lengthChunk(model)` 为共用格式。
- **reader goroutine**：`startReader` 用 done channel 中止，退出循环后必须能立即
  终止阻塞中的 send/read，不能依赖 `resp.Body.Close()` 兜底。

## 关联代码

- `pkg/apifwd/zen.go`：另一套基于 Gin 的 zen 转发库（v4/v6 显式 IP 解析 + cooldown），
  与 `cmd/zen-proxy` 功能重叠，可用于 server 端替代或对照。
