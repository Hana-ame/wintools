# Zen 代理栈 V2 — capture-proxy 与 zen-multi 完整文档

本文档记录 Zen 代理栈第二次重构（V2）的全部变化、功能设计，以及与 opencode.ai 原版
zen 端点 / 原 zen-proxy 的差异。

- 代码位置：`scripts/capture_proxy.go`（新）、`cmd/zen-multi/main.go`（改造）
- 对比对象：`cmd/zen-proxy/main.go`（远程 V1 版）、`cmd/local-proxy-detected/main.go`
- 上游端点：`https://opencode.ai/zen/v1`（免费 / 公共 key `Bearer public`）

---

## 1. 架构与请求流转

```
opencode ──> zen-multi :8443 (本机 systemd, 聚合)
                  │
                  ├──> capture-proxy A (vps.moonchan.xyz:8443, v4/v6/auto)
                  ├──> capture-proxy B (bwh.moonchan.xyz:8443, v4/v6/auto)
                  └──> capture-proxy C (c.moonchan.xyz:8443, v4/v6/auto)
                            │
                            └──> opencode.ai/zen/v1 (Cloudflare)
```

- **zen-multi**（本机）：无状态聚合器。收到请求后按随机序 + 冷却跳过遍历所有上游
  capture-proxy，任一成功即返回；全失败按最后错误回 502/503/429。
- **capture-proxy**（每节点一个）：真正的「出口代理」，负责双栈 v4/v6 拨号、
  opencode 客户端伪装、SSE 流检测与工具调用注入。
- 直连链路（可选）：`opencode ──> capture-proxy :8000 ──> zen` 也是独立可用入口。

模型名路由（zen-multi）：

| 模型 | 行为 |
|------|------|
| `deepseek-v4-flash-free` | 自动选源，随机序 + failover |
| `deepseek-v4-flash-inf` | 自动选源 + 无限续命注入（见 §6.4） |
| `deepseek-v4-flash-vps/bwh/cloudcone` | 强制指定源，**失败不 fallback** |

---

## 2. capture-proxy 功能总览

`scripts/capture_proxy.go` 从「只抓包返回 404」的一次性工具重写为完整转发代理：

| 功能 | 说明 |
|------|------|
| 双栈 v4/v6 转发 | 按 `--mode` 决定尝试顺序，`auto` = v6→v4 failover |
| 模式 API | `GET/POST /mode`，运行中切换 v4/v6/auto（双 IP = 双额度） |
| opencode 伪装 | 透传客户端 header，缺失时补齐 opencode UA / x-opencode-* / X-Session-Id |
| gzip 请求体 | `Content-Encoding: gzip` 自动解压再转发（支持大 body） |
| 抓包 | `--out` 目录保存每个请求的 method/URL/headers/body |
| SSE 流检测 | 预读 30s 首包 + 10s/token 节奏 + 工具流 180s 思考窗口 |
| 工具注入 | stall/EOF/error 且无产出时注入 `echo 继续` tool_call |
| h1 回退 | http2 挂起/超时时自动用 http/1.1 重试一次 |
| 冷却 | `FreeUsageLimitError` 冷却到 UTC 午夜 |
| TLS | `--cert/--key` 起 HTTPS（与 V1 zen-proxy 仅证书差异） |
| 统计 | `/status` 分 v4/v6 的 reqs/ok/errs/free_limit |

### 2.1 命令行 flags（弃用 argv）

```bash
go run scripts/capture_proxy.go \
  --listen 0.0.0.0:8000 \   # 监听地址 (默认 127.0.0.1:8000)
  --mode auto \             # auto | v4 | v6 (默认 auto)
  --out /tmp/captured \     # 抓包目录 (空串禁用)
  --cert fullchain.cer \    # 提供后以 HTTPS 监听
  --key privkey.pem
```

### 2.2 模式 API

```bash
curl -s http://127.0.0.1:8000/mode                    # {"mode":"auto",...}
curl -s -X POST http://127.0.0.1:8000/mode?mode=v4    # 切 v4
curl -s -X POST -d '{"mode":"v6"}' http://127.0.0.1:8000/mode
curl -s http://127.0.0.1:8000/status                  # 含 cooldown / 分栈统计
```

`auto` 模式：尝试顺序 `["v6","v4"]`；`v4`/`v6` 只走单一栈。某栈返回
`FreeUsageLimitError` 即冷却到 UTC 午夜并切另一栈。

### 2.3 opencode 伪装（关键设计）

上游 zen 走 Cloudflare，opencode 真实客户端直连带：

```
User-Agent: opencode/1.18.16 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14
X-Session-Id / X-Session-Affinity: ses_...
x-opencode-session / x-opencode-request / x-opencode-client
```

**问题**：proxy 若只透传 `Authorization` 而不带上述 header，Cloudflare 会对请求
hang（不发响应头），表现为「直连有反应、proxy 总是超时」。

**方案**（`impersonate()`）：
1. 透传客户端全部安全 header（走 `pkg/proxyheaders.ForwardRequestHeaders`）；
2. `Authorization` 显式转发，缺失回退 `Bearer public`；
3. **强制覆盖 UA 为 opencode UA**——除非客户端本来就是真实 opencode（UA 以
   `opencode` 开头）。curl / Go 默认 UA 会被上游挑战 hang；
4. 补齐 `X-Session-Id` / `X-Session-Affinity` / `x-opencode-session` /
   `x-opencode-request` / `x-opencode-client`（客户端缺失时填占位值）。

### 2.4 gzip 请求体

`readBody()`：若客户端 `Content-Encoding: gzip`，解压后再 JSON 解析与转发。
限制 `maxRequestBody = 10MB`，超限 413。解压日志 `gunzip request body: 114 -> 109 bytes`。

---

## 3. zen-multi 功能总览（V2 变化）

`cmd/zen-multi/main.go` 从「聚合三个远程 zen-proxy」改为「聚合多个 capture-proxy」：

| V1（旧） | V2（新） |
|----------|----------|
| `upList` 写死 bwh/vps/cloudcone 三个 zen-proxy | `--source name=base` 运行时指定，可重复 |
| argv 位置参数 `host port` | `--listen` / `--source` flags |
| 无模式控制 | `/mode` API 一个开关同步所有源 |
| 无 gzip 入站 | `readBody()` 解压 gzip 请求体 |
| `/status` 无 base 字段 | `/status` 显示 base 地址 |

### 3.1 flags

```bash
/usr/local/bin/zen_multi_go \
  --listen 127.0.0.1:8443 \
  --source vps=https://vps.moonchan.xyz:8443 \
  --source bwh=https://bwh.moonchan.xyz:8443 \
  --source cloudcone=https://c.moonchan.xyz:8443
```

### 3.2 模式同步

`POST /mode?mode=v4|v6|auto` → 遍历所有上游，逐一向其 `/mode` 发 POST 切换。
任一源失败不阻断其余；全部失败返回 502。GET `/mode` 返回各源 base + cooldown。

### 3.3 请求体 gzip

入站 gzip 解压（同 capture-proxy）。**出站**仍 gzip 压缩转发给上游
capture-proxy（capture-proxy 会再解压），保持 `Content-Encoding: gzip` 链路。

---

## 4. 与 zen 原版的设置差异

「zen 原版」指：opencode.ai 的 zen 端点（Cloudflare 后）对真实 opencode 客户端的
要求，以及 V1 `cmd/zen-proxy` 的参数。

### 4.1 对外行为差异（客户端视角）

| 项 | zen 原版直连 | V2 代理栈 |
|----|--------------|-----------|
| 认证 | `OPENCODE_API_KEY` 真实 key | `Bearer public`（免费端点公共 key） |
| 必备 header | opencode UA + x-opencode-* + X-Session-Id，缺则 Cloudflare hang | capture-proxy 自动补齐伪装，curl 也能用 |
| IP 冗余 | 单 IP，额度用尽即 429 | v4+v6 双栈 + 多节点聚合，额度翻倍 |
| 流中断 | 客户端看到截断 | 工具流注入 `echo 继续` / seal `[DONE]` |
| 请求体 | 明文 JSON | 支持 gzip 压缩 |

### 4.2 参数对照（capture-proxy vs V1 zen-proxy）

| 项 | V1 `cmd/zen-proxy` | V2 `capture-proxy` |
|----|--------------------|--------------------|
| 启动参数 | 位置参数 `addr port cert key timeout server_id mode` | `--listen --mode --out --cert --key` |
| 模式 | 第 7 参数 `http/https/auto`，静态 | `--mode` + `/mode` API 运行中可切 |
| v6/v4 顺序 | 写死 `["v6","v4"]` | 按模式：`auto`→v6先，`v4`/`v6`→单栈 |
| h1 回退 | 无（V1 直接报错） | http2 超时自动 h1 重试 |
| opencode 伪装 | 无 | 强制 opencode UA + 补齐 x-opencode-* |
| gzip 入站 | 有（gunzip） | 有 |
| 抓包 | 无 | `--out` 目录 |
| 流超时 | `stallTimeout=10s` | `stallTimeout=30s` 首包预读 |
| 工具注入 | 无（只 seal `[DONE]`） | stall/EOF/error 注入 `echo 继续` |
| 统计 | `/status` 分栈 | `/status` 分栈 + `/mode` |

### 4.3 超时与冷却参数

| 常量 | capture-proxy | zen-multi | V1 zen-proxy |
|------|---------------|-----------|--------------|
| connectTimeout | 10s | 10s | 10s |
| headerTimeout | —（用 60s ResponseHeaderTimeout） | 30s | 10s |
| stallTimeout（首包预读） | 30s | 10s | 10s |
| tokenGapTimeout | 10s | 10s | 10s |
| toolStallTimeout | 180s | 180s | 180s |
| thinkGrace | 45s（未用，保留） | — | — |
| 连接失败冷却 | 无（单实例直接 502 换栈） | 60s | — |
| FreeUsageLimitError 冷却 | 直接到 UTC 午夜 | 升级 1min→5min→午夜 | 升级 1min→5min→午夜 |
| 连续失败阈值 | — | 3 次 | 3 次 |

> 注意：capture-proxy 与 V1 zen-proxy 的 stallTimeout 不同（30s vs 10s）。
> capture-proxy 放宽首包等待，缓解上游 Cloudflare 慢响应导致的误判。

---

## 5. 设计细节

### 5.1 双栈 failover（capture-proxy）

1. `resolveOnce()` 用公共 DNS（1.1.1.1 / 8.8.8.8 / 223.5.5.5 / 114.114.114.114）
   解析 `opencode.ai` 拿 v4/v6 IP，绕开 Termux `::1:53` 等坏 resolver；
2. 两个 HTTP client（v4/v6）+ 两个 h1 回退 client，`DialContext` 直接拨 IP，
   `Host` header 保持 `opencode.ai`，`InsecureSkipVerify` + SNI；
3. 按 `stacks()` 顺序尝试；某栈 `client.Do` 失败 → 记错误 → 下一栈；
4. http2 超时（`timeout awaiting response headers` / `http2`）→ 用 h1 client
   重建请求重试一次（body 必须重建，原 req 已被耗尽）；
5. `FreeUsageLimitError` → 该栈冷却到 UTC 午夜，下一请求跳过。

### 5.2 SSE 流检测与 stall

- **预读**：最多等 `stallTimeout`（capture-proxy 30s）拿首个真实 SSE 事件
  （`data:` / `event:`）。无真实数据 → 换栈。
- **预读错误**：预读事件含 `error` 字段 → 换栈。
- **token 节奏**：首个真实事件后，相邻真实事件间隔 `tokenGapTimeout`（10s）；
  工具流（`saw_tool`）放宽到 `toolStallTimeout`（180s 思考窗口）。
- **事件计数**：只有「推进内容」的事件（非空 content / tool_call /
  finish_reason / `[DONE]`）才刷新 stall 时钟；心跳事件不算，防止上游持续
  心跳喂饱检测。
- **提交后禁止 failover**：`WriteHeader(200)` + flush 首包后，任何失败
  （stall / mid-stream error / 客户端断开）都不允许切源，否则双写响应。
  用 `(ok, committed)` 返回值约定，仅 `committed=false` 可 failover。

### 5.3 意外中断 + 工具调用注入（对应「意外中断+条件=tools call (继续)」）

核心：请求带 `tools` 时（`recoverable=true`），若流未产出任何 tool_call 就
stall / EOF / error，则注入一个假的 `bash` tool_call 让客户端执行 `echo 继续`，
工具循环恢复而不是报错。

注入事件格式（SSE 三连，`finish_reason:"tool_calls"` + `[DONE]` 收尾）：

```
data: {"id":"idle",...,"choices":[{"index":0,"delta":{"tool_calls":[
      {"index":0,"id":"call_idle","type":"function",
       "function":{"name":"bash","arguments":"{\"command\":\"echo 继续\"}"}}]},
      "finish_reason":null}]}

data: {"id":"idle",...,"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]
```

各路径收尾：

| 路径 | 处理 |
|------|------|
| 上游 `[DONE]` 或 finish_reason 非 tool_calls | 正常转发 |
| 流 EOF / 硬断流 | recoverable 且无产出 → 注入 idle；saw_tool 未闭合 → seal `finishToolCall`；否则补 `[DONE]` |
| stall 超时 | recoverable 无产出 → 注入 idle 并成功返回；工具流 → `lengthChunk`（finish_reason:length）+ `[DONE]` |
| 已注入但上游 `[DONE]` 未到就 stall | 只补裸 `data: [DONE]` |
| 非 recoverable（无 tools）stall | 补发 UpstreamStall 错误事件 + `[DONE]` |

### 5.4 无限续命（zen-multi `-inf` 模型）

`deepseek-v4-flash-inf` 模型 + 带 tools 时（`canInject=true`）：

- 流正常收尾（finish_reason ≠ tool_calls）→ `neutralizeFinish` 清空
  finish_reason 后注入 `infInject`（`echo 请继续完善当前项目…` + `sleep 1800`）；
- 上游 `[DONE]` 到达但无 tool_call → 注入后再放行；
- EOF / 断流 → 注入 + 补 `[DONE]`；
- idle 超时 → 注入 `idleInject`（`echo 继续`）。

与 capture-proxy 的「recoverable 注入」层级不同：capture-proxy 兜底中断，
zen-multi 驱动 `-inf` 模型持续运转。

### 5.5 冷却策略（FreeUsageLimitError）

| 层级 | 策略 |
|------|------|
| capture-proxy | 该栈冷却到 **UTC 午夜**（`nextUTCMidnight`） |
| zen-multi（每源） | 升级：第 1 次 1min → 第 2 次 5min → ≥3 次锁到午夜；成功清零 |
| 连接失败（zen-multi） | 短冷却 60s |

设计原因：`FreeUsageLimitError` 可能是瞬时限流，不能一上来锁死一整天；
达到阈值（3 次）才视为真·配额耗尽。

### 5.6 抓包

`capture.save()`：每个请求写入 `--out/%06d_时间戳_路径.txt`，含
method / URL / proto / remote / host / 全部 header（排序）/ body。用于排查
「直连有反应 proxy 无反应」类问题——直接看抓包文件里客户端到底发了什么。

---

## 6. 部署现状（2026-08-11）

| 位置 | 组件 | 状态 |
|------|------|------|
| 本机 `:8443` | zen-multi（systemd `zen-multi.service`） | active，聚合 vps/bwh/cloudcone |
| 本机 `:8000` | capture-proxy（`--mode auto --out /tmp/opencode/captured`） | active，独立入口（opencode `zen-8000` 模型） |
| vps.moonchan.xyz:8443 | capture-proxy（`zen.service`，`--mode auto`） | active |
| c.moonchan.xyz:8443 | capture-proxy（`zen.service`，`--mode auto`） | active |
| bwh.moonchan.xyz:8443 | capture-proxy（`zen.service`） | **已停止**（手动） |

远程三台 systemd `zen.service`：

```
[Service]
Type=simple
ExecStart=/root/capture_proxy_go --listen 0.0.0.0:8443 --mode auto \
  --cert /root/.acme.sh/*.moonchan.xyz_ecc/fullchain.cer \
  --key  /root/.acme.sh/*.moonchan.xyz_ecc/*.moonchan.xyz.key \
  --out /root/captured
Restart=always
RestartSec=3
```

> 证书目录名含字面 `*`（acme.sh 产物），systemd 直接按字面解析。

opencode 配置（`~/.config/opencode/opencode.json`）：

```json
"zen-multi": { "options": { "baseURL": "http://127.0.0.1:8443/v1", "apiKey": "public" } }
```

---

## 7. 构建

```bash
# capture-proxy
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o capture_proxy_go ./scripts/capture_proxy.go

# zen-multi
CGO_ENABLED=0 go build -o /usr/local/bin/zen_multi_go ./cmd/zen-multi/

# 部署远程（例）
bash ~/script/ssh/vps.sh "cat > /root/capture_proxy_go.new" < capture_proxy_go
bash ~/script/ssh/vps.sh "systemctl stop zen && mv /root/capture_proxy_go.new /root/capture_proxy_go \
  && chmod +x /root/capture_proxy_go && systemctl start zen"
```

---

## 8. 运维速查

```bash
# 模式切换（同步所有源）
curl -s -X POST "http://127.0.0.1:8443/mode?mode=v4"     # zen-multi 聚合入口
curl -s -X POST "http://127.0.0.1:8000/mode?mode=v6"      # 单个 capture-proxy
curl -s http://127.0.0.1:8443/status                      # 各源 cooldown/token
curl -s http://127.0.0.1:8000/status                      # 分栈统计

# 远程单节点
bash ~/script/ssh/vps.sh "journalctl -u zen -n 30"
bash ~/script/ssh/vps.sh "ls /root/captured/ | tail"

# bwh 已停用; 如要恢复
bash ~/script/ssh/bwh.sh "systemctl start zen"
```
