# wintools exe 功能说明

## cmd/*(每个都是独立可执行程序)

### ech-proxy
ECH 域前置 + SNI 伪装反向代理。两种模式:
- `--http` 本地模式:监听 0.0.0.0:8443,按 Host 路由转发
  - `l.moonchan.xyz` / `twimg.l.moonchan.xyz` → Twitter 媒体 (ECH)
  - `ex.l.moonchan.xyz` → EXHentai (ECH)
  - `sukebei.l.moonchan.xyz` → sukebei.nyaa.si (SNI 伪装,绕过 DNS 污染 + SNI 阻断)
  - `zen.l.moonchan.xyz` → opencode.ai Zen API
- TLS 远端模式:证书/密钥/上游配置每次启动经 proxy.moonchan.xyz 拉到内存,不落盘
- 内置内存 cookie jar:上游 Set-Cookie 自动保存并随后续请求回传

### api-server
内存键值存储 HTTP API(默认 :8080)。`/kv/*` 读写,支持 TTL 过期(0=不过期),带 `/healthz`。

### localdns
本地 DoH 转发 DNS 服务器(默认 UDP :5353)。把 DNS 查询转发到 DoH 端点(默认 `https://moonchan.xyz/doh`),用于绕过被污染的本地 DNS。

### local-proxy-detected
Ollama 兼容端点(默认 :11434),转发到 opencode.ai/zen/v1。双栈 v6/v4 failover + 流检测(首 token 等待、token 节奏 stall 检测、180s 工具思考窗口、EOF 封流、断流注入 idle 工具调用)。

### zen-multi
本地多源聚合器,转发到多台 zen-proxy(bwh/vps/cloudcone)的 /v1/chat/completions,带 failover、cooldown、用量统计。

### zen-proxy
转发 /chat/completions 到 opencode.ai 的免费 zen 端点。支持 http/https/auto 三种监听模式,failover/cooldown/stall 语义,Go 流式处理。

## scripts/*.go(单文件工具)

### capture-proxy
本地双栈(v4/v6)转发代理,抓取并转发 opencode.ai/zen/v1。默认纯 HTTP(本地用),也可 `--cert/--key` 起 TLS。

---

> `ech-shared` 是 CGo 共享库(供 Android 侧调用),不产出 exe。
