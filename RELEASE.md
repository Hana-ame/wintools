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

### kv-store
内存键值存储 HTTP API(默认 :8080)。`/kv/*` 读写,支持 TTL 过期(0=不过期),带 `/healthz`。

### localdns
本地 DoH 转发 DNS 服务器(默认 UDP :5353)。把 DNS 查询转发到 DoH 端点(默认 `https://moonchan.xyz/doh`),用于绕过被污染的本地 DNS。

### capture-proxy
zen 家族(zen-proxy / local-proxy-detected / local-proxy / zen-multi / scripts/capture_proxy.go)合并后的单一程序,`-role` 分两种:
- `provider`(默认):Ollama 兼容端点,转发到 opencode.ai/zen/v1。模式 `--mode`/`/mode`:auto(**先 v6 再 v4**)/v4/v6;请求带 `?stack=v4|v6` 可绕过模式直接走指定栈;流检测(首 token 等待、token 节奏 stall 检测、180s 工具思考窗口、EOF 封流、断流注入 idle 工具调用)、FreeUsageLimitError 三次递进冷却(1min → 5min → 锁午夜)、`--detect=false` vanilla 纯透传、`--out` 抓包、`/status` 统计、usage 计费、可选 `--cert/--key` TLS。
- `multi`:多源聚合器,转发到多台 provider 的 /chat/completions,**不 failover**——模型必须带源名 `deepseek-v4-flash-<name>`,只打该源,不带源 400,失败即报错(502/429/404)。带 cooldown、用量统计。源列表暂时硬编码在 multi.go 顶部 `defaultSources`(3 台 VPS × v4/v6),无 `--source` 参数。

---

> `ech-shared` 是 CGo 共享库(供 Android 侧调用),不产出 exe。
