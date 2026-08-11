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

### zen (集成在 ech-proxy 内)
`zen.l.moonchan.xyz` 走完整 provider 逻辑(由旧 zen-proxy / local-proxy-detected / capture_proxy 合并):转发到 opencode.ai/zen/v1,模式 auto(**先 v6 再 v4**)/v4/v6,请求带 `?stack=v4|v6` 可绕过模式直走指定栈;流检测/注入、FreeUsageLimitError 三次递进冷却、gzip 请求体、usage 计费。三条路径通(`/zen/v1`、`/v1`、裸)。

### ip-proxy
多 IP 多出口 HTTP CONNECT 分流代理(为 l.moonchan.xyz / opencode 本地出口准备,只绑定 127.0.0.x):
- `client`:每个本地 IP 一条线路,JSON 配置(`client.json`,gitignore),opencode 设 `HTTPS_PROXY=http://127.0.0.x:7890` 选线;CONNECT opencode.ai 走该线出口(ws/wss),其他 host 直连
- `server`:部署在 VPS 的 WS 隧道接收端(`/connect?target=`),拨号真实目标可 `--force v4|v6`(v4/v6 双栈独立额度),纯 ws 监听,TLS 由 nginx 终止

---

> `ech-shared` 是 CGo 共享库(供 Android 侧调用),不产出 exe。
