# wintools

Go 网络工具集:多站点 ECH/SNI 反代、zen API 三重整合代理、多 IP 分流、WebRTC 隧道、本地 DoH、KV 存储、CORS chat proxy。

## 组件

| 组件 | 路径 | 说明 |
|------|------|------|
| ech-proxy | `cmd/ech-proxy` | 多站点反代:ECH 域前置 / SNI 伪装 / 直连三种出网模式,响应域名重写、通配入口、固定 cookie、SW 注入 |
| capture-proxy | `cmd/capture-proxy` | zen 三重整合单入口(zen-proxy + local-proxy-detected + 抓包):Ollama 兼容端点 → opencode.ai/zen/v1 |
| ip-proxy | `cmd/ip-proxy` | 多 IP 分流 CONNECT 代理(client + VPS 端 WS 隧道 server) |
| webrtc-proxy | `cmd/webrtc-proxy` | HTTP proxy over PeerJS-signaled WebRTC DataChannel(serve/client 双模式) |
| localdns | `cmd/localdns` | 本地 DNS 服务器,转发到 DoH 端点 |
| kv-store | `cmd/kv-store` | KV 存储 HTTP API(Gin,TTL) |
| opencode-proxy | `cmd/opencode-proxy` | OpenCode Go API 的 CORS chat proxy(仅放行 aichat.moonchan.xyz,带每日配额) |
| ech | `pkg/ech` | 可直接使用的 ECH 域前置 HTTP 客户端 |
| echproxy | `pkg/echproxy` | ech-proxy 的上游配置加载 / 证书下载 / 重写 / 代理 handler |
| netdial | `pkg/netdial` | Termux/Android 环境网络坑的公共修复(DNS 固定 + CA 附加) |
| peerjs | `pkg/peerjs` | PeerJS 公共云信令客户端(webrtc-proxy 用) |
| kv | `pkg/kv` | 并发安全的内存 KV 存储 |
| proxyheaders | `pkg/proxyheaders` | 请求/响应头透传工具 |

## 前置条件

- Go 1.26+(见 `go.mod`)
- CGO 可关闭,所有代理均支持静态编译

## ech-proxy

多站点反向代理(`l.moonchan.xyz` 系域名),配置单一来源为仓库内 `certs/l.moonchan.xyz/upstream.json`(启动时从 GitHub 拉取,含上游目标、出网模式、重写、通配、cookie、SW 注入、屏蔽列表)。TLS 证书同样在启动时自动拉取。

### 运行

```bash
go build -o ech-proxy ./cmd/ech-proxy/ && ./ech-proxy
```

### 命令行参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `-addr` | `0.0.0.0:8443` | 监听地址 |
| `-http` | `false` | HTTP 模式(不启用 TLS,本地代理) |

环境变量:

| 变量 | 说明 |
|------|------|
| `LOCALIP` | DoH 接入 IP(绕过本地 DNS 直连) |
| `IP_MODE` | IP 协议偏好,`v4` / `v6` / 空(自动) |

### upstream.json 配置

```jsonc
{
  "cert_path": "https://.../fullchain.cer",
  "key_path":  "https://.../privkey.pem",
  "upstreams": {
    "iwara.l.moonchan.xyz": {
      "host": "iwara.tv",          // 上游真实域名
      "mode": "sni",               // ""=ECH | sni | direct
      "referer": "https://iwara.tv/",
      "rewrites": { "iwara.tv": "iwara.l.moonchan.xyz" },   // 响应内域名替换
      "wildcard": { "prefix": "iwara-", "entry_suffix": ".l.moonchan.xyz", "upstream_suffix": ".iwara.tv" }
    },
    "dlsite.l.moonchan.xyz": {
      "host": "dlsite.com",
      "mode": "sni",
      "sw_inject": true            // 无 SW 站点注入拦截 SW(绝不可用于有 workbox 的站点)
    }
  },
  "blocked_hosts": ["..."]
}
```

- 出网模式:`""`/`ech` = Cloudflare ECH 域前置;`sni` = DoH 解析真实 IP + 假 SNI + Host 路由;`direct` = 普通 HTTPS 直连
- `wildcard` 通配入口:`iwara-img.l.moonchan.xyz` → `img.iwara.tv` 等,动态域名不用逐个配
- `sw_inject`:仅对**没有自己的 service worker** 的站点(dlsite)开启,代理在 HTML 注入 `/wt-sw.js` 注册并兜底返回拦截脚本;iwara 这类有 workbox 的站点绝不开启(注入会与 workbox 冲突导致整站崩溃)

### 测试

```bash
go test -race ./pkg/echproxy/ ./cmd/ech-proxy/
```

## capture-proxy

zen 家族三重整合单入口(旧 zen-proxy + local-proxy-detected + local-proxy + capture_proxy.go 合体),转发到 `opencode.ai/zen/v1`:

- **v6/v4 failover**:auto 先 v6 再 v4,请求 `?stack=v4|v6` 可指定栈
- **流检测/注入**:预读首 token、token 节奏 stall 检测、工具调用思考窗口、EOF/DONE 兜底封流、意外中断注入 `echo 继续` 恢复工具循环
- **冷却**:FreeUsageLimitError 三次递进冷却(1min / 5min / 锁到午夜)
- **抓包**:`--out dir` 把每个请求(method/URL/全部 header/body)存盘
- **usage 计费**:按模型累计 token/cost,`/stats` 查询
- **ban 限流**:`--ban` 按 IP 限流(200 req/10min)
- **gzip 请求体**:上游 gzip 先解压再转发
- **vanilla 透传**:`--detect=false` 不做流检测,仅转发 + 统计
- **TLS 监听**:`--cert/--key` 可选

### 运行

```bash
go build -o capture-proxy ./cmd/capture-proxy/
./capture-proxy --listen 127.0.0.1:8000 --mode auto [--out dir] [--detect=false] [--cert/--key]
```

控制 API:`GET /mode`(含冷却剩余)、`POST /mode`(v4|v6|auto 切换)、`GET /status`(按协议族统计)、`GET /stats`(按模型计费)。

## ip-proxy

多 IP 分流 CONNECT 代理:

- `client`:读 `client.json`(六线 `wss://vps|bwh|cloudcone-zen-v4|v6.moonchan.xyz`),本地开 HTTP CONNECT,opencode 设 `HTTPS_PROXY` 选线
- `server`:VPS 端 WS 隧道,`--force v4|v6` 指定出网栈,纯 ws 由 nginx 反代

详见 `cmd/ip-proxy/README.md`。

## webrtc-proxy

HTTP proxy over PeerJS 公共云信令 + WebRTC DataChannel:

- `serve`:注册固定 peer id `wt-<name>`,接收 DataConnection,把信道上的 HTTP 请求转发到本地 HTTP 目标
- `client`:随机 peer id,连接 `wt-<name>`,本地开 HTTP 端口,所有请求经 DataChannel 隧道

## localdns

```bash
go build -o localdns ./cmd/localdns/ && ./localdns -doh https://moonchan.xyz/doh -port 5353
```

## kv-store

```bash
go build -o kv-store ./cmd/kv-store/ && ./kv-store -port 8080 -ttl 0 -tick 30s
```

`-ttl 0` 表示永不过期。接口详见 `docs/api.md`。

## opencode-proxy

```bash
OPENCODE_GO_API_KEY=<key> ./opencode-proxy -addr 0.0.0.0:8000
```

`POST /chat/completion` → `https://opencode.ai/zen/go/v1/chat/completions`,CORS 仅放行 `https://aichat.moonchan.xyz`,key 从环境变量读(不写死),内置每日配额。

## Windows 编译

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o <name>.exe ./cmd/<name>/
```

推 `v*` tag 自动触发 GitHub Actions 全量编译并发布 release。

## 测试

```bash
go test -race ./...
```
