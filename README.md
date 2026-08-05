# wintools

Go 网络工具集：ECH 域前置反向代理、本地 DoH DNS 解析器、KV 存储 API 服务、zen 代理栈、WebRTC 演示。

## 组件

| 组件 | 路径 | 说明 |
|------|------|------|
| ech-proxy | `cmd/ech-proxy` | 基于 ECH (Encrypted Client Hello) 域前置的反向代理 |
| localdns | `cmd/localdns` | 本地 DNS 服务器，转发到 DoH 端点 |
| api-server | `cmd/api-server` | KV 存储 HTTP API（Gin） |
| zen-proxy / zen-multi / local-proxy | `cmd/zen-proxy` 等 | opencode.ai 免费 zen 端点代理栈 |
| ech 客户端库 | `pkg/ech` | 可直接使用的 ECH 域前置 HTTP 客户端 |
| echproxy | `pkg/echproxy` | ech-proxy 的上游配置加载 / 证书下载 / 代理 handler |
| kv | `pkg/kv` | 并发安全的内存 KV 存储 |
| apifwd | `pkg/apifwd` | Zen 转发库（Gin，v4/v6 显式 IP） |

详细文档：

- `docs/zen-proxy.md` — zen 代理栈架构 / 构建 / 部署
- `docs/api.md` — KV API 接口说明
- `docs/webrtc-intro.md` — WebRTC 概念与演示

## 前置条件

- Go 1.26+（见 `go.mod`）
- CGO 可关闭，所有代理均支持静态编译

## ech-proxy

通过 Cloudflare 的 ECH 基础设施，将请求转发到目标域名。TLS 证书会在启动时自动从 `proxy.moonchan.xyz` 下载到 `certs/l.moonchan.xyz/`。

### 运行

```bash
go run ./cmd/ech-proxy/
# 或
go build -o ech-proxy ./cmd/ech-proxy/ && ./ech-proxy
```

### 命令行参数

| Flag | 默认值 | 说明 |
|------|--------|------|
| `-addr` | `0.0.0.0:8443` | 监听地址 |
| `-cert` | `certs/l.moonchan.xyz/fullchain.cer` | TLS 证书文件 |
| `-key` | `certs/l.moonchan.xyz/privkey.pem` | TLS 密钥文件 |
| `-http` | `false` | HTTP 模式（不启用 TLS，本地代理），使用内置上游配置 |

环境变量：

| 变量 | 说明 |
|------|------|
| `LOCALIP` | DoH 接入 IP（绕过本地 DNS 直连） |
| `IP_MODE` | IP 协议偏好，`v4` / `v6` / 空（自动） |
| `ZEN_API_KEY` | `zen.l.moonchan.xyz` 的 Bearer key，默认 `public` |

### Windows 编译

```bash
GOOS=windows GOARCH=amd64 go build -o ech-proxy.exe ./cmd/ech-proxy/
```

### 测试

```bash
# 本地测试（跳过证书校验）
curl -k -x "" --resolve "l.moonchan.xyz:8443:127.0.0.1" "https://l.moonchan.xyz:8443/favicon.ico"

# 或从其他机器
curl -k "https://l.moonchan.xyz:8443/favicon.ico"
```

## ECH 客户端库

`pkg/ech` 提供可直接使用的 ECH 域前置 HTTP 客户端：

```go
import cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"

req, _ := http.NewRequest("GET", "https://example.com/", nil)
resp, err := cloudflare_ech.Do(req)
```

可用配置函数：`InitDefault`、`SetDohURL`、`SetDoHConfig`、`SetIPMode`、`CheckDualStack`。

## localdns

```bash
go run ./cmd/localdns -doh https://moonchan.xyz/doh -port 5353
# 或使用脚本（DOH_ENDPOINT 环境变量覆盖端点）
./run_localdns.sh
```

## api-server

```bash
go run ./cmd/api-server -port 8080 -ttl 0 -tick 30s
```

`-ttl 0` 表示永不过期。接口详见 `docs/api.md`。

## 测试

```bash
go test -race ./...
```
