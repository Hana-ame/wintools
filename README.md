# wintools

## ech-proxy

基于 ECH (Encrypted Client Hello) 域前置的反向代理。通过 Cloudflare 的 ECH 基础设施，将请求转发到被墙的目标域名。

### 前置条件

- Go 1.23+
- TLS 证书（`certs/l.moonchan.xyz/`）

### 从 VPS 拉取证书

```bash
mkdir -p certs/l.moonchan.xyz
~/script/ssh/vps.sh "cat ~/.acme.sh/'*.l.moonchan.xyz_ecc'/fullchain.cer" > certs/l.moonchan.xyz/fullchain.cer
~/script/ssh/vps.sh "cat ~/.acme.sh/'*.l.moonchan.xyz_ecc'/'*.l.moonchan.xyz.key'" > certs/l.moonchan.xyz/privkey.pem
```

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
| `-domain` | `l.moonchan.xyz` | TLS 域名（仅日志） |
| `-cert` | `certs/l.moonchan.xyz/fullchain.cer` | TLS 证书文件 |
| `-key` | `certs/l.moonchan.xyz/privkey.pem` | TLS 密钥文件 |
| `-upstream` | `video-cf.twimg.com` | 上游目标域名 |

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

### ECH 客户端库

`cloudflare_ech` 包提供可直接使用的 ECH 域前置 HTTP 客户端：

```go
import "github.com/Hana-ame/wintools/cloudflare_ech"

req, _ := http.NewRequest("GET", "https://example.com/", nil)
resp, err := cloudflare_ech.Do(req)
```
