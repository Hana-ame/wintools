# ip-proxy 使用说明 (opencode 多线分流)

多 IP 出口分流 CONNECT 代理: 本机 `client` 监听多个本地地址,
每个地址对应一条出口(WS 隧道), 给 opencode 等工具做 HTTPS 代理选线。

## 架构

```
opencode → client(本机 CONNECT 代理) → wss://<vps>-zen-<v4|v6>.moonchan.xyz
                                       → server(VPS 端 WS 隧道) → 目标站
```

- `client`: 本机跑, 监听 `127.0.x.y:7890`, 支持 CONNECT (HTTPS 代理)
- `server`: VPS 上跑, 收 WS 连接后替 client 建到目标的 TCP 连接

## 文件

- `client.json`: 监听地址 → 出口 映射 (已 gitignore, 按需改)
- 六线出口 (三台 VPS × 双栈):
  | 线 | 本地监听 | 出口 |
  |---|---|---|
  | vps v4 | `127.0.1.4:7890` | `wss://vps-zen-v4.moonchan.xyz` |
  | vps v6 | `127.0.1.6:7890` | `wss://vps-zen-v6.moonchan.xyz` |
  | bwh v4 | `127.0.2.4:7890` | `wss://bwh-zen-v4.moonchan.xyz` |
  | bwh v6 | `127.0.2.6:7890` | `wss://bwh-zen-v6.moonchan.xyz` |
  | cloudcone v4 | `127.0.3.4:7890` | `wss://cloudcone-zen-v4.moonchan.xyz` |
  | cloudcone v6 | `127.0.3.6:7890` | `wss://cloudcone-zen-v6.moonchan.xyz` |

  WS 域名由 nginx 反代到 VPS 上的 server (loopback 127.26.8.12/13:8080)。

## 启动 client (Windows / Linux)

```bash
ip-proxy -role client -config client.json
```

## 给 opencode 用

opencode 支持 `HTTPS_PROXY` 环境变量。选一条线启动:

```bash
# 例: vps v4 线
HTTPS_PROXY=http://127.0.1.4:7890 opencode

# 例: bwh v6 线
HTTPS_PROXY=http://127.0.2.6:7890 opencode
```

- 换线 = 换环境变量里的监听地址, 重启 opencode
- 本机 hosts 无需配置, client 监听的是 127.0.x.y 虚 IP

## VPS 端 server (仅部署时需要)

```bash
# 纯 WS, 由 nginx 反代 (--force 指定栈)
ip-proxy -role server -listen 127.26.8.12:8080 --force v4
ip-proxy -role server -listen 127.26.8.13:8080 --force v6
```
