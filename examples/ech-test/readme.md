# ECH 域前置测试工具

## 概述

本示例是一个独立的 ECH（Encrypted Client Hello，加密客户端问候）域前置测试工具。它不依赖项目中任何其他包，完整实现了从 DoH（DNS over HTTPS）获取 ECH 配置、建立 ECH-over-TLS 连接、发送 HTTP 请求并验证 ECH 是否被目标服务器接受的完整流程。

ECH 是一种 TLS 扩展，用于加密 Client Hello 握手消息中的敏感字段，特别是 SNI（Server Name Indication，服务器名称指示）。SNI 会在握手早期以明文方式暴露用户正在访问的网站域名。ECH 通过对整个 Client Hello 进行加密，只有通过 DNS HTTPS 记录获得的公钥持有者（即真实目标服务器）才能解密。Cloudflare 是全球最大的 ECH 部署方之一，`cloudflare-ech.com` 是 Cloudflare 提供的 ECH 测试域名。

本工具的核心价值在于：
- 独立验证 ECH 配置获取链路（DoH → HTTPS 记录 → SVCB 解析 → ECH 配置提取）
- 独立验证 ECH-over-TLS 握手是否被 Cloudflare 的 ECH 终结器接受
- 为调试 ECH 连接问题提供详细的逐步骤输出

## 架构设计

本工具的设计遵循以下原则：

1. **零外部依赖**：除了 Go 标准库外，不依赖任何第三方包。所有的 DNS 解析、SVCB 记录解析、Base64/Hex 解码和正则表达式提取都是手写实现。

2. **双格式支持**：ECH 配置在 DNS HTTPS 记录中可能以两种格式出现 — Base64 编码的 `ech` SvcParam 值（格式如 `ech="ABc123..."`），或者以 `\#` 前缀的十六进制线格式（RFC 9460 原始格式）。本工具同时支持这两种格式，可以覆盖绝大多数 DNS 服务器的返回模式。

3. **缓存机制**：获取到的 ECH 配置会以域名+TTL 的方式缓存在内存中，避免重复查询。缓存是协程安全的，使用 `sync.Mutex` 保护。

4. **逐步骤日志**：每一个操作阶段（DoH 查询 → SVCB 解析 → TCP 连接 → TLS 握手 → HTTP 请求）都有详细的结构化日志输出，方便快速定位故障点。

## 核心流程

### 1. ECH 配置获取

ECH 配置的获取是一个多阶段过程，通过 DNS HTTPS 记录（Type 65）查询到目标域名的 ECH 公钥：

```
fetchECHConfig(domain)
    ├── 检查内存缓存 → 命中直接返回
    ├── 构造 DoH JSON 请求
    │   └── GET https://moonchan.xyz/doh?name=<domain>&type=65
    │   └── Accept: application/dns-json
    ├── 解析 JSON 响应
    │   └── 遍历 Answer[]，过滤 type=65 的记录
    ├── 解析 SVCB 记录
    │   ├── Base64 格式: ech="base64data" → base64.StdEncoding.DecodeString
    │   └── 十六进制线格式: \# 13 002000050100... →
    │       ├── hex.DecodeString → wire bytes
    │       └── parseSVCBWire → 遍历 SvcParams，提取 key=5 (ech)
    ├── 缓存结果 (TTL 60s ~ 24h)
    └── 返回 ECH 配置列表 ([]byte)
```

关键函数 `parseSVCBWire` 负责解析 RFC 9460 定义的 SVCB/HTTPS 记录的线格式。该格式的结构如下：

```
+--------+--------+-------------------+--------+-----------+
| Prio.  | Target | SvcParam-key=val  | SvcParam-key=val | ...
+--------+--------+-------------------+--------+-----------+
  2 bytes  DNS name  (key 2b)(len 2b)(val len)
```

`parseSVCBWire` 的实现步骤：
1. 跳过 Priority 字段（2 bytes）
2. 跳过 Target Name（以 label-length-0 结尾的 DNS 名字序列）
3. 遍历剩余的 SvcParams，每 4 bytes 为 key+len 头，后跟 valLen bytes 的值
4. 当 key == 5（ECH SvcParam key）时返回对应的值为 ECH 配置

### 2. TLS 连接

TLS 连接是本工具的核心验证环节。与普通 HTTPS 客户端不同，本工具使用自定义的 `DialTLSContext` 拦截器：

```
DialTLSContext(ctx, network, addr)
    ├── 解析 addr 获得目标域名 host
    ├── 实际 TCP 连接 cloudflare-ech.com:443（伪装外壳）
    ├── 构造 tls.Config
    │   ├── ServerName = host（真正的目标域名，用于 Inner SNI）
    │   ├── EncryptedClientHelloConfigList = echConfig（从 DoH 获取的 ECH 公钥）
    │   ├── MinVersion = tls.VersionTLS13（ECH 需要 TLS 1.3+）
    │   └── NextProtos = ["h2", "http/1.1"]（支持 HTTP/2）
    ├── tls.Client(rawConn, tlsCfg)
    ├── tlsConn.HandshakeContext(ctx)
    └── 检查 tlsConn.ConnectionState().ECHAccepted
        ├── true  → ECH 握手成功
        └── false → ECH 握手失败或降级
```

ECH 的工作原理决定了为什么需要这样的连接方式：

1. **外层 SNI**：客户端在 TCP 连接建立后，发送 TLS Client Hello 时，外层 Client Hello 中的 SNI 字段设置为 shell 域名（`cloudflare-ech.com`）。这一层是明文传输的，中间人可以看到 "cloudflare-ech.com"，但看不到真正的目标。

2. **内层 SNI（加密）**：真正的目标域名（如 `video-cf.twimg.com`）被加密在 ECH 扩展的 ClientHelloInner 中。只有拥有对应 ECH 私钥的 Cloudflare 边缘服务器才能解密这个内层。

3. **路由决策**：Cloudflare 边缘节点收到 TLS Client Hello 后，如果成功解密 ECH，就根据内层 SNI 将连接路由到对应的源站（如 Twitter 的 CDN）；如果解密失败，则回退到外层 SNI 的路由（通常是 ECH 的测试页或 404）。

### 3. HTTP 请求

TLS 连接建立后，本工具使用标准的 Go HTTP 客户端发起请求。但由于传输层已经被 `DialTLSContext` 拦截，所有请求实际上都是通过已经建立的 ECH 隧道发出的：

```
HTTP GET https://video-cf.twimg.com/
    ├── Transport.DialTLSContext 被调用
    ├── 建立到 cloudflare-ech.com:443 的 ECH-over-TLS 连接
    ├── 发送 HTTP 请求（真正的 Host 头 = video-cf.twimg.com）
    ├── Cloudflare 边缘解密 ECH → 根据内层 SNI 路由
    ├── 请求到达 Twitter 的 CDN
    └── 响应返回客户端
```

## 输出示例

运行本工具时的典型输出（ECH 握手成功的情况）：

```
[Init] 正在获取全局伪装外壳 cloudflare-ech.com 的 ECH 密钥...
[Init] 成功获取外壳 ECH 密钥！(长度: XX bytes)

--- 开始发起 HTTP 请求: https://e-hentai.org/ ---

[TCP] 拦截拨号: 目标网站 [e-hentai.org] -> 实际建立 TCP 连接至 [cloudflare-ech.com:443]
[TLS] >>> 成功！服务器通过了 ECH 解密，Inner SNI: e-hentai.org 路由成功。
[HTTP] 状态码: 200 OK
[HTTP] 协议: HTTP/2.0
[HTTP] 内容截取: <!DOCTYPE html>...
```

如果 ECH 握手失败，输出为：

```
[TLS] >>> 失败或降级！服务器未能使用 ECH。
```

## 与 pkg/ech 库的关系

本示例中的代码逻辑与 `pkg/ech` 库的功能重叠，但有以下区别：

| 方面 | 本示例 | pkg/ech 库 |
|------|--------|-----------|
| 定位 | 独立测试工具，一次性验证 | 可复用的生产库 |
| 依赖 | 零外部依赖 | 零外部依赖 |
| 缓存 | 简单的 sync.Mutex + map | atomic.Pointer + sync.Mutex |
| 刷新 | 无自动刷新 | 5 分钟自动刷新协程 |
| 多域名 | 遍历多个目标 | 单默认客户端 |
| 用途 | 手动测试 ECH 连接 | 生产环境 ECH 域前置 |

## 使用方式

```bash
# 直接运行
go run ./examples/ech-test/
```

## 测试的域名

本工具默认测试两个域名：
- `e-hentai.org` — 基于 Cloudflare 的网站，支持 ECH
- `video-cf.twimg.com` — Twitter/X 的视频 CDN，经过 Cloudflare 并由 Twitter 使用 ECH

这两个域名都位于 Cloudflare 网络之后，应支持 ECH 连接。测试结果可以反映当前的 ECH 生态兼容性。

## 常见问题

1. **ECH 握手总是失败**：可能原因包括 DNS HTTPS 记录尚未传播、目标域名未启用 ECH、或网络中间设备拦截了 ECH 扩展。可以尝试更换 DNS 递归服务器。

2. **DoH 查询返回空**：如果 `moonchan.xyz/doh` 不可达，可以修改 `reqURL` 为其他公共 DoH 服务器（如 `https://cloudflare-dns.com/dns-query`），注意调整请求格式（有些 DoH 需要 POST + `application/dns-message` 格式）。

3. **ECHAccepted 始终为 false**：即使 ECH 握手失败，TLS 连接仍然可能建立（降级模式）。这时 HTTP 请求仍然能完成，但 SNI 暴露为 shell 域名而非目标域名。Cloudflare 可能返回默认页而不是目标网站内容。

## 代码结构

```
examples/ech-test/
├── readme.md          # 本文档
└── main.go            # ECH 测试入口
    ├── DoHResponse    # DNS-over-HTTPS JSON 响应的数据结构
    ├── echEntry       # ECH 配置缓存条目
    ├── getCachedECH   # 从缓存读取 ECH 配置
    ├── setCachedECH   # 写入 ECH 配置到缓存
    ├── parseSVCBWire  # 解析 SVCB 记录线格式
    ├── fetchECHConfig  # 通过 DoH 获取 ECH 配置主流程
    └── main            # 入口：测试多个目标域名的 ECH 连接
```
