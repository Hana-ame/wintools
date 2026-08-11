# sukebei.nyaa.si 访问方案技术报告

> 结论先行:sukebei.nyaa.si **不在 Cloudflare 后面**（自有 VPS），ECH 域前置无效；
> 已落地 **SNI 伪装直连**（ech-proxy `--http` 模式，v1.7.8+）；
> 另有 **不依赖 SNI 伪装** 的 Cloudflare 反代 + ECH 方案（方案二），可完全避免自签证书问题。

---

## 1. 侦察阶段（实测数据）

### 1.1 DNS 污染判定

本地 DNS 与 DoH（`moonchan.xyz/doh`）结果对比：

| 解析来源 | sukebei.nyaa.si | 判定 |
|---|---|---|
| 本地 dig（污染） | 31.13.96.195 / 157.240.x（Facebook 段） | 伪造 |
| moonchan.xyz DoH | **198.251.89.38**（FranTech 卢森堡 VPS） | 真实 |
| 对照组 e-hentai.org（DoH） | 172.66.132.196（Cloudflare 段） | 真实 |

### 1.2 CDN / ECH 能力判定

- `sukebei.nyaa.si` HTTPS（type=65）记录：**无 Answer**（仅 SOA）→ 非 Cloudflare 托管，无 ECH 支持。
- `cloudflare-ech.com` HTTPS 记录：含 `ech=` SvcParam → 外壳可用。
- 结论：cloudflare-ech.com 的 ECH 域前置只能路由 **Cloudflare 自己的 zone**，对 sukebei 无效（实测 TLS 握手 `handshake failure`）。

### 1.3 阻断特征定位

| 探测 | 结果 | 含义 |
|---|---|---|
| TCP `198.251.89.38:443` | 通 | IP 未封锁 |
| curl SNI=sukebei.nyaa.si | 0.26s 秒断 | **SNI 阻断**（ClientHello 明文 SNI 触发 RST） |
| openssl 假 SNI | 握手成功（自签 CN=localhost） | 服务器不校验 SNI，按 Host 路由 |

**结论：卡点只有一个——ClientHello 里的明文 SNI。**

---

## 2. 方案一：SNI 伪装直连（已实现）

### 2.1 原理

```
浏览器/客户端
   │  Host: sukebei.nyaa.si
   ▼
本地 ech-proxy (127.0.0.1:8443)
   │  ① DoH 解析真实 IP: 198.251.89.38（绕过污染 DNS，TTL 缓存）
   │  ② TCP 直连 198.251.89.38:443
   │  ③ TLS ClientHello SNI = "cloudflare-ech.com"（不可疑域名，绕过 SNI 阻断）
   │  ④ HTTP Host: sukebei.nyaa.si（源站 nginx 按 Host 路由到 Sukebei）
   ▼
sukebei.nyaa.si 源站 (198.251.89.38)
```

关键点：**GFW 的 SNI 阻断只看 ClientHello 明文 SNI；源站 nginx 的路由只看 HTTP Host 头。** 两者解耦，中间夹一个"假 SNI + 真 Host"即可穿透。

### 2.2 实现细节（`pkg/echproxy/proxy.go`）

配置：`UpstreamConfig.Mode = "sni"`，与 ECH 模式共用同一张路由表：

```json
"sukebei.l.moonchan.xyz": {
    "host": "sukebei.nyaa.si",
    "mode": "sni"
}
```

链路四要素：

1. **DoH 解析** `resolveHostIP()`：`moonchan.xyz/doh?name=<host>&type=1`，取 A 记录，按 TTL 缓存（下限 60s / 上限 24h），每次重试前清缓存。
2. **假 SNI**：`tls.Config{ServerName: "cloudflare-ech.com"}`——对外是普通 HTTPS 流量，无任何特征。
3. **自签证书处理**：`InsecureSkipVerify: true`（源站证书 CN=localhost）。安全取舍：可换用固定证书公钥/指纹校验。
4. **ALPN 只声明 `http/1.1`**：实测 Go 客户端带 `h2` 扩展时被 RST 概率显著升高，去掉 h2 后 5/5 稳定。

概率性 RST 处理：失败 → 清 IP 缓存 → 重建连接重试 1 次。

### 2.3 实测数据（WSL 本地，污染网络）

| 客户端 | ClientHello 特征 | 成功率 |
|---|---|---|
| curl | 带 h2,http/1.1 ALPN | 1/3（概率 RST） |
| openssl s_client | 无 ALPN | 10/10 |
| **Go 客户端** | 仅 http/1.1 ALPN | **5/5** |

端到端：`GET /` → `HTTP/1.1 200 OK`，`<title>Browse :: Sukebei</title>`（真实 Sukebei 页面）。

### 2.4 限制与风险

- 依赖源站"按 Host 路由且不校验 SNI"的行为；若源站换成按 SNI 出证书（多证书站点）则需固定证书。
- `InsecureSkipVerify` 存在中间人风险（仅限代理与源站之间）。
- 只解决 SNI 阻断；若未来 IP 被封则失效。

---

## 3. 方案二：不需要 SNI 伪装 —— Cloudflare 反代 + ECH（未实现，备选）

### 3.1 原理

让 sukebei **看起来在 Cloudflare 后面**：用自己的 CF 域名挂一个反代（Worker / 规则回源），CF 边缘（海外）回源 sukebei VPS 不受 GFW 影响；本地客户端走 **ECH** 直连自己的 CF 域名——正规 TLS、证书由 CF 签发、无假 SNI、无自签证书。

```
浏览器 → 本地 ech-proxy → ECH(cloudflare-ech.com 外壳)
       → sukebei.l.moonchan.xyz (Cloudflare 边缘, 自有域名, 真证书)
       → CF Worker/规则 → sukebei.nyaa.si (198.251.89.38, 海外回源不受墙)
```

### 3.2 Worker 实现（约 20 行）

```js
export default {
  async fetch(req) {
    const url = new URL(req.url);
    url.hostname = "sukebei.nyaa.si";
    url.protocol = "https:";
    return fetch(url, {
      method: req.method,
      headers: req.headers,
      body: req.body,
      // 源站是自签证书,关掉 CF 回源的证书校验
      backend: { verificationMode: "off" },
    });
  },
};
```

在 Cloudflare 面板把 `sukebei.l.moonchan.xyz` 挂到该 Worker（Route 绑定），即可复用 ech-proxy 已有的 **ECH 模式**（`mode` 留空），本地代码零改动。

### 3.3 与 SNI 伪装对比

| 维度 | 方案一 SNI 伪装（已实现） | 方案二 CF 反代 + ECH（备选） |
|---|---|---|
| 依赖 | 仅 DoH + 源站行为 | Cloudflare 账号 + Worker 配额 |
| 证书 | 自签，需跳过校验 | CF 签发，正规校验 |
| 流量特征 | 普通 TLS 到国外 VPS IP | ECH 加密 SNI 到 CF |
| 客户端改动 | 已集成 | 无需改动（复用 ECH 模式） |
| 风险 | 源站改行为 / IP 被封 | CF 被封 / Worker 限额 |
| 延迟 | 直连最短 | 多一跳 CF |

**选型建议**：本地单机用方案一（零外部依赖）；想正规化/多人共享/防 IP 封锁用方案二。
