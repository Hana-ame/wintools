# peerfs 代码结构

> 2026-09 实测。全部路径相对于仓库根目录 `/mnt/d/Workplace/wintools`。
> 行数用 `wc -l` 实测，非估算。

---

## 1. 项目总览

```
wintools/                          单 Go 模块（go.mod，peerfs-chat 已并入）
├── cmd/            13 个可执行程序（CI 全量 build，push v* tag 发布）
├── pkg/            11 个库包
├── peerfs-chat/    peerfs 原型（信令服务器 + Go 客户端 + standalone 入口）
├── docs/           设计文档 + 研究文档
├── examples/       示例程序（ech-test、ipc、webrtc-demo）
├── scripts/        运维脚本
├── captured/       抓包数据（运行时，非代码）
├── certs/          证书（运行时，非代码）
├── .github/workflows/             CI/CD（go.yml、release.yml、ech-proxy.yml）
├── go.mod / go.sum
├── AGENTS.md      项目约定
├── README.md
└── RELEASE.md     发布流程
```

---

## 2. `cmd/` — 可执行程序

### peerfs 相关

| 程序 | 文件 | 行数 | 说明 |
|---|---|---|---|
| **`media-node`** | `cmd/media-node/main.go` | 124 | Go peer 节点入口。`-signal` 标志控制是否内嵌信令服务器；`-root` 指定文件根目录；`-shost`/`-sport` 指定远程信令地址 |
| **`peerfs-proxy`** | `cmd/peerfs-proxy/main.go` | 325 | peerfs 代理入口 |

### 其他程序

| 程序 | 主要文件 | 行数 | 说明 |
|---|---|---|---|
| `capture-proxy` | `local.go` + `main.go` | 1,840 + 24 | zen 家族三重整合（zen-proxy + local-proxy-detected + local-proxy + scripts/capture_proxy.go） |
| `echcheck` | `main.go` | 76 | ECH 检查工具 |
| `ech-proxy` | `main.go` + `static/index.html` | 201 + 751 | 站点反代（Go 逻辑 + 静态 UI） |
| `ech-shared` | `shared.go` | 340 | ech-proxy 公共库 |
| `ip-proxy` | `client.go` + `server.go` + `main.go` | 252 + 171 + 54 | IP 代理 |
| `kv-store` | `main.go` | 42 | 键值存储 |
| `localdns` | `main.go` | 18 | 本地 DNS |
| `multi-proxy` | `main.go` | 501 | 多协议代理 |
| `opencode-proxy` | `main.go` | 359 | AI chat CORS 代理 |
| `twitter-downloader` | `main.go` | 317 | Twitter 下载器 |
| `webrtc-proxy` | `main.go` | 614 | WebRTC 代理隧道 |

---

## 3. `pkg/` — 库包

### 3.1 `pkg/peerjs/` — WebRTC 传输层（Pion）

| 文件 | 行数 | 关键内容 |
|---|---|---|
| `peer.go` | 551 | `Peer` 结构；**`CreateOffer()` 在 `:400`**（唯一的 offer 创建点，Go 永远是 offerer）；`Open()`、`Close()`、信令对接、ICE 候选注入 |
| `connection.go` | 500 | `DataConnection` 封装；`SendText()`、`SendBinary()`、`SendThrottled()`（SCTP 流控）；`Close()` 清理 |
| `peer_test.go` | 31 | 测试 |
| `token_test.go` | 29 | 测试 |

### 3.2 `pkg/peerfs/` — 文件服务层

| 文件 | 行数 | 关键内容 |
|---|---|---|
| `node.go` | 492 | **核心文件**。协议定义与实现：<br>`Header` 结构 `:45`（`UnmarshalJSON` 在 `:59`）<br>`Entry` 结构 `:70`<br>`Config` 结构 `:85`（`PeerID`/`Root`/`Token`/`Signaling`/`ICEHook`）<br>**`Node` 结构 `:97`**；`New()` `:109`<br>`startAnnounceLoop()` `:189`（周期性 announce + 拉取节点列表）<br>`doAnnounce()` `:215`<br>`Start()` `:265`；`Close()` `:297`<br>`connState` 结构 `:305`；`frameWriter` 接口 `:312`<br>`onConn()` `:318`；**`onMessage()` `:337`**（消息分发：`ping`/`pong`/`list`/`read`）<br>`handleList()` `:376`；**`handleRead()` `:414`**（支持 offset、负数 offset 读 moov box `:437-443`）<br>`replyErr()` `:486` |
| `signaling.go` | 40 | `MountSignaling(mux, key, tokens)` `:32`——把信令服务器挂进任意 `http.ServeMux`，挂载 `/peerjs`、`/peerjs/id`、`/discover/announce`、`/discover/nodes` |
| `console.go` | 164 | 浏览器控制台 HTTP handler（内嵌 HTML/JS） |
| `integration_test.go` | 210 | 集成测试 |
| `node_test.go` | 279 | 单元测试 |

### 3.3 `pkg/peerfs/web/` — Web peer（浏览器端）

| 文件 | 行数 | 关键内容 |
|---|---|---|
| **`bridge.js`** | **801** | **PeerFS SDK。P0 要改的就是这个文件。**<br>`PeerFS(opts)` 构造 `:40`<br>`connect()` `:133`（信令 + WebRTC 连接）<br>`rdc.onopen` `:188`（发 `hello`）<br>`rdc.onmessage` `:203`（消息入口）<br>`_createPrimaryWorker()` `:229`<br>**`_onWorkerData()` `:298`**（消息分发器）<br>`_releaseWorker()` `:353`<br>`_acquireWorker()` `:363`<br>`list()` / `read()` API `:500+`<br>**缺口**：无 `announce`、无 `list`/`read` 处理器、无 `discover` |
| `index.html` | 493 | 浏览器控制台 UI |
| `sw.js` | 185 | Service Worker——虚拟 HTTP 206 管道（边下边播 + Seek） |
| `peerjs.min.js` | 8 | vendored peerjs 1.5.4（压缩；**无许可声明**，§12.0 已记录） |

### 3.4 其他库包

| 包 | 主要文件 | 行数 | 说明 |
|---|---|---|---|
| `echproxy/` | `proxy.go` | 1,392 | ECH 代理核心逻辑 |
| `ech/` | `client.go` | 463 | ECH 客户端 |
| `kv/` | `store.go` | 304 | 键值存储 |
| `localdns/` | `localdns.go` | 140 | 本地 DNS |
| `apifwd/` | `fwd.go` | 161 | API 转发 |
| `api/` | `handler.go` | 104 | API handler |
| `netdial/` | `netdial.go` | 129 | **Termux/Android 网络修复**（DNS 固定公共解析 + CA 附加 `$PREFIX/etc/tls/cert.pem`）。任何在 Termux 上跑、有出站请求的 proxy 一律用此包 |
| `proxyheaders/` | `proxyheaders.go` | 99 | 请求/响应头透传 |

---

## 4. `peerfs-chat/` — peerfs 原型

> 已并入根模块（原 `peerfs-chat/go.mod` 已删）。构建运行一律从仓库根：
> `go run ./peerfs-chat/server`、`go build ./peerfs-chat/goclient`。

### 4.1 信令服务器

| 文件 | 行数 | 说明 |
|---|---|---|
| `local/signalserver/go.mod` | 2 | `module github.com/Hana-ame/go-peerserver`。根 `go.mod:69` 有 `replace` 指向此目录 |
| **`local/signalserver/signalserver.go`** | **485** | **信令服务器唯一实现。**<br>`NewServer(key, opts...)`<br>`HandleWS()` — WebSocket 信令（OFFER/ANSWER/CANDIDATE/LEAVE 按 `dst` 转发）<br>`HandleID()` — 随机 ID 分配<br>`HandleAnnounce()` `:356` — 节点注册（`peerId` + `collections` + `nodeType` + `uptime` + 上下行字节）<br>`HandleNodes()` — 在线节点查询<br>离线队列（`dst` 不在线入队，带过期）<br>心跳保活（5s）<br>token 白名单 |

### 4.2 入口

| 文件 | 行数 | 说明 |
|---|---|---|
| `server/main.go` | 48 | **standalone 信令服务器入口**。手动挂载 `/peerjs`、`/peerjs/id`、`/discover/announce`、`/discover/nodes` + 静态文件 |
| `goclient/main.go` | 498 | Go 客户端（peerfs-chat 原型） |

### 4.3 文档与页面

| 文件 | 行数 | 说明 |
|---|---|---|
| `README.md` | 497 | peerfs-chat 说明 |
| `docs/peerjs-file-loading.md` | 430 | 设计文档 |
| `web/index.html` | 407 | 旧版浏览器页面（已被 `pkg/peerfs/web/index.html` 取代） |

---

## 5. `docs/` — 文档

| 文件 | 行数 | 说明 |
|---|---|---|
| `peerfs.md` | 426 | peerfs 设计文档：架构、六大机制、协议规范、快速启动、接入教程 |
| **`research/peerfs-compare.md`** | **1,377** | **10 项目对比研究**（§12–§17：许可可行性、组件矩阵、六步流水线、移植清单、目标架构评估） |
| `peerfs-code-structure.md` | — | **本文档** |
| `peerfs-center-server.md` | 216 | 中心服务器设计 |
| `webrtc-intro.md` | 144 | WebRTC 入门 |
| `webrtc-proxy.md` | 157 | WebRTC 代理隧道 |
| `api.md` | 104 | API 文档 |
| `CHANGELOG.md` | 45 | 变更日志 |
| `research/freegpt.md` | 64 | 研究文档 |

---

## 6. `examples/` — 示例

| 目录 | 文件 | 行数 |
|---|---|---|
| `ech-test/` | `main.go` + `readme.md` | 273 + 178 |
| `ipc-async/` | `manager/main.go` + `worker/main.go` + `readme.md` | 186 + 108 + 356 |
| `ipc-example/` | `manager/main.go` + `worker/main.go` + `readme.md` | 216 + 133 + 269 |
| `webrtc-demo/` | `main.go` + `README.md` | 221 + 77 |

---

## 7. `scripts/` — 脚本

| 文件 | 行数 | 说明 |
|---|---|---|
| `run-peerjs-demo.sh` | 138 | peerjs demo 启动脚本 |
| `usage_report.py` | 171 | 用量报告 |
| `restore_demo_media.sh` | 80 | 恢复 demo 媒体文件 |

---

## 8. peerfs 相关代码速查表

### 8.1 按角色

| 角色 | 文件 | 行数 |
|---|---|---|
| **Go peer**（传输） | `pkg/peerjs/peer.go` | 551 |
| **Go peer**（传输） | `pkg/peerjs/connection.go` | 500 |
| **Go peer**（文件服务） | `pkg/peerfs/node.go` | 492 |
| **Go peer**（信令挂载） | `pkg/peerfs/signaling.go` | 40 |
| **信令服务器** | `peerfs-chat/local/signalserver/signalserver.go` | 485 |
| **Go 入口** | `cmd/media-node/main.go` | 124 |
| **Go 客户端** | `peerfs-chat/goclient/main.go` | 498 |
| **standalone 信令入口** | `peerfs-chat/server/main.go` | 48 |
| **Web peer** | `pkg/peerfs/web/bridge.js` | 801 |
| **Web UI** | `pkg/peerfs/web/index.html` | 493 |
| **Service Worker** | `pkg/peerfs/web/sw.js` | 185 |
| **vendored peerjs** | `pkg/peerfs/web/peerjs.min.js` | 8（压缩） |

### 8.2 按部署模式

| 模式 | 进程 | 涉及文件 |
|---|---|---|
| **Standalone 信令 + Go 节点** | `peerfs-chat/server`（信令）+ `cmd/media-node -signal=false`（节点） | `peerfs-chat/server/main.go` + `cmd/media-node/main.go` + `pkg/peerfs/*` + `pkg/peerjs/*` |
| **内嵌信令（单进程）** | `cmd/media-node -signal`（信令 + 节点 + 页面） | `cmd/media-node/main.go` + `pkg/peerfs/signaling.go` + `pkg/peerfs/node.go` + `pkg/peerjs/*` |

### 8.3 按协议流

```
信令流（仅握手，不传数据）:
  browser/Go → /peerjs (WS) → signalserver.go HandleWS
  browser/Go → /peerjs/id (GET) → signalserver.go HandleID
  Go peer → /discover/announce (POST) → signalserver.go HandleAnnounce
  Go peer → /discover/nodes (GET) → signalserver.go HandleNodes

数据流（WebRTC DataChannel，点对点）:
  browser ↔ Go peer:  bridge.js ↔ node.go (via peer.go DataConnection)
  Go peer ↔ Go peer:  peer.go ↔ peer.go
  browser ↔ browser:  bridge.js ↔ bridge.js  ← 需要 P0/P1 才能实现
```

### 8.4 Go peer vs Web peer 对比

| | Go peer | Web peer |
|---|---|---|
| 代码位置 | `pkg/peerfs/node.go` + `pkg/peerjs/` | `pkg/peerfs/web/bridge.js` |
| 传输层 | Pion WebRTC（`peer.go`） | peerjs 1.5.4（vendored） |
| Offer 方向 | **永远 offerer**（`peer.go:400`） | 永远 answerer |
| Serve 能力 | ✅ `handleList` `:376` + `handleRead` `:414` | ❌ 无（§15.4） |
| Announce | ✅ `startAnnounceLoop` `:189` | ❌ 无 |
| Discover | ✅ `doAnnounce` `:215` 里拉取 | ❌ 无 |
| 生命周期 | 持久（24/7） | 临时（tab 生命周期） |
| 文件源 | 本地文件系统 | 需补（File 对象 / IndexedDB） |

---

## 9. 待做（§17.7 实施顺序）

| 阶段 | 内容 | 改动文件 | 行数 |
|---|---|---|---|
| **P0** | Web peer announce + discover | `bridge.js` | ~30 |
| **P1** | Web peer serve 路径（File 对象） | `bridge.js` | ~105 |
| P2 | ChunkAck 背压 + 断点续传 | `bridge.js` + `node.go` | ~40 |
| P3 | IndexedDB 持久化（可选） | `bridge.js` | ~60 |
| P4 | 密码保护 | `bridge.js` + `node.go` + `Config` | ~30 |

**P0 + P1 = 目标架构（Go + Web 对称 P2P 网络），约 135 行，全部在 `bridge.js`，Go 侧零改动。**
