# peerfs 外部对标项目对比（实读源码）

> 目的：不靠 GitHub API 元数据，把外部项目真的 clone 下来读代码，逐维度对比 peerfs。
> 每个结论都带 `文件:行号` 证据；证据来自 `/tmp/peerfs-compare/` 下的 shallow clone
> （`--depth 1`）与仓库内源码。

## 0. 对标仓库清单

| 仓库 | ★ | 语言 | 对标维度 |
|---|---|---|---|
| `kern/filepizza` | 10,157 | TypeScript | 传输协议、分片、流控 |
| `webtorrent/instant.io` | 3,594 | JS | **浏览器播放器喂数据方式** |
| `dmotz/trystero` | 2,730 | TS | 无服务器信令、信令 E2EE |
| `Novage/p2p-media-loader` | 1,714 | TS | **播放器集成架构** |
| `peers/peerjs-server` | 4,696 | TS | 信令服务器参考实现 |
| `MrRobotoGit/tiramisu` | 135 | Go | **虚拟文件系统（FUSE）** |
| `PeerXu/meepo` | 94 | Go | WebRTC 代理隧道 |
| `Akshay7273/sendbeam` | 22 | Go | 断点续传、加密、ICE 缓存 |
| `perguth/peertransfer` | 334 | JS | 信令加密 |
| `meduar/webrtc-web-proxy` | 1 | Go | webrtc-proxy 直接竞品 |

---

## 1. 基线：peerfs 当前参数（对比锚点）

| 维度 | peerfs 现状 | 证据 |
|---|---|---|
| WebRTC 栈 | `pion/webrtc/v4`，自写 PeerJS 信令客户端 | `pkg/peerjs/peer.go:1` |
| 信令 | 公共云 `0.peerjs.com` 或自托管 peerserver（`replace` 到 `./peerfs-chat/local/signalserver`） | `go.mod:69` |
| 分片 | **64KB 裸二进制块，无逐块头** | `pkg/peerfs/node.go:40` |
| 流控 | `BufferedAmount > 64KB` 轮询退避（200µs→5ms 倍增） | `pkg/peerjs/connection.go:88,409-425` |
| 并发 | 1 条 PeerConnection + **64 条 DataChannel（DCEP RFC 8832）**，每通道 lock-step 单请求 | `pkg/peerfs/web/bridge.js:161-221` |
| 文件语义 | `list`(readdir) / `read`(offset+size，**size=-1 到尾；offset 可为负=从 EOF 倒数读 moov box**) | `pkg/peerfs/node.go:23-37,437-446` |
| 认证 | `hello` + token，10s 超时门禁 | `pkg/peerfs/node.go:41,323-359` |
| NAT | `ICEUDPMux` 单 socket + **反向 STUN 打洞**（3 次 STUN Binding Request，从 Pion 监听 socket 发出）+ Google/Cloudflare STUN，无 TURN 凭证 | `pkg/peerjs/connection.go:52-64,319-373` |
| 浏览器呈现 | **Service Worker 拦截 `/__peerfs/stream/*` → 伪造 HTTP 206/200 + `ReadableStream` + `MessageChannel`** | `pkg/peerfs/web/sw.js:26-183` |
| 加密 | 无应用层加密（依赖 DTLS） | — |
| 断点续传 | **无** | — |
| 目录缓存 | **无**，每次 `list` 重新 `ReadDir`+`Stat` | `pkg/peerfs/node.go:376-412` |

---

## 2. 传输层对照

| 项目 | 分片大小 | 逐块头 | 流控 | 加密 | 断点续传 |
|---|---|---|---|---|---|
| **peerfs** | 64KB | ❌ 无 | BufferedAmount 退避 | ❌ | ❌ |
| **filepizza** | **256KB** (`MAX_CHUNK_SIZE = 256*1024`) | ✅ `{type:Chunk, fileName, offset, bytes, final}` | ✅ **ChunkAck 回包** | ❌（全仓无 AES/X25519） | ❌ |
| **sendbeam** | block/frame 两层，重切块对齐 | ✅ frame header + counter | ✅ | ✅ **每 frame AEAD**（`frameCounterBytes`/`aeadTagBytes`） | ✅ 完整 resume 协议 |
| **peertransfer** | 单次整块读取 | — | — | 仅**信令** AES | ❌ |
| **instant.io** | WebTorrent piece | WebTorrent 协议 | WebTorrent | DTLS | WebTorrent 级 |

### 关键差异

**filepizza 的每块协议**（`kern_filepizza/src/hooks/useUploaderConnections.ts:19` 定义 `MAX_CHUNK_SIZE = 256 * 1024`；`:241` 用 `file.slice(offset, end)` 切片后发出带 `offset`/`final` 的 `Message`；`useDownloader.ts:215-245` 收块后**立刻发 `ChunkAck`**）——这是明确的"offset + ack"流控。

peerfs 的裸块 + `BufferedAmount` 退避是**更轻**的设计，代价是：
- 无 offset 信息 → 无法做**选择性重传**与**断点续传**（大文件断线后必须从 0 重传）
- 无 ack → 发送端无法感知对端实际接收位置（只能靠 DTLS 的可靠有序 + `SendThrottled` 的本地缓冲水位）
- 块小 4 倍（64KB vs 256KB）→ 每 MB 多 4.5 倍 SCTP 消息数

peerfs 的 64KB 是刻意选的（`node.go:40` 注释 "SCTP 消息安全上限内"），配合 64 通道并发，吞吐上并不吃亏；但**缺 offset 语义是结构性短板**。

**sendbeam 是目前工程质量最高的对标**（`packages/wire/` 下有 `protocol.md`、`durable-receive.md`、`adr/`、`compat-matrix.md`、fuzz + differential 测试）：
- `transfer_chunker.go:3-7`：**"输入块边界无关，输出对齐到 frame"** 的重切块器，内存恒定在"一个输入块 + 一个 frame"
- `resumeauth.go:97-250`：`ResumeRoot`→`ResumeSecret`→`ResumeTranscript`→`ResumeOffererProof`/`ResumeJoinerProof`/`ResumeSessionMaster`，基于 transcript 绑定的完整续传协商
- `icecache.go:13`：`ICEConfigTTL = 15 * time.Minute`，**过期则强制重新拉取 ICE 配置**，防止短生命周期 TURN 凭证在客户端缓存里超期使用
- `trusted_auth.go`：ED25519 设备身份 + 签名

### peertransfer 的"信令加密"值得单独说

`perguth_peertransfer/browser.js:2` 引入 `crypto-js` 的 AES，`:68` **只加密 `data.signal`（SDP/CANDIDATE）**，密钥来自 `browser.js:57` 的 `key = window.location.hash.substr(1) || randomHex('24')`。

README `:16-17` 明确区分了两层：
> Data is transferred using end-to-end encryption (due to WebRTC). / The messages that are relayed by a server to initiate the p2p WebRTC connections are encrypted.

即：**数据面靠 DTLS 就够，但信令面必须额外加密**——因为信令服务器能看到明文 IP 候选（含内网地址），可被用来做 IP 泄露或握手期 MITM。这个观点 peerfs 目前完全没做（信令走明文 WS，自托管场景下服务器可见全部 SDP/CANDIDATE）。**链接的 hash 即密钥**是一个极简优雅的实现方式。

---

## 3. 最关键发现：浏览器播放器怎么吃到 WebRTC 数据

这是 peerfs 最核心、也最没被外部项目解决好的问题。三家三种做法：

### 3.1 peerfs —— Service Worker 伪造 HTTP 206（自研，独一无二）

`pkg/peerfs/web/sw.js:26` `fetch` 事件拦截 `/__peerfs/stream/`，`:50-63` 解析 `Range: bytes=start-end`（含 `bytes=-N` 后缀语法），`:65` 建 `MessageChannel` 转给主页面，`:132-151` 用 `ReadableStream` 把块灌进去，`:159-174` **返回 `206 Partial Content` + `Content-Range` + `Accept-Ranges`**。

效果：原生 `<video src="/__peerfs/stream/a.mp4">` 直接工作，浏览器按标准 Range 语义请求，进度条可任意拖。零集成代码。

### 3.2 instant.io —— **没有**虚拟流，纯 Blob 累积

`webtorrent_instant.io/client/index.js:227-235`：
```js
file.appendTo(util.logElem, {
  maxBlobLength: 2 * 1000 * 1000 * 1000 // 2 GB
}, function (err, elem) { ... })
file.getBlobURL(function (err, url) { ... })
```
`maxBlobLength` 2GB 是**上限参数**，超过就攒一个 Blob 出一个可点链接。`:272` 打包 zip 时也是 `URL.createObjectURL(blob)`。

全仓 grep `MediaSource`/`addSourceBuffer`/`new Response(`/`206` **均无命中**（`client/` 只有 `index.js` 286 行 + `util.js`；`index.js:94-96` 注册了 `/sw.js` 但该文件在仓库里不存在）。

**结论：instant.io 不具备边下边播/拖动进度条能力**，它靠 WebTorrent 攒 Blob。peerfs 在这点上严格更强。

### 3.3 p2p-media-loader —— 装饰播放器的 loader，而非拦截 HTTP

`Novage_p2p-media-loader/packages/` 下有两个平级集成包：
- `p2p-media-loader-hlsjs/src/`：`playlist-loader.ts`、`fragment-loader.ts`
- `p2p-media-loader-shaka/src/`：`loading-handler.ts`、`manifest-parser-decorator.ts`

即它**接管 HLS.js / Shaka Player 的数据源层**，让 segment 从 WebRTC 对等体来而不是从 CDN HTTP 来；MSE 那套仍然是播放器库自己的。README `:68-72` 明确 "Supports live and VOD streams over HLS or MPEG-DASH"、":85 WebRTC Data Channels"、":102 initially downloads media segments over HTTP(S) from a source server or CDN to start playback quickly"。

### 3.4 结论

| 方案 | 层 | 优点 | 局限 |
|---|---|---|---|
| **peerfs SW 虚拟 206** | 浏览器 fetch 层 | 任意 `<video>`/`<img>`/fetch 零改动；支持 Range 拖动 | 需 SW 支持 + 同域；仅单节点单文件流 |
| **p2p-media-loader 装饰 loader** | 播放器库内部 | 天然支持 ABR/HLS/DASH/多 peer 分流 | 必须是 HLS/DASH + 指定播放器库 |
| **instant.io Blob 累积** | JS 层 | 最简单 | 无流式播放、2GB 上限 |

**两种"虚拟 HTTP"思路处在不同层级，不是替代关系。** peerfs 的方案对"让任意网页元素直接播放远端文件"这个具体目标是唯一可行的；p2p-media-loader 的方案对"规模化 P2P CDN"才是正解。peerfs 若将来做多 peer 分流，值得参考它的 `segment-manager.ts` 分层设计。

---

## 4. 信令架构对比

### 4.1 官方 `peers/peerjs-server` v2 的完整消息集

`peers_peerjs-server/src/enums.ts`：
```ts
export enum MessageType {
  OPEN, LEAVE, CANDIDATE, OFFER, ANSWER, EXPIRE, HEARTBEAT, ID_TAKEN, ERROR
}
```
消息模型极简（`src/models/message.ts`）：`{ type, src, dst, payload? }`，处理用**策略模式**（`src/messageHandler/handlersRegistry.ts` 的 `registerHandler(type, handler)`）。

`src/messageHandler/handlers/transmission/index.ts` 的三个关键分支：
1. 目标在线 → 直接 `socket.send(JSON.stringify(message))`
2. **发送抛异常 → 关 socket / `removeClientById` + 向 `srcId` 补发 `LEAVE`**（注释原文："Tell other side to stop trying."）
3. 目标不在线 → **入队等重连**，但 `LEAVE`/`EXPIRE` 不入队

`heartbeat/index.ts` 只做一件事：`client.setLastPing(Date.now())`。

### 4.2 wintools `peerfs-chat/local/signalserver` vs 官方

**wintools 版已经超越官方**：
| 能力 | 官方 v2 | wintools 本地版 |
|---|---|---|
| 离线队列上限 | ❌ **无界** | ✅ `maxQueuedPerDst = 100`（`signalserver.go:71`，超了丢最旧 `:247-248`） |
| 队列条目过期 | ❌ | ✅ 30s TTL（`:135`）+ `sweepQueues()` 周期清扫（`:74-100`，注释明确 "H3 修复"） |
| 死连接检测 | `checkBrokenConnections` 服务 | ✅ `heartbeatTTL = 90s`（`:136`）+ 60s WS 读超时（`:185`） |
| 读上限 | — | ✅ 40KB（`:184`） |
| 节点发现 | **完全没有** | ✅ `HandleAnnounce` + `peerColls`/`peerStats`（uptime/上下行字节） |
| token 白名单 | token 校验 | ✅ 可选白名单（`:168-173`） |

**但 wintools 版有两个官方已解决的问题它没解决**：

1. **`route()` 忽略发送失败，不补发 `LEAVE`** —— `signalserver.go:234-236`：
   ```go
   dst := s.clients[m.Dst]
   if dst != nil {
       _ = dst.send(m)   // ← 错误被丢弃
       return
   }
   ```
   若目标 socket 还在表里但实际已半开（对端崩溃、NAT 映射消失），OFFER 会被静默吞掉，发起方**永久卡在等 ANSWER**。官方 `transmission/index.ts` 的 catch 分支正是为这个场景写的。

2. **`route()` 持锁发送** —— `route()` 全程持有 `s.mu`（`:231` `s.mu.Lock()`），`:235` 的 `dst.send(m)` 用 `client.sendMu` 串行写 gorilla websocket。任何一个慢/死客户端都会**卡住整个信令服务器的路由**。同文件 `removeClient()` 已经写对了（`:276-289` 先收集 `victims`、释放 `s.mu`、再逐个 send）——两处模式不一致。

这两条都是小改动、高价值的修复，且本仓库内已有正确范例。

### 4.3 trystero —— 完全不同的信令范式

trystero 不是"改进的 peerjs-server"，而是**把信令抽象成 topic pub/sub**（`dmotz_trystero/packages/core/src/strategy.ts`）：

```ts
// StrategyAdapter 接口 —— 每个后端实现这 4 个
{ init, subscribe, announce, deactivate }
// subscribe(relay, rootTopic, selfTopic, onMessage, onOffers, ctx)
// announce(relay, rootTopic, selfTopic, extra, ctx)
```

9 个后端：`core` / `torrent` / `nostr` / `mqtt` / `ipfs` / `supabase` / `firebase` / `ws-relay` / `trystero`。各自把 topic 映射到原生能力：MQTT `subscribeAsync`/`publish`（`mqtt/src/index.ts:46-49`）、Nostr `subscribe`/`publishTopic`（`nostr/src/index.ts:175,429`）、Supabase Realtime `broadcast` channel（`supabase/src/index.ts:47` `getChannelName = 'room:${topic}:messages'`）、Firebase RTDB 子节点监听（`firebase/src/index.ts:61-69`）、IPFS pubsub filter（`ipfs/src/index.ts:53-70`）。

**本质区别**：
- peerjs-server / peerfs：**按 peer ID 点对点路由**，服务器必须维护全量客户端注册表，是有状态中继
- trystero：**按 topic 广播**，任何订阅该 topic 的节点都能收到，**服务器不需要知道任何 peer ID** → 因此可以跑在 BitTorrent 蜂群、Nostr 中继、MQTT broker 这种"根本不需要注册"的基础设施上

**trystero 的"无服务器"真实含义**：不是不需要任何中继，而是**中继不需要为你写一行代码**。这是 peerfs 的自托管 peerserver 做不到的。

值得抄的三个细节：
- **topic 名做 SHA1**：`rootTopicP = sha1(topicPath(libName, appId, roomId))`（`strategy.ts`），中继看不到明文房间名
- **信令 E2EE**：`key = genKey(config.password ?? '', appId, roomId)`，SDP 走 `toCipher(plainOffer).sdp` 加密后再上广播通道
- **announce 节奏**：稳态 `5_333ms`，预热 `[233, 533, 1_333]ms`（抖动避免雷群效应）；`topic-strategy.ts` 默认稳态 `60_000ms`；还有 **passive room**（`:isPassive`，不主动 announce，收到有效信号才激活，`passiveActivationGraceMs = 7_533`）

---

## 5. WebRTC 代理隧道三家对比

### 5.1 重大发现：`meduar/webrtc-web-proxy` 名不副实

名字和 `cmd/webrtc-proxy` 几乎完全重合，实际读码结论：**它不是代理**。

`meduar_webrtc-web-proxy/peer.go` 是 Pion 官方 data-channels 示例的照抄：
- `peer.go:63-79` DataChannel open 后 `for range time.NewTicker(5s).C { d.SendText(signal.RandSeq(15)) }` —— **每 5 秒发一条随机字符串**
- `peer.go:86-88` 收到消息只是 `fmt.Printf` 打印
- `main.go:31` `// run("date")` —— 注释掉的执行系统命令
- 通信层 `communication/callSignal.go:11` `apiurl = "http://localhost:8000"`，用 HTTP `Get`/`Post` **轮询** `getMessage`/`SetServerMessage`

全仓 677 行 Go + 128 行 JS，**零帧封装、零分块、零流控、零 HTTP 代理逻辑**。

它唯一的技术选择值得注意：`peer.go:103-110` 用 `<-gatherComplete` **阻塞等 ICE 收集完成**，注释写明 "we do this because we only can exchange one signaling message ... in a production application you should exchange ICE Candidates via OnICECandidate" —— 即**禁用了 trickle ICE**，只交换一条信令消息。peerfs 的 trickle ICE + `pendingCands` 早到候选缓存（`pkg/peerjs/connection.go:290-303,375-385`）是生产级做法，这里是个反面教材。

### 5.2 `PeerXu/meepo` —— 真正的对标，且架构更强

meepo 是 **"无公网 IP 用 P2P 通道接入服务"** 的完整实现，README `:134-162` 演示 `meepo teleport` 把 `alice:http:8080` 服务搬到本地 `127.0.0.1:8080` 上 curl。

**协议选型与 webrtc-proxy 不同**：meepo 用 **SOCKS5**（`go.mod`: `github.com/things-go/go-socks5`）而非 HTTP over DataChannel，README `:205-228` 给出 `*.mpo` 泛解析 + `curl -x socks5h://127.0.0.1:12341 http://<MeepoID>.mpo/` 的用法。

架构亮点：
- **传输层可插拔**：`pkg/meepo/new_transport.go:10-13` `import _ "github.com/PeerXu/meepo/pkg/transport/loopback"` + `webrtc_transport "github.com/PeerXu/meepo/pkg/transport/webrtc"`，通过 `transport.Transport` 接口抽象
- **身份**：ED25519（`README.md:37`，`go.mod`: `github.com/teserakt-io/golang-ed25519` + `github.com/mikesmitty/edkey`）
- **状态**：Redis（`go-redis/v8`）+ gin HTTP API
- **序列化**：msgpack（`vmihailenco/msgpack/v5`）
- **自组网**：README `:185` "When disable selfmash, if alice want to build a transport to eve, it is using Default Signaling Server" —— 支持**经第三方 peer 中转**
- **STUN**：15 个 STUN server 列表（`pkg/meepo/option.go:18-33`），比 peerfs 的 3 个更宽

**借鉴价值**：webrtc-proxy 目前是单传输、单协议（HTTP）、单信令。meepo 的 `Transport` 接口抽象 + SOCKS5 支持 + 泛解析域名，是"把 webrtc-proxy 从 demo 变成基础设施"的直接路线图。

### 5.3 peerfs 自己仓库里的两种多路复用哲学

顺带一个内部对比（同一份代码库里两种做法）：
- `cmd/webrtc-proxy`：**1 条 DataChannel + N 条多路复用流**，帧格式 `[4B len][1B type][4B streamID][payload]`，`main.go:11-22,57-64`；`streams map[uint32]*pipeStream`（`:106`）
- `pkg/peerfs`：**64 条 DataChannel + 每通道 1 个请求**，DCEP RFC 8832 派生（`web/bridge.js:161-221`）

两者都是"一次 ICE 握手换 N 条逻辑流"，成本相同。差异是：
- DCEP 方案代码更省（不用手写帧封装/解复用），但依赖浏览器 DCEP 支持（Chrome/Edge/Firefox 已支持，Safari 支持有版本要求）
- 手写多路复用兼容任意对端（Go 对 Go 也行），且 `main.go:109-115` 注释记录了已经踩过的坑：io.Pipe 同步写导致 head-of-line 阻塞，改为有界 `reqBodyQueue`

`webrtc-proxy` 还有 `maxFrame = 1 << 20` 的恶意帧防护（`:82-84`，注释说明旧版本定义了却从不使用），这个意识 `peerfs` 的裸块协议没有对应物——不过 peerfs 的块大小是发送端自己控制的，攻击面更小。

---

## 6. 虚拟文件系统两种形态：tiramisu vs peerfs

### 6.1 tiramisu 的做法

tiramisu 内嵌了 fork 版 `anacrolix/torrent`（`go.mod`: `github.com/anacrolix/torrent => ./internal/anacrolix-torrent`），核心在 `internal/anacrolix-torrent/fs/`：

`file_handle.go:24-85` 的 `Read` 是**阻塞式**：
```go
r := me.tf.NewReader()          // anacrolix: 读到未下载数据时会阻塞等待
resp.Data = resp.Data[:req.Size]
...
n, readErr = io.ReadFull(r, resp.Data)   // 填满整个请求缓冲
if readErr == io.ErrUnexpectedEOF { readErr = nil }  // 读到当前已下载的末尾即返回
```
配套 `torrentfs.go:26-31`：
```go
type TorrentFS struct {
  Client *torrent.Client
  destroyed chan struct{}
  mu sync.Mutex
  blockedReads int      // ← 正在等待数据的读操作数
  event sync.Cond
}
```
每次 Read 都 `blockedReads++` + `event.Broadcast()`，让上层 UI 能显示"正在等待数据"而不是静默卡死。取消时返回 `fuse.EINTR`（`:82-83`），文件被销毁返回 `fuse.EIO`。

`file_handle.go:51-56` 还有一段 FreeBSD 兼容注释值得注意：
> A user reported that on freebsd 12.2, the system requires that reads are completely filled. Their system only asks for 64KiB at a time. I've seen systems that can demand up to 16MiB at a time.

目录层另有 `internal/vfs/dircache.go` —— **带 TTL 的目录列表缓存**（`DirCacheEntry{Entries, ExpiresAt}` + `sync.RWMutex`），`inodemap.go` 721 行做 inode 映射，`metadata.go` 156 行做元数据。`torrentfs.go:20` `defaultMode = 0o555` 只读。

### 6.2 两种形态的本质区别

| | **peerfs** | **tiramisu** |
|---|---|---|
| 数据方向 | 远端文件 → 浏览器虚拟流 | 远端内容 → 本机 FUSE 目录 |
| 消费方 | `<video>`/`<img>`/fetch | 任意本地程序（Plex/Jellyfin/ffprobe） |
| 数据是否已在本地 | ✅ 完整在本地磁盘 | ❌ **可能只下了一部分** |
| 读未就绪数据 | 不存在此问题 | **阻塞等待**（`ReadFull` + `ErrUnexpectedEOF` 容错） |
| 驱动模型 | 读请求驱动（push 块到通道） | **读请求驱动下载**（pull piece） |
| 流控 | SCTP BufferedAmount 退避 | FUSE 读阻塞 + `blockedReads` 计数 |
| 并发 | 64 DataChannel | FUSE 多 handle，每个独立阻塞 |

**收敛点（有意思）**：两者最终都用了同一个 idiom —— `io.ReadFull` 填满整个缓冲 + 把 `ErrUnexpectedEOF` 当成功。peerfs 在 `node.go:468` 对**本地完整文件**这么做；tiramisu 对**可能不完整**的 torrent 文件这么做。前者是防御性，后者是语义必需。

### 6.3 借鉴判断

tiramisu 的 FUSE 思路对 peerfs **不构成直接替代**（目标场景不同），但有两处可直接借用：

1. **目录列表 TTL 缓存** —— peerfs `handleList`（`node.go:376-412`）每次都对目录做 `ReadDir(-1)` + 逐个 `de.Info()`。大目录下控制台反复导航时，这会通过网络重传全部条目。加个 30-60s TTL 的进程内缓存完全符合 peerfs "纯内存无状态"（内存缓存，不落盘）的既有约定。
2. **`blockedReads` 语义** —— 当 peerfs 将来支持"内容不在本地、需从上游拉"（`cmd/peerfs-proxy` 已经走 ECH 拉 twimg）时，一个阻塞计数能让浏览器控制台显示"等待中"而非静默超时。

---

## 7. 结论：peerfs 到底独特在哪

1. **"HTTP 端口零文件暴露"这条约束**在所有对标项目里都没有人坚持。filepizza/instant.io/peertransfer 的服务器要么不存文件要么明确上传到服务端；tiramisu 的服务器是 torrent tracker/HTTP seed。peerfs 是唯一把"文件 100% 走 WebRTC SCTP，HTTP 只发控制台和候选信标"当设计公理的。
2. **Service Worker 虚拟 HTTP 206 管道**在本次对比的 10 个项目里**唯一实现**。instant.io 靠 Blob 累积（无流式播放），p2p-media-loader 走另一层（装饰播放器 loader）。这是 peerfs 最不可替代的资产。
3. **反向 STUN 打洞（`PunchCandidate`）**是自研设计：从 Pion 监听的同一块 UDP socket 发 3 次 STUN Binding Request，强制 NAT 建会话映射。对标项目里没有等价物 —— meepo 靠 15 个 STUN server 堆量，sendbeam 靠 ICE 配置缓存，其余都是纯 STUN/TURN。
4. **自托管信令服务器已经在安全性和发现能力上超过官方 `peerjs-server` v2**（有界队列、TTL 过期、心跳死连接、节点发现），这个事实值得写进 README。

**结构性短板**（外部项目都有解法）：无断点续传（sendbeam 有完整方案）、无选择性重传（filepizza 的 offset+ack）、无信令加密（peertransfer/trystero 都有）、无目录缓存（tiramisu 有）、无 TURN 凭证（sendbeam 有 TTL 缓存机制）。

---

## 8. 可借鉴清单（按收益/成本排序）

| # | 借鉴点 | 来源 | 解决 peerfs 的问题 | 预估改动 | 状态 |
|---|---|---|---|---|---|
| 1 | **`route()` 补发 LEAVE + 释放锁再 send** | `peers/peerjs-server` transmission handler；本仓 `signalserver.go` 的 `removeClient` 已有正确范例 | 目标半开时发起方永久卡死；单客户端卡死全服务器 | 小（~15 行） | ✅ 已做 |
| 1b | **`LEAVE`/`EXPIRE` 按 `src` 定位连接**（本仓 `pkg/peerjs` 原按 `connectionId`，恒查不到） | 官方 `peerjs` 1.5.4 `_handleMessage` | Go 侧收不到对端掉线通知 | 小 | ✅ 已做 |
| 1c | **ICE `Failed` 状态兜底关连接** | pion 官方回调；本仓 `goclient` 已有回调但未动作 | 打洞失败时 dc 永久泄漏 | 小 | ✅ 已做 |
| 1d | **`Connect` 等 DC 真正 open 再返回** | 本仓 `Peer.Open` 的三路 select 范式 | 调用方超时形同虚设 | 小 | ✅ 已做 |
| 1e | **`Close()` 未 open 分支也做清理** | 无外部来源，自查 | 僵尸连接残留 `p.conns` | 小 | ✅ 已做 |
| 2 | **信令 payload AES-GCM 加密** | `peertransfer/browser.js:57,68`（hash 即密钥）；trystero `genKey`+`encrypt` | 自托管/公共信令可见明文 SDP 与内网 IP 候选 | 中（信令两端 + `pkg/peerjs`） | 待做 |
| 3 | **read 请求带 offset 语义 + 接收端记录进度 → 断点续传** | `sendbeam` ADR 0004 落盘契约 + `protocol.md:259-262` 的 `ResumeState`（见 §10.1 最小裁剪） | 大文件断线必须从 0 重传 | 中（砍掉 `resumeauth` 全套后可控） | 待做 |
| 4 | **目录列表 TTL 缓存** | `tiramisu/internal/vfs/dircache.go` | 大目录重复导航重传全量条目 | 小 | 待做 |
| 5 | **TURN 支持**：客户端 `/rtc-config` 拉取 + **服务端动态签发凭证**（两层，见 §14.2） | 客户端侧 `instant.io/server/index.js:98-115`；**服务端侧 `filepizza/src/coturn.ts`**（HMAC-MD5 + TTL + Redis，比静态凭证更对）；缓存侧 `sendbeam/icecache.go:13` `ICEConfigTTL=15min` | 硬编码 STUN、无 TURN 兜底、对称 NAT 必挂；凭证永不过期不可撤销 | 中 | 待做 |
| 6 | **多信令多路 announce（故障转移）** | trystero **`relayConfig.redundancy` 数字式配置**（`types.ts:63-65`）+ **Go 侧 `meepo/pkg/signaling/` 的 Engine 注册表 + `chain` + `redis`**（§14.4，MIT，与借鉴 #8 同一套范式） | 单一信令服务器宕机全挂 | 中 | 待做 |
| 7 | **`SendThrottled` 加总超时**（tiramisu `Interrupt()` 的"关 reader 解阻塞"范式） | `tiramisu/internal/gostorm/native/native.go:343-354` | 对端活着但拥塞时 `SendThrottled` 无限 5ms 退避 | 小 | 待做 |
| 7b | **`blockedReads` 等待计数**（**当前不适用**，上游是本地文件） | `tiramisu/internal/anacrolix-torrent/fs/torrentfs.go:27`、`fs/file_handle.go:42-52` | 为将来接入远端/边下边服务的数据源做准备 | 小 | 暂缓 |
| 8 | **传输层接口抽象 + SOCKS5** | `meepo/pkg/meepo/new_transport.go`、`things-go/go-socks5` | webrtc-proxy 只能代理 HTTP，无法代理任意 TCP | 大（webrtc-proxy 重构） | 待做 |
| 9 | **头/尾磁盘预热**（64MB 头 + 16MB 尾，替代被动等负 offset 读 moov） | tiramisu `internal/warmup/warmup.go`（含稀疏洞跟踪的 `tailSpan`，见 §14.1） | 非 faststart 视频首帧与 seek 延迟 | 中 | 待做（GPL，只抄思路） |
| 10 | **发布物签名（minisign Ed25519）+ SBOM + SRI** | sendbeam `minisign.pub`、`scripts/minisign.go`、`scripts/generate-sbom.sh`、`apps/web/sri.ts` | wintools 的 `v*` tag 发布 `.exe` 目前**无任何签名/校验和**，用户无法验证下载物 | 小（签名）/ 中（SRI） | 待做 |
| 11 | **`trickleIce` 做成配置项**（而非硬编码） | trystero `types.ts:71` | peerfs 目前硬编码开 trickle，无法按网络条件关掉 | 小 | 待做 |

---

## 9. 本次对比顺带发现的 peerfs 缺陷

> 本节的 1–5 已在 2026-09 的修订中修复，行号为修复后的位置。

### 9.1 `sw.js` Range 解析变量先用后声明 ✅ 已修

```js
console.log('[SW-Range] ... Range:', rangeHeader || '全量');  // 原 :36
...
var rangeHeader = event.request.headers.get('Range') || '';   // 原 :51
```

`var` 提升使第 36 行的 `rangeHeader` 恒为 `undefined`，日志永远打印"全量"。仅影响日志。
修法：把整个 Range 解析块移到日志之前。

### 9.2 `signalserver.route()` 忽略 `send()` 返回值 ✅ 已修

原 `:235` `_ = dst.send(m)`。目标 socket 已半开但还没从 `clients` 表摘除时（对端崩溃、
NAT 映射消失），OFFER/ANSWER 被静默吞掉，发起方永久卡在等握手。
对标 `peers/peerjs-server` `transmission/index.ts` 的 catch 分支，修法：摘除死连接、
广播 LEAVE 给其余在线节点、再定向补发一份带 `Dst` 的 LEAVE 给发起方，并保留 dst 的
离线队列（dst 可能带相同 ID+token 重连）。

### 9.3 `signalserver.route()` 持 `s.mu` 调用可能阻塞的 `send()` ✅ 已修

原 `:231` 用 `defer s.mu.Unlock()` 覆盖全程，而 `send()` 内部
`WriteJSON` 带 10s 写超时（`signalserver.go` 的 `send()`）。一个慢/死客户端会持锁
10s 卡死整个信令服务器的路由。本文件 `removeClient` 早就写对了（先收集 victims、
释放锁、再逐个 send），两处模式不一致。修法：查表后立刻出锁再写。

### 9.4 `pkg/peerjs` 静默忽略所有 LEAVE / EXPIRE ✅ 已修（本轮新发现，比 9.2/9.3 更严重）

`peer.go` 的 `handleMessage` 统一用 `dc := p.conns[payload.ConnectionId]` 定位连接，
但**服务端构造 LEAVE/EXPIRE 时没有 payload**（只带 `type`/`src`），
`payload.ConnectionId` 恒为空串 → `p.conns[""]` 永远查不到 → 这两类消息被静默丢弃。

官方 `peerjs` 1.5.4 的 `_handleMessage` 原文（`pkg/peerfs/web/peerjs.min.js` 解包）：

```js
case O.Leave:  this._cleanupPeer(r), this._connections.delete(r);   // r = e.src
case O.Expire: this.emitError(E.PeerUnavailable, ...);
```

即两个分支**都按 `src` 匹配**。修法：LEAVE/EXPIRE 改为按 `m.Src` 遍历 `p.conns`
匹配 `Remote()`，收集后出锁再 `Close()`（`Close` 内部要抢 `dc.peer.mu`，同锁重入会
死锁）。影响面：修之前，Go 侧对端掉线根本收不到通知，僵尸 `DataConnection` 一直挂着。

### 9.5 全仓无 ICE 连接状态处理 ✅ 已加最小 watchdog（本轮新发现）

`grep OnConnectionStateChange` 全仓只有 `peerfs-chat/goclient/main.go:366` 一处，
且**只 `log.Printf` 不动作**。`pkg/peerjs`（media-node / peerfs-proxy / webrtc-proxy
都依赖它）完全没有。后果：目标不可达（防火墙丢包、对称 NAT 打洞失败、对方进程退出）
时 `dc` 永久留在 `p.conns`、UDP socket 一直占着，`Connect()` 的调用方拿不到任何
失败信号。修法：`wireICEWatchdog()`，仅在 `PeerConnectionStateFailed`（确定失败）时
关连接；`Disconnected` 不处理——弱网抖动会自愈，贸然关会误杀正在建立的连接。
两处建连接的路径（`Connect()` 主动发起、`onOffer()` 应答）都已挂上。

### 9.6 `Connect(ctx)` 完全忽略传入的 context ✅ 已修（本轮新发现）

`peer.go` 的 `Connect(ctx context.Context, remote string)` 发完 OFFER 立刻
`return dc, nil`，`ctx` 从头到尾没被读过。调用方
（`cmd/webrtc-proxy/main.go:554` 的 30s `connectCtx`、
`pkg/peerfs/integration_test.go:58` 的 30s ctx）以为在限制握手时长，实际目标不可达时
拿回一个永不 open 的 dc 继续往下用。

同文件 `Peer.Open` 早就做对了：`select { ctx.Done() / time.After(p.deadline) / p.msgs }`
三路，Connect 是漏网的那一个。修法：DataConnection 增加 `openCh`
（`wireDataChannel` 的 OnOpen 里幂等关闭），`Connect` 发完 OFFER 后
`select openCh / ctx.Done()`，超时则 `Close()` + 返回 `ctx.Err()`。

### 9.7 `Close()` 对未 open 的连接不做清理 ✅ 已修（本轮新发现）

原 `!dc.open` 分支只 `pc.Close()` 就 return，**不摘 `p.conns`、不回调 `onClose`**。
于是"从未完成握手"的连接（目标不可达、9.4 的 LEAVE 到达时还在等 ANSWER）永久残留在
`p.conns`，上层也收不到任何 close 通知。这也是 9.4 的修复必须配套改 9.7 的原因——
不改的话 LEAVE 只会关 PC，表项照样泄漏。修法：抽 `teardown()`，两个分支共用。

### 9.8 记录项（未改）

- **`pkg/peerfs/web/bridge.js` 完全不处理 LEAVE**——浏览器侧靠 DataChannel 的
  `close` 事件自然降级，不算 bug，但与 9.4 修好后的 Go 侧行为不对称。
- **协议漂移**：`peerfs-chat/goclient/main.go` 只实现 `list`/`read` 两个协议头，
  `pkg/peerfs/node.go` 实现 7 个（`hello`/`list`/`read`/`entries`/`meta`/`done`/`err`）。
  chunkSize 一致（64KB）。goclient 是旧 demo，与主实现并存容易踩坑。

---

## 10. 本轮深挖的补充结论

### 10.1 sendbeam 的最小 resume 裁剪方案（可直接照搬）

`sendbeam/docs/protocol.md` + `docs/adr/` 给出了 64KB 裸块协议加断点续传的**最小
实现路径**，砍掉绝大部分复杂度：

| 要保留 | 证据 |
|---|---|
| **落盘顺序契约**：verify → write → flush → 原子推进 journal → 才可 ack | `adr/0004:25-37` |
| **每文件 `committedBlocks` 高水位**（整块推进，无字节偏移字段，回退被拒绝） | `adr/0004:56-65` |
| **恢复时接收方先报 `ResumeState`**，发送方从该偏移重启，只补缺块；声明必须匹配 manifest | `protocol.md:259-262,273-279` |
| **最小状态字段**：`{transferId, manifestFingerprint, files:[{idx,size,blockSize,blocks,committedBlocks}]}` | 由上三条推得 |

| 可以整砍 | 理由 |
|---|---|
| `resumeauth.go` 全套（四消息互认 `resume_init→challenge→confirm→ready`、HKDF 派生链、角色分离 HMAC） | `resumeauth.go:12-14,97-105,219-245,266-271`——这套是为**离线身份证明**设计的；无 e2ee 时身份退化为会话内共享的随机 `transferId`/令牌，绑定交给 manifest 校验即可 |
| `digestCheckpoint`（前缀重哈希） | `adr/0004:114` 已明确允许为 null，缺省即整块重哈希 |
| ADR 0008（撤销同步）、0009（流量填充）、0006/0007（工程化） | 与单次传输的 resume 无关 |
| 浏览器 OPFS/lease/同源 reattachment | `compat-matrix.md:163-165` 本就无跨设备 |

两层分片的设计理由（`protocol.md:229-230,250-253`、`transfer_chunker.go:23-24`）：
**block 是 ack/重传/resume/checkpoint 的逻辑单位**（默认 1 MiB，大→控制开销小），
**frame 是传输/内存单位**（默认 16 KiB、上限 64 KiB，小→内存有界、贴合背压）；
frame 永不跨 block 边界，in-flight 上限 8 块兜底接收端内存。
peerfs 目前只有"frame"这一层（64KB 裸块、无 offset、无 ack），缺口是"block"这一层。

### 10.2 官方 peerjs 1.5.4 的 LEAVE 处理是本次 9.4 修复的依据

解包 `pkg/peerfs/web/peerjs.min.js`（v1.5.4）可见 `_handleMessage` 的完整分支：
`OPEN` / `ERROR` / `ID-TAKEN` / `INVALID-KEY` / `LEAVE` / `EXPIRE` / `OFFER` / ...
其中 `LEAVE` 调 `_cleanupPeer(r)` + `_connections.delete(r)`，`EXPIRE` 发
`PeerUnavailable` 错误——**两者都以 `e.src`（离开的对端 id）为定位键，不是
`connectionId`**。这同时确认了 9.4 修法的协议正确性，也确认 wintools 自托管信令
服务器**在协议语义上与官方客户端一致**（官方客户端只按 src 找连接，服务端按 dst
路由，两端约定一致）。

### 10.3 instant.io 的 TURN 凭证下发：peerfs 缺的最直接一块

peerfs 的 `newPeerConnection`（`pkg/peerjs/connection.go:52-58`）**硬编码 3 个 STUN、
零 TURN**，对称 NAT 必挂。instant.io 给出了可以直接照搬的最小形态：

| 组件 | 证据 |
|---|---|
| 服务端端点 `GET /__rtcConfig__`，返回 `{comment, rtcConfig}`，带 CORS 白名单 | `webtorrent_instant.io/server/index.js:98-115` |
| 凭证来源：gitignored 的 `secret.rtcConfig`（不是公共 API 动态签发） | `server/index.js:107`；样例 `secret/index-sample.js:8-25` |
| 配置形态：STUN + TURN 三形态（`turn:...:443?transport=udp` / `?transport=tcp` / `turns:`）+ 静态 `username`/`credential`；另带 `sdpSemantics:'unified-plan'`、`bundlePolicy:'max-bundle'`、`iceCandidatePoolsize:1` | `secret/index-sample.js:8-25` |
| 客户端请求：5s 超时 | `client/index.js:102-117` |
| **降级路径**：拉取失败 → `cb(new Error('Could not get WebRTC config from server. Using default (without TURN).'))` | `client/index.js:107` |
| 降级实际行为：`util.error(err)` 只打日志，然后**照样用 `SimplePeer.config` 默认配置创建 client**（纯 STUN），不阻断 | `client/index.js:31-40` |

结论：这套是**运行时下发 + 静态凭证**，换 TURN 提供商不用改客户端代码。
peerfs 移植成本很低——`Config.ICEHook`（`connection.go:65-67`）已经是可插拔钩子，
只需加一个 `/rtc-config` 端点 + 启动时拉取即可，客户端不用改协议。
对比 sendbeam 的 `ICEConfigTTL=15min`（`icecache.go:13`）：它是**缓存**语义，
instant.io 是**下发**语义，两者正交，可以都做。

### 10.4 filepizza 的 channel 机制：服务端存的是元数据，不是文件

`filepizza/src/channel.ts` 澄清了一个容易搞错的点：

- `Channel = { secret?: string, longSlug: string, shortSlug: string, uploaderPeerID: string }`
  （`channel.ts:8-13`）
- `serializeChannel`/`deserializeChannel`（`channel.ts:65-74`）**只是 Redis 存储用的
  JSON 序列化，不是 URL 编码**。slug 在 URL 里只是查询键。
- 每个 channel 生成**两个 slug**：`shortSlug`（可猜测的短链，方便分享）和
  `longSlug`（抗暴破的长链），两条键都指向同一个 Channel（`channel.ts:124-131`）。
- `scrubSecret = true` 时把 `secret` 字段置 undefined 再返回（`channel.ts:72-73`）——
  用于对外 API 响应脱敏。
- `renewChannel(slug, secret, ttl)` 需要 secret 才能续期（`channel.ts:24`）——
  long slug + secret 就是"归属证明"。
- 元数据存 Redis、带 TTL（`channel.ts:130-131`）。

与 peerfs 的本质区别：filepizza 是**"slug → uploaderPeerID"的有状态查表**（需要 Redis），
peerfs 是**无状态服务器 + `/discover/nodes?coll=` 直接暴露 peer ID**。
权衡：filepizza 有隐私性（分享链接不暴露 peer ID）、可撤销（`destroyChannel`）、可续期；
代价是多一个有状态组件。peerfs 的无状态设计更简单，但分享链接就是 peer ID 本身。
如果之后要做"公开分享链接"，filepizza 的 short/long 双 slug 模型值得抄。

---

## 11. 分项目最佳实践分析

> 结合第 1–10 节的对比结论，逐个外部项目回答三个问题：它做对了什么、peerfs 该抄哪一条、
> 哪条明确不要抄。行号为 2026-09 复核后的位置（trystero 已升到 v3 多包结构、tiramisu 的
> torrentfs 在 `internal/anacrolix-torrent/` 下，早期记录的路径已作废并更正）。

### 11.1 `kern/filepizza` —— 抄"offset + ack"，抄双 slug，别抄服务端存文件

**做对了什么**：`MAX_CHUNK_SIZE = 256 * 1024`（`src/hooks/useUploaderConnections.ts:19`）；
发块前 `file.slice(offset, end)` 切片并带 `{fileName, offset, bytes, final}` 头
（`:241`）；收块端**立刻回 `ChunkAck`**（`useDownloader.ts:215-245`）。这是三个对标里
唯一具备"选择性重传 + 断点续传"协议基础的。channel 机制用 Redis 存
`{secret?, longSlug, shortSlug, uploaderPeerID}`（`src/channel.ts:8-13`），双 slug 指向
同一条记录（`:124-131`），`renewChannel(slug, secret)` 用 secret 做归属证明（`:24`）。

**该抄**：① read 请求加 offset 语义 + 接收端记进度——这是借鉴清单 #3 的地基；
② short/long 双 slug 的分享模型（peerfs 目前分享链接就是 peer ID，无隐私性、不可撤销）。

**不要抄**：Redis 作状态存储（peerfs 服务器无状态是设计公理，`/discover/nodes` 的成本
几乎为零；抄这个等于换架构）。256KB 也不必抄——见 11.6 的 FreeBSD 64KiB 注记，
peerfs 的 64KB + 64 通道并发已经是合理的。

### 11.2 `webtorrent/instant.io` —— 抄 `/__rtcConfig__`，别抄 Blob 累积

**做对了什么**：`GET /__rtcConfig__` 从 gitignored 的 `secret.rtcConfig` 下发 ICE 配置
（`server/index.js:98-115`），带 CORS 白名单（`:96-104`）；配置形态含 TURN 三形态
（`turn:…:443?transport=udp` / `?transport=tcp` / `turns:`）+ 静态 `username`/`credential`
（`secret/index-sample.js:8-25`）。**降级路径写得很干净**：客户端 5s 超时（`:102-117`），
失败回调 `new Error('Could not get WebRTC config from server. Using default (without TURN).')`
（`:107`），然后 `util.error(err)` 打日志、**照样用纯 STUN 建 client 不阻断**（`:31-40`）。

**该抄**：借鉴清单 #5。peerfs 的 `Config.ICEHook`（`pkg/peerjs/connection.go:65-67`）
已经是可插拔钩子，只需加一个 `/rtc-config` 端点 + 启动时拉取，**协议一行都不用改**。
这是整份清单里投入产出比最高的一项。

**不要抄**：Blob 累积（`client/index.js:227` 的 `appendTo({maxBlobLength: 2GB})` +
`getBlobURL()`）。这是"必须攒完整个文件才能播"的设计，peerfs 的 SW 虚拟 206 已经赢在这里，
别回头。

### 11.3 `dmotz/trystero` —— 抄 AES-GCM 信令加密 + 抖动预热，别抄 9 个后端

**做对了什么**（v3 多包结构：`packages/` 下共 9 个包 = `core` + 7 个后端
`firebase`/`ipfs`/`mqtt`/`nostr`/`supabase`/`torrent`/`ws-relay` + `trystero` 元包；
发布版的 `trystero` 只 re-export nostr——`packages/trystero/src/index.ts:1`，
其余后端按包单独安装，是个很克制的设计）：
- 信令端到端加密：`const algo = 'AES-GCM'`（`packages/core/src/crypto.ts:3`），
  `genKey`/`deriveRoomNamespace`/`encrypt`/`decrypt`（`:27/:43/:52/:71`）。关键是
  **SDP 加密后才进信令**：`publishCipheredSignalingMessage` 里
  `ctx.toCipher(signal).then(encryptedSignal => ... buildPayload(encryptedSignal.sdp))`
  （`packages/core/src/signal-handler.ts:69-78`）——不是事后加密，是构造 payload 时就密文。
- **Offer 预热池**：`offer-pool.ts:137-160` 的 `checkout(n, leaseOffers, encryptOffer)`
  预先生成并加密 offer，有 `offerLeaseTtlMs` 租约与 `recycle`。这是把"建 PeerConnection
  很慢"藏起来的标准做法。
- **抖动 announce**：`announceIntervalMs = 5_333` + `announceWarmupIntervalsMs = [233, 533, 1_333]`
  + `passiveActivationGraceMs = 7_533`（`packages/core/src/strategy.ts:42-45`）——预热期
  密而短，稳态 5.3s，避免所有客户端同步广播打爆中继。
- `StrategyAdapter {init, subscribe, announce, deactivate}`（`strategy.ts:55-58`）四方法
  适配器，`StrategyContext` 注入后端差异。

**该抄**：借鉴清单 #2（信令 AES-GCM）直接照 `signal-handler.ts` 的"构造时加密"顺序做，
**不要**做成"组装明文 → 再整包加密"，那样密钥泄漏后历史消息全露。借鉴清单 #6 的多信令
容错照 `strategy.ts` 的适配器 + 抖动 announce 实现。

**不要抄**：9 个后端。peerfs 只需要"自托管信令 + 一个公共兜底"，多后端是为库的分发形态
服务的，不是协议需要。Offer 预热池也不急——它解决的是"大规模陌生人发现"，peerfs 是
已知 peer 的定向连接，预热收益低。

### 11.4 `Novage/p2p-media-loader` —— 抄"CDN 兜底 + 下载即播种"，别抄 loader 装饰

**做对了什么**：`fragment-loader.ts:28` 始终保留一个
`#createDefaultLoader = () => new config.loader(config)`（即 CDN 加载器）；
`load()` 里先问 core 这个 segment 能否 P2P 加载（`:61-67`），不能就直接
`this.#defaultLoader.load(context, config, callbacks)`（`:71`）**回落到 CDN**。
回退阈值可配：`types.ts:311` "If set to `0`, the HTTP fallback will occur immediately"。
加载后的 ArrayBuffer 刻意留缓存"to keep our cached ArrayBuffer intact for seeding to
other peers"（`:90`）——下载一次、喂多个对端，超节点模型。

**该抄**：① **CDN/HTTP 兜底思路**——peerfs 的 SW 虚拟流一旦对端不可达就 503，
应该同样保留一条"回落到本地 HTTP 直读"的降级路径；② 超节点语义——peerfs 现在一个
连接一个 read，同一文件被 3 个浏览器同时读会读 3 遍磁盘，缓存复用值得考虑。

**不要抄**：装饰播放器 loader。peerfs 的 SW 拦截 HTTP 206 是更通用的一层（任何标签页
`<video>`/`<img>` 零集成），p2p-media-loader 的做法绑定 HLS.js/Shaka 的
`fragment-loader`/`playlist-loader` 接口，换播放器就废。两者不是替代关系，peerfs 的路线
更对。

### 11.5 `peers/peerjs-server` —— 抄服务拆分与两个兜底服务，别抄无界队列

**做对了什么**：服务端拆成独立服务而非一个大 handler——`src/services/` 下有
`checkBrokenConnections`、`messagesExpire`、`webSocketServer` 三个独立服务，
`src/instance.ts:7-12` 导入，`:43-48` 构造注入，`:98-99` 一起 start。
`MessageHandler` 是注册表（`:12,:40`），消息进 `handle(client, message)`（`:77,:87`）。
第 4.2 节已对比过：`messagesExpire` 让离线队列真正过期（peerfs 用 30s TTL +
`sweepQueues` 已做到）；`checkBrokenConnections` 定期踢死连接（**peerfs 没有等价物**）。

**该抄**：① `checkBrokenConnections`——peerfs 只清理 `queues`，`clients` 表里
"读路径活着但写路径死掉"的连接永远不会被摘除，只能等 60s 读超时；
② 服务化拆分模式（`NewServer` 目前把所有职责塞一个 struct，随功能增加会失控）。

**不要抄**：无界离线队列（peerfs 的 `maxQueuedPerDst=100` 是对的）；handler 注册表的
"任意 handler 可注册"开放性——peerfs 的消息类型是封闭集合，硬编码 switch 更清晰。

### 11.6 `MrRobotoGit/tiramisu` —— 抄"关 reader 解阻塞"的取消范式，`blockedReads` 现在用不上

**做对了什么**：`fs/file_handle.go:42-52` 的读走 goroutine，进入前
`blockedReads++` + `event.Broadcast()`，`io.ReadFull(r, resp.Data)` 阻塞到数据真正就绪
（`:55`）；defer 里 `blockedReads--` + 再 Broadcast（`:70-75`）。**可中断**：
`native.go:343-354` 的 `Interrupt()` 关 pipe reader 让 ReadFull 立刻返回，并置
`interrupted` 标志使 `ReadAt` 稳定返回 `ErrInterrupted`——注释原文
"Reader side close is enough to unblock ReadFull"。

一条意外发现：**`file_handle.go:47-54` 有一段 FreeBSD 实测注记**——"on freebsd 12.2, the
system requires that reads are completely filled. Their system only asks for **64KiB** at
a time. I've seen systems that can demand up to **16MiB** at a time"。peerfs 选 64KB 恰好
踩在真实客户端的下限上，这个选择被 torrent 生态的实测数据侧面验证了，不用改。

**该抄**：① **"关 reader 解阻塞"这个取消范式**——`Interrupt()` 靠关 pipe reader 让阻塞的
`ReadFull` 立刻返回并置标志位。对照 `pkg/peerjs` 的 `SendThrottled`（`connection.go:423-438`）：
它**有** `dc.closeCh` 出口（连接关闭时会退），但**没有总超时**——对端活着但网络拥塞、
DTLS 缓冲永远排不空时，会一直停在 5ms 退避上缓慢卡住。加一个总超时（比如 10s）就是
`Interrupt()` 思路的 peerfs 版。② `blockedReads` 计数本身**目前不适用**——peerfs 的上游是
本地文件系统，`io.ReadFull` 永远拿得到数据（EOF 走 `ErrUnexpectedEOF` 归零，
`node.go` 的 read 循环已正确容忍），"数据还没下载下来"这种场景不存在。它只在未来接入
远端/边下边服务的数据源时才需要，先记着别做。

**不要抄**：整棵 `anacrolix/torrent`。tiramisu 是"把 torrent 协议做成 FUSE 文件系统"，
peerfs 是"把本地文件系统做成 P2P"，方向相反，依赖一个 torrent tracker/peer 网络与
peerfs 的点对点语义冲突。

### 11.7 `PeerXu/meepo` —— 抄 Transport 抽象与状态订阅，别抄泛解析域名

**做对了什么**：`pkg/transport/transport.go:34-47` 的 `Transport` 接口把"一条到某个
peer 的传输"抽象出来，状态订阅是**句柄式**的：
`OnTransportState(TransportState, func(hid HandleID)) HandleID` +
`UnsetOnTransportState(s, hid)`（`:39-40`）——能精确退订，不泄漏闭包。
注册中心用 `newTransportFuncs sync.Map` + `NewTransportFunc`（`:49-55`），实现可插拔。
**失败处理**：`pkg/meepo/new_transport.go:96-104` 定义 `h` 在
`TransportStateFailed`/`TransportStateClosed` 时先
`closeTeleportationsByPeerID(peerID)` 再 `removeTransport(peerID)`——这正是本轮 §9.5
给 `pkg/peerjs` 加 `wireICEWatchdog()` 的同一个模式，**由最强的对标项目独立验证了
该修法的正确性**。STUN 清单 16 个（`pkg/meepo/option.go:18-33`）。

**该抄**：① 借鉴清单 #8 的 Transport 接口抽象 + `newTransportFuncs` 注册中心，
这是把 `cmd/webrtc-proxy` 从"只能代理 HTTP"变成"能代理任意 TCP"的钥匙（SOCKS5 就是
第一个可插拔实现）；② 句柄式状态订阅——比 `wireICEWatchdog` 的硬编码回调干净，
下一步就该把它做成可退订。

**不要抄**：泛解析 `*.mpo` 域名 + Redis + ED25519 设备身份这一整套"服务化"包袱。
meepo 是当公共基础设施用的，peerfs 是当点对点协议用的，复杂度预算不同。15 个 STUN 也
不必抄——peerfs 有反向 STUN 打洞（`PunchCandidate`）这个自研优势，堆 STUN 数反而掩盖
它。

### 11.8 `Akshay7273/sendbeam` —— 抄 block 层与落盘契约，别抄 resume-auth

**做对了什么**：`docs/protocol.md:229-230` 明确两层——`maxFrame`（默认 16 KiB、上限
64 KiB）是传输/内存单位，`blockSize`（默认 1 MiB）是 **ack/retry/resume 的逻辑单位**；
`transfer_chunker.go:23-24` "A block boundary is never crossed by a frame"；
`DEFAULT_INFLIGHT_BLOCKS = 8` 兜底接收端内存（`protocol.md:250-253`）。
`docs/adr/0004` 的落盘顺序契约（`:25-37`）与整块 `committedBlocks` 高水位（`:56-65`）
是最小 resume 的地基；恢复时接收方先发 `ResumeState`、发送方只补缺块且声明必须匹配
manifest（`protocol.md:259-262,273-279`）。

**该抄**：借鉴清单 #3，按 §10.1 的裁剪方案做——**只留 ADR 0004 的落盘契约 +
`committedBlocks` 高水位 + `ResumeState` 声明校验**，最小状态字段
`{transferId, manifestFingerprint, files:[{idx,size,blockSize,blocks,committedBlocks}]}`。

**不要抄**：`resumeauth.go` 全套（四消息 HMAC 互认 + HKDF 派生链 + transcript 绑定）。
那套是给"离线身份认证"用的；peerfs 无 e2ee，身份退化为会话内共享的随机 `transferId`，
绑定交给 manifest 校验即可。同理 ADR 0008（撤销同步）、0009（流量填充）、0006/0007
（工程化）全部跳过。

### 11.9 `perguth/peertransfer` —— 抄"hash 即密钥"，别抄它没有的数据面

**做对了什么**：`browser.js:2` `let aes = require('crypto-js').AES`，
`:68` `data.signal = aes.encrypt(data.signal, key).toString()`，
`:74` 解密。密钥来自 URL hash（`key = window.location.hash.substr(1) || randomHex('24')`）
——**分享链接本身就承载密钥**，没有服务端、没有密钥分发。

**该抄**：借鉴清单 #2 的最简实现——peerfs 可以一样用 URL hash/查询参数做信令密钥，
`pkg/peerjs` 的信令消息在 `send()` 前后各加一次 AES-GCM 即可，信令服务器协议不变
（服务器看到的就是密文，天然不需要改）。这比 trystero 的方案轻得多。

**不要抄**：其他什么都别抄。它只加密信令、数据面零处理，且 crypto-js 是纯 JS 实现
（性能与常量时间保证都远不如 Web Crypto 的 AES-GCM）。抄它的**思路**，用 Web Crypto 实现。

### 11.10 `meduar/webrtc-web-proxy` —— 反面教材，但抄它的"不要做什么"

**做对了什么**：什么都没做对。全仓 677 行 Go 是 Pion 官方 data-channels 示例照抄：
每 5 秒 `SendText(RandSeq(15))` 发随机字符串，收到消息只 `Print`，零代理逻辑。

**该抄**：没有可抄的。**该抄的是它的反面**——它 `<-gatherComplete` 等待所有候选收齐才
发 offer，等于**禁用 trickle ICE**；peerfs 的 `pendingCands`/`hasRemoteDesc`
（`pkg/peerjs/connection.go`）早到候选缓存是生产级的，别被这种示例代码带偏。

**不要抄**：整个项目。保留它进对比清单的意义只有一个——避免下一个读代码的人看到
"webrtc-web-proxy" 这个名字误以为有对标价值。

---

## 11.11 汇总：按"先做什么"排序

| 顺序 | 做什么 | 抄自 | 成本 | 为什么是这个顺序 |
|---|---|---|---|---|
| **1** | `/rtc-config` 端点 + TURN 凭证下发 | instant.io `server/index.js:98-115` | 小 | `Config.ICEHook` 已是钩子，协议零改动；对称 NAT 是目前最硬的单点故障 |
| **2** | 信令 AES-GCM（hash 即密钥） | peertransfer `browser.js:68` + trystero `signal-handler.ts:69-78` | 小 | 信令服务器协议不变（看到的就是密文）；泄露 SDP/内网 IP 是当前最大信息暴露面 |
| **3** | `checkBrokenConnections` 周期清扫 `clients` 表 | peerjs-server `src/services/checkBrokenConnections` | 小 | 本轮 §9.2/§9.3 已修了路由层，这一条补齐"死连接残留"的最后一块 |
| **4** | read 加 offset + 接收端 `committedBlocks` 高水位 → 断点续传 | sendbeam ADR 0004 + filepizza `useUploaderConnections.ts:241` | 中 | §10.1 的裁剪方案已把成本从"中大"压到"中"；砍掉 resume-auth 后是纯协议扩展 |
| **5** | CDN/HTTP 兜底路径 + 同文件多读复用 | p2p-media-loader `fragment-loader.ts:61-71,90` | 中 | SW 虚拟流现在对端不可达就 503，没有降级出口 |
| **6** | Transport 接口抽象 + 句柄式状态订阅 | meepo `pkg/transport/transport.go:34-47` | 大 | 把 webrtc-proxy 从 HTTP-only 变成任意 TCP 代理（SOCKS5 为首个实现）；也是 §9.5 的正规化下一步 |
| **7** | `SendThrottled` 加总超时 | tiramisu `native.go:343-354` 的 `Interrupt()` 取消范式 | 小 | `SendThrottled` 已有 `closeCh` 出口但无总超时，对端拥塞会无限退避 |
| — | 双 slug 分享链接 / 多信令容错 / DirCache / ICE 配置缓存 | filepizza / trystero / tiramisu / sendbeam | 小-中 | 价值真实但不紧急，排在上面几条之后 |

---

## 12. 能否直接引用这些开源项目（许可可行性）

> 结论先行：**有 3 个可以直接依赖，1 个可以小范围拷贝，2 个法律上明确不能用，
> 其余要么只能当"设计参考"要么 npm 包已过期是个坑。** 依据是 2026-09 逐个
> `gh api repos/<r>/license` + `git ls-remote` + `registry.npmmirror.com` 实测。

### 12.0 前提：wintools 自己没有 LICENSE

`gh api repos/Hana-ame/wintools` 返回 `license: null`，仓库内无 `LICENSE` 文件、
`grep -r "MIT License|Apache License|GPL|SPDX-License-Identifier"` 零命中。
仓库是 **public**。按美国版权法，无许可 = 默认保留所有权利。

这对"把别人的代码引进来"的影响不大——约束来自**源项目**的许可，不来自目标仓库。
但它有两个直接后果：

1. 引入 MIT/Apache 代码后，**必须**新增 `THIRD_PARTY_NOTICES`（或对应文件头声明），
   否则连"我保留了版权声明"都做不到。wintools 目前**没有任何第三方声明惯例**
   （`grep -r "THIRD_PARTY|NOTICE|Attribution"` 零命中）。
2. 更实际的一点：**已 vendor 的 `pkg/peerfs/web/peerjs.min.js` 目前没有保留许可声明**。
   它是裸 min 产物，全文搜不到版权行，只有内嵌 package.json 里的 `"license":"MIT"` 字样。
   而 MIT 的要求是" Redistributions of source code must retain the above copyright
   notice"——**现有产物本身就不满足 MIT 的署名条件**。这是历史遗留，建议随本次一起补。

### 12.1 许可矩阵

| 项目 | 许可 | 可否 `go get` / `npm i` | 结论 |
|---|---|---|---|
| `dmotz/trystero` | **MIT** | ✅ npm `trystero@0.25.4` | **首选，直接依赖** |
| `PeerXu/meepo` | **MIT** | ✅ 合法 module 路径，1 个 semver tag | **可直接依赖** |
| `Akshay7273/sendbeam` | **MIT** | ✅ `github.com/sendbeam/wire`、`github.com/sendbeam/engine`，10 个 tag | **可直接依赖** |
| `Novage/p2p-media-loader` | **Apache-2.0** | ✅ `p2p-media-loader-core` / `-hlsjs` / `-shaka`（**无 scope**） | 可依赖，但注意 Apache 义务 |
| `webtorrent/instant.io` | MIT | ❌ 未发布 npm | 只能当设计参考 |
| `peers/peerjs-server` | MIT | ⚠️ 最新 tag 是 `v1.1.0-rc.2`（**预发布**） | 只能当设计参考 |
| `kern/filepizza` | **BSD-3-Clause**（自写文本，带 "All rights reserved" 历史开头，但正文是标准三条；另在同文件尾部夹带一份 SIL OFL v1.1 用于 `static/fonts`） | ❌ 未发布 | 可小范围拷贝（需署名 + 不得用其名称背书） |
| `perguth/peertransfer` | MIT | ⚠️⚠️ npm 只有 `1.0.0`，**发布于 2017-10-08** | **npm 坑，见 12.4** |
| `MrRobotoGit/tiramisu` | **GPL-3.0** | ❌ `module tiramisu`（非域限定路径，**不可作为 import path**） | **法律上不可用** |
| `meduar/webrtc-web-proxy` | **无 LICENSE** | — | **法律上不可用** |

### 12.2 首选：trystero（真正能开箱即用的那一个）

实测 `registry.npmmirror.com/trystero/latest` 的元数据：

```
version: 0.25.4
type:    module          (纯 ESM)
exports: { ".": "./dist/index.mjs",
           "./mqtt": "./dist/mqtt.mjs",  "./nostr": "./dist/nostr.mjs",
           "./firebase": ..., "./ipfs": ..., "./supabase": ...,
           "./torrent": ..., "./package.json": "./package.json" }
dist.unpackedSize: 42363    (42 KB)
```

三个关键属性都对：

- **42 KB** —— 放进浏览器包几乎无感；
- **按后端拆子路径导出** —— 只引 `trystero/mqtt` 或 `trystero/nostr` 时只带一个后端，
  天然 tree-shakeable，不会把 7 个后端的代码全带进去；
- **MIT** —— 只需保留版权行。

**用法**：`npm i trystero`（或走 `npm i trystero/mqtt` 的单后端形态），然后按 §11.3
的结论接一个后端。对 peerfs 的具体价值是借鉴清单 #2（信令 AES-GCM）和 #6（多信令容错）
——**这两个可以不自己写，直接站在 trystero 上**。代价是接受它的 topic pub/sub 信令范式
（§4.3 已分析，与 peerjs 的 `dst` 路由范式不同），需要一层适配。

### 12.3 次选：两个 Go 项目

- **`github.com/sendbeam/wire` + `github.com/sendbeam/engine`**：MIT、合法 module 路径
  （`packages/wire/go.mod:1`、`packages/engine/go.mod:1`）、仓库有 10 个 semver tag。
  对应借鉴清单 #3。**但**：sendbeam 是 `go.work` 多模块工作区（`packages/wire`、
  `packages/engine`、`apps/{desktop,cli,server}`），且 wire 包本身依赖它的协议常量与
  AEAD 封装，**单独引 `wire` 只会有价值在"抄它的 protocol.md 文档"**——代码层面它和
  e2ee 强绑定，砍掉 e2ee 后大部分类型都会变得无意义。所以：**文档可直接引用，代码建议只抄
  §10.1 的三样**。
- **`github.com/PeerXu/meepo`**：MIT、合法 module 路径、`pkg/` 下是标准库结构
  （`api meepo ofn sdk signaling teleportation transport util`），有公开构造函数
  `NewMeepo(opts ...NewMeepoOption)`（`pkg/meepo/meepo.go:187`）。对应借鉴清单 #8。
  **但**它只有一个 semver tag（`v0.0.0-2024...`），依赖面宽（SOCKS5、Redis、
  泛解析），且它是把 peerfs 要做的事做成**公共基础设施**的完整实现——引它等于接管
  它的架构决策。建议：抄 `Transport` 接口形状与句柄式状态订阅（§11.7），不引依赖。

### 12.4 明确不能用 / 有坑的

1. **`MrRobotoGit/tiramisu` — GPL-3.0，双重排除**
   许可是 GPL-3.0（仓库自带 LICENSE）。**并且** `go.mod` 第一行是 `module tiramisu`——
   裸名，不是域限定的合法 module 路径，`go get` 根本拉不到，只能源码拷贝。
   拷贝 GPL 代码进一个无许可的公共仓库，会让组合体落入 GPL-3.0 传染范围。
   一个顺带的事实：它 fork 的上游 `anacrolix/torrent` 是 **MPL-2.0**（实测
   `gh api repos/anacrolix/torrent/license`），tiramisu 自己的代码用了比上游**更严格**的
   GPL-3.0。所以连"只引 anacrolix 原版"都只能拿到 MPL-2.0 的文件级传染，而不是干净。
   → **只当设计参考**（§11.6 的 `Interrupt()` 范式思路，不引代码）。

2. **`meduar/webrtc-web-proxy` — 无 LICENSE**
   `gh api` 返回 `NONE`，仓库内无许可文件。无许可 = all rights reserved，
   **法律上没有授权，不能引**。而且它本来也没什么可引的（§11.10）。

3. **`perguth/peertransfer` — npm 包是 2017 年的，是个坑**
   实测 `registry.npmmirror.com/peertransfer`：`dist-tags.latest = "1.0.0"`，
   `versions = ["1.0.0"]`，**发布时间 2017-10-08**。而仓库 tag 已到 `v2.1.1`。
   也就是说 `npm i peertransfer` 装到的是**七年前的 1.0.0**，不是我在 §11.9 里分析的
   v2.1.1 代码。想用的话只能 `git clone` 后本地 vendor，不能用 npm。
   不过这个项目本来就只有信令加密这一条值得抄（`browser.js:68` 几行），
   手写 10 行 Web Crypto 比引一个过期包更划算。

4. **`peers/peerjs-server` — 只有预发布 tag**
   最新是 `v1.1.0-rc.2`（`git ls-remote` 实测），不是正式版。而且它是 npm/TS 项目，
   不是 Go 库。→ 只当设计参考（§11.5 的 `checkBrokenConnections` 思路）。

5. **`webtorrent/instant.io`** — MIT 但未发布 npm，且要引的只是 `/__rtcConfig__` 那个
   端点（`server/index.js:98-115`）+ `secret/index-sample.js` 的配置形状。
   自己写 20 行 Go 更划算。

### 12.5 唯一建议真依赖的一个 + 配套动作

**`trystero`**，理由已在 12.2。配套需要做三件小事：

1. 新增 `THIRD_PARTY_NOTICES.md`，首条写 trystero（MIT，保留其 LICENSE 全文）；
2. **顺手补 `pkg/peerfs/web/peerjs.min.js` 的署名**——它是 MIT 但当前无版权声明，
   属于历史遗留的合规缺口；
3. wintools 自己补一个 `LICENSE`（否则引别人的 MIT 代码进一个无许可仓库，
   下游使用者连"我能不能用 wintools"都答不上来）。

### 12.6 未能验证的部分

- 我**没法在本地实测 `go get`**：这个 sandbox 禁写 module cache，
  `go get github.com/sendbeam/wire@latest` 报
  `can't find or create lock file: open /home/lumin/go/pkg/mod/cache/vcs/.../lock: permission denied`。
  所以上面 Go 侧的"可依赖"结论依据是 `git ls-remote` 的 tag 存在性 + `go.mod` 的
  module 路径合法性，**不是实际拉取成功**。真要用之前请在干净环境跑一次
  `GOPROXY=direct go get` 确认。
- npm 侧走了 `registry.npmmirror.com`（国内镜像）实测，数据可信；
  `registry.npmjs.org` 在本环境超时。
- `kern/filepizza` 的 LICENSE 是**自写文本**：开头 "All rights reserved" 是 BSD 系许可的
  历史样板（UC Berkeley 原文就有，被下面的 "Redistribution and use ... are permitted"
  实质条款覆盖），正文三条 = 标准 **BSD-3-Clause**（含 no-endorsement 条款，比 BSD-2 多
  一条"不得用版权方名称为衍生产品背书"）。**同一文件尾部还夹带一份 SIL Open Font License
  v1.1**，专门管辖 `static/fonts`。所以要拷 filepizza 的协议代码（`src/channel.ts`、
  `useUploaderConnections.ts`）时，署名要按 BSD-3 处理，别只写 "BSD"；而字体资产是
  另一个许可，不能混在一起声明。

---

## 13. 组件覆盖矩阵：谁实现了你的哪一部分

你的设计是 **Go 服务端 + JS/SW 浏览器客户端 + 信令服务器** 三件套。
按这个维度把 10 个项目逐个标出来（2026-09 实测目录结构）：

| 项目 | Go 服务端（P2P 文件节点） | JS 浏览器端 | Service Worker | 信令服务器 |
|---|---|---|---|---|
| `kern/filepizza` | ❌ **Node.js**（Next.js：`src/app/`、`routes.ts`、`redisClient.ts`、`zip-stream.ts`、`coturn.ts`） | ✅ React（`src/components/`） | ❌ | ✅ 自建（Redis + coturn 管理） |
| `webtorrent/instant.io` | ❌ Node.js 壳（`server/index.js` 只有 `compress`、静态文件、`/__rtcConfig__`、SPA fallback） | ✅ | ⚠️ **空壳**，见下 | ❌ 只下发 RTC 配置 |
| `dmotz/trystero` | ❌ | ✅ 库（`packages/core`） | ❌ | ❌ 提供 9 个**后端适配器**（不自建服务器） |
| `Novage/p2p-media-loader` | ❌ | ✅ 库（core + hlsjs + shaka） | ❌ | ❌ |
| `peers/peerjs-server` | ❌ Node.js | ❌ | ❌ | ✅ **本项目就是信令服务器** |
| `MrRobotoGit/tiramisu` | ✅ **Go**（FUSE + torrent） | ❌ | ❌ | ❌ |
| `PeerXu/meepo` | ✅ **Go 库**（`pkg/{meepo,transport,teleportation,...}`） | ❌ | ❌ | ⚠️ 有 `pkg/signaling` 但只做引擎注册，见 §14.4 |
| `Akshay7273/sendbeam` | ✅ **Go**（`packages/{wire,engine}` + `apps/server`） | ✅ `apps/web`（含 `sri.ts`） | ❌ | ❌ |
| `perguth/peertransfer` | ❌ | ✅ 单页（`browser.js` + `index.html`） | ❌ | ❌ 用公共 peerjs |
| `meduar/webrtc-web-proxy` | ✅ Go | ✅ 单页 | ❌ | ❌ |

**三个结论**：

1. **没有人实现完整三件套**。`sendbeam` 最接近（Go + web + server app），但没有 SW；
   `filepizza` 的服务器最完整（Redis + coturn + zip-stream），但是 Node.js 不是 Go。
2. **SW 一栏全表只有一个 ❌ 和一个空壳**。peerfs 的 Service Worker 虚拟 HTTP 206 管道
   依然**独一无二**（§3.4 的结论得到更直接的印证）。而且 instant.io 那个"有 SW"是
   假的——`static/sw.js` 全文只有：
   ```js
   /* global self */
   self.addEventListener('fetch', e => {})
   ```
   它注册了 SW（`client/index.js:94-95` `navigator.serviceWorker.register('/sw.js')`）
   但 fetch 回调是空的。这是 §3.2"instant.io 没有流式播放"的又一直接证据：
   **它连 SW 都建了壳、但没用**。
3. **Go 侧只有 tiramisu / meepo / sendbeam / webrtc-web-proxy 四个**，其中真正可读的
   是 tiramisu（GPL 排除）、meepo（最完整）、sendbeam（文档最完整）。
   **peerfs 是"Go 服务端 + SW"组合的唯一实例**，这个组合在 10 个项目里没有对标物——
   意味着 §8 借鉴清单里"Go 侧抄代码"和"浏览器侧抄代码"是**两个独立的移植通道**，
   不能指望一个项目同时给你两头的参考。

---

## 14. 之前没讲到、但开源项目已经实现的东西

> 这一节只收录**第 1–13 节都没有讲过**的特性。每条都带证据行号。

### 14.1 tiramisu 的磁盘预热：**64 MB 头 + 16 MB 尾**双缓存（与 peerfs 最相关）

`internal/warmup/warmup.go` 的常量定义：

```go
var FileSize int64 = 64 * 1024 * 1024          // 每文件"头"缓存上限，默认 64 MB
TailWarmupSize int64 = 16 * 1024 * 1024        // 16 MB 尾（Cues/seek index）
warmupSuffix   = ".warmup"                     // 头缓存文件
tailSuffix     = ".warmup-tail"                // 尾缓存单独一个文件
warmupWriteBuf = 16 * 1024 * 1024              // 与 pump chunk 同尺寸
handleIdleMax  = 30 * time.Second              // 空闲句柄 30s 后关闭
missingTTL     = 10 * time.Second              // 负缓存（查无此文件）寿命
warmupQuota    = 32 * 1024 * 1024 * 1024       // 总配额 32 GB（配置可覆盖）
```

**这直接命中 peerfs 已有的负 offset 读 moov box 场景**（`pkg/peerfs/node.go` 的
`read` 处理里有"支持负数 offset 读取文件末尾 N 字节（如非 faststart 视频读取末尾
moov box）"）。区别是：peerfs 是**被动地**等浏览器发负 offset 请求；tiramisu 是
**主动**把文件头 64 MB 和尾 16 MB 提前拉到磁盘，让播放器和 seek 立即可用。

顺带一个**正确性陷阱**值得记下来（`warmup.go:107-113` 原文）：

> "Writes land at whatever offsets the client probes, so the file is sparse: a single
> watermark would mark unwritten holes as covered, and reading a hole returns zeros
> with err==nil - served to the player as if it were real MKV data."

稀疏文件的洞读出来是 `0` 字节且 `err==nil`——**会被播放器当成真实数据**。所以他们
用 `tailSpan`/`tailRange` 跟踪**实际写入的区间**，而不是单一高水位。这个坑在任何
"边下边服务"的实现里都会踩，peerfs 如果将来接远端数据源一定要避开。

另有两处工程细节：`sync.Pool for write buffers`（`:78`，避免每 chunk 分配 16 MB 堆内存）；
`OnWarmupStateChange` 回调**故意用同步而非轮询**（`:36-42` 注释解释：`WriteChunk()`
只入队就返回，调用方紧接着查 `IsWarmingUp()` 会跑在 worker 前面，同步回调按构造消除了
这个竞态）。

### 14.2 filepizza 的 **coturn.ts：动态 TURN 凭证签发**（比 instant.io 强一档）

§10.3 说 instant.io 是"静态凭证"。但 filepizza 做的是**动态签发**——
`src/coturn.ts` 的 `generateHMACKey(username, realm, password)` 用
`crypto.createHash('md5').update(`${username}:${realm}:${password}`).digest('hex')`
派生 coturn 的 `hmac-auth` 密钥，`setTurnCredentials(username, password, ttl)` 写入
Redis 并带 TTL，`COTURN_ENABLED` 环境变量开关。

这是 RFC 5389 的标准 `username-based auth`（`<username>!<timestamp>` 形式）。
意义：**凭证可过期、可撤销、可按用户隔离**，不需要把长期有效凭证下发给浏览器。
peerfs 若要做 TURN，应该直接抄这个模式而不是 instant.io 的静态形态——这也修正了
§8 借鉴清单 #5 的表述：`sendbeam` 的 `ICEConfigTTL` 解决"客户端别缓存过期配置"，
`filepizza` 的 coturn 解决"服务端别下发永不过期的凭证"，两者是**不同的问题**。

### 14.3 filepizza 的 **zip-stream.ts：流式 ZIP 打包下载**

`src/zip-stream.ts` 实现了流式 ZIP 生成（自带 `Crc32` 类，注明"Based on
StreamSaver.js"），文件夹下载**不需要先把整个文件夹攒进内存再打包**——边遍历边写入
ZIP 流。peerfs 目前一个连接一次 read 一个文件，没有"整个目录打包下载"的能力；
`zip-stream.ts` 是这个功能的现成参考实现（注意：BSD-3 且未发布 npm，只能源码拷贝）。

### 14.4 meepo 的 **pkg/signaling：Go 侧的可插拔信令引擎注册表**

`pkg/signaling/signaling.go:14-19` 定义 `Engine` 接口（`Wire(dst, src *Descriptor)` +
`OnWire(handler)` + `Close()`），`:23` 是 `newEngineFuncs sync.Map` 注册中心 +
`RegisterNewEngineFunc(name, fn)`，目录里还有 `chain/`（多后端串联）和 `redis/`
（Redis 后端）。

这与 §11.7 抄的 `pkg/transport` 是**同一套注册表范式**，但用在信令层——
也就是说 meepo 把"信令后端可插拔 + 多后端故障转移"和"传输层可插拔"用完全一致的模式
做了两遍。**借鉴清单 #6（多信令容错）在 Go 侧有现成形状可抄**，不必去 trystero 的
TS 生态找（trystero 是库，peerfs 需要的是服务端/节点侧）。

### 14.5 sendbeam 的**发布物完整性链**（完全没讲过的一个维度）

之前 12 节全部聚焦传输协议与信令，没人讲**供应链完整性**。sendbeam 有四层：

| 层 | 证据 | 作用 |
|---|---|---|
| **minisign 签名** | 仓库根 `minisign.pub` + `scripts/minisign.go`（自带 Ed25519 实现） | 发布二进制可验签，防篡改/防假冒 release |
| **SBOM** | `scripts/generate-sbom.sh` + `_test.sh` | 软件物料清单，可审计依赖 |
| **Homebrew** | `Formula/sendbeam.rb` | 包管理器分发，用户可 `brew install` |
| **SRI** | `apps/web/sri.ts` + `sri.test.ts` | 浏览器端 Subresource Integrity，防 CDN/代理注入 |

`scripts/` 里还有 `generate-fuzz-corpora.go`、`generate-package-manifests.go`、
`generate-update-manifest.go`（更新通道清单）。**这条对 peerfs 的现实意义**：
wintools 用 `v*` tag 触发 CI 发布 `.exe`（见 AGENTS.md），用户从 GitHub release 下载——
**目前没有任何签名或校验和机制**。`minisign.pub` + `scripts/minisign.go` 是一个
可直接参照的极简方案（Ed25519，无第三方依赖）。

### 14.6 trystero 的 **SharedPeerManager + passive 模式 + 两个开关**

之前 §11.3 只讲了 AES-GCM、OfferPool、抖动 announce。还有四个没讲的：

- **`SharedPeerManager`**（`packages/core/src/shared-peer.ts:128`）：
  `byApp[appId][peerId]` 的 `SharedPeerState` 表 + `getHealth(peer): 'live' | 'stale'` +
  `isPeerUnderlyingStale()` + `setRoomPresenceHandler()`。即**同一个底层
  PeerConnection 在多个 room 之间复用**，并带健康态判定与空闲回收
  （`strategy.ts:45` 的 `sharedPeerIdleMsDefault = 123_333`，约 2 分钟）。
- **`passive?: boolean`**（`types.ts:68`）+ `passiveActivationGraceMs = 7_533`
  （`strategy.ts:45`）+ 测试套件 `passive-mode.spec.ts` / `passive-scale.spec.ts`：
  **被动监听模式**——先挂着不主动 announce，被需要时才激活，带 7.5s 宽限期。
- **`trickleIce?: boolean`**（`types.ts:71`）：trickle ICE 是**开关**，不是默认行为。
  对照 §11.10——`webrtc-web-proxy` 用 `<-gatherComplete` 硬等，而 trystero 把它做成了
  可配置项。
- **`relayConfig: { urls?: string[], redundancy?: number }`**（`types.ts:63-65`）：
  **多信令容错是一个数字**——`redundancy` 直接指定同时挂几个后端。这比 §11.3 里说的
  "并行多 relay + `announceErrorStreaks`"更简洁。

### 14.7 一句话汇总

| 没讲过的东西 | 来源 | 对 peerfs 的可用度 |
|---|---|---|
| 64MB 头 + 16MB 尾磁盘预热 + 稀疏洞跟踪 | tiramisu `internal/warmup/warmup.go` | **高**（直接命中 moov box 场景，但 GPL 只能抄思路） |
| 动态 TURN 凭证签发（HMAC-MD5 + TTL + Redis） | filepizza `src/coturn.ts` | **高**（比 §10.3 的 instant.io 方案更对，BSD-3） |
| 流式 ZIP 打包下载 | filepizza `src/zip-stream.ts` | 中（功能需求尚不明确） |
| Go 侧信令引擎注册表 + chain + redis | meepo `pkg/signaling/` | **高**（借鉴清单 #6 的 Go 侧形状，MIT） |
| 发布物签名 + SBOM + Homebrew + SRI | sendbeam `minisign.pub`、`scripts/`、`Formula/`、`apps/web/sri.ts` | **高**（wintools 目前 release 无签名） |
| SharedPeer 跨房间复用 + passive 模式 | trystero `shared-peer.ts:128`、`types.ts:68` | 中（peerfs 是定向连接，复用收益低） |
| `redundancy` 数字式多后端 + `trickleIce` 开关 | trystero `types.ts:63-65,71` | **高**（配置项级改动） |
| instant.io 的 SW 是空壳 | instant.io `static/sw.js` | 反面证据（加强 §3.4） |

---

## 15. 六步流水线对照：发现 → 打洞 → 连接 → DataChannel → 互传文件 → 流式

> 按你描述的实际链路逐步拆解。每格是"做没做 + 怎么做 + 证据"，不是简单打勾。

### 15.1 六步的定义

1. **发现**：两端怎么知道对方存在（拿到对方的 peer ID / 会话句柄）
2. **打洞**：发现后通过 UDP 发包穿透防火墙（标准 ICE / 自研 / TURN 兜底）
3. **连接**：建立 WebRTC PeerConnection（握手完成）
4. **DataChannel**：建立 SCTP 数据通道
5. **互传文件**：**双向**——两端都能当发送方（而不是固定上传方/下载方）
6. **流式**：接收端不必等全量到达就能开始消费（边下边播 / 分块响应）

### 15.2 逐项目矩阵

| 项目 | ① 发现 | ② 打洞 | ③ 连接 | ④ DataChannel | ⑤ 互传文件 | ⑥ 流式 |
|---|---|---|---|---|---|---|
| **peerfs（本仓）** | ✅ **自研** `/discover/nodes?coll=`（`pkg/peerfs/signaling.go:38`） | ✅ **自研反向 STUN** `PunchCandidate`（3 次 Binding Request 从 Pion 监听 socket 发出）+ 早到候选缓存 | ✅ | ✅ 64 通道池（DCEP） | ⚠️ **半双向**，见 15.4 | ✅ SW 虚拟 HTTP 206 |
| `kern/filepizza` | ✅ 自建 Node.js 服务器 + Redis，`createChannel(uploaderPeerID)` / `fetchChannel(slug)`（`src/channel.ts:23,24`） | ⚠️ 标准 ICE + **coturn 动态 TURN**（`src/coturn.ts`） | ✅ | ✅ | ❌ **单向**：`useUploader*` / `useDownloader` 角色硬分离 | ✅ 分块带 `offset` + `ChunkAck`；`zip-stream.ts` 边写边流 |
| `webtorrent/instant.io` | ✅ **WebTorrent 原生**（DHT + tracker，`client/index.js:33`） | ⚠️ 标准 ICE + `/__rtcConfig__` TURN | ✅（simple-peer） | ✅ | ✅ **完全双向**：下载完自动变 seed，每个 peer 都可上传 | ❌ **必须攒完整个 Blob**（`getBlobURL`，无虚拟流） |
| `dmotz/trystero` | ✅ topic pub/sub，9 个后端 | ⚠️ 标准 ICE（`scripts/test-ice.ts` 只是**测试脚本**，非生产打洞） | ✅ | ✅ | ✅ **对称双向**：两端都是对等 sender/receiver | ✅ 裸消息通道天然流式（但它是消息库，不是文件工具） |
| `Novage/p2p-media-loader` | ✅ 自有 core 引擎的 peer 注册 | ⚠️ 标准 ICE | ✅ | ✅ | ⚠️ 传的是**媒体 segment**，下载方同时 seed（超节点），非通用文件 | ✅ **这是它的核心目的**，边播边下 + CDN 兜底 |
| `peers/peerjs-server` | ✅（它的"发现"= 按 peer ID 路由） | ❌ 不做 | ❌ | ❌ | ❌ | ❌ → **只做第 1 步** |
| `MrRobotoGit/tiramisu` | ✅ torrent 协议（tracker/DHT） | ⚠️ 标准 ICE | ✅ | ✅ | ⚠️ torrent 语义下互相传 piece，不是"传一个文件" | ✅ **边下边读**（FUSE + 64MB 头 / 16MB 尾预热） |
| `PeerXu/meepo` | ✅ DNS 泛解析 + `pkg/signaling` 链（`chain` + `redis`） | ⚠️ 标准 ICE + **16 个 STUN**（`option.go:18-33`） | ✅ | ✅ | ❌ **不传文件**：它是 SOCKS5 代理网络（`teleportation`） | ✅ 代理流天然流式 |
| `Akshay7273/sendbeam` | ✅ 自建 `apps/server`（`relay_open` / `peer-joined` 配对，`protocol.md:34`） | ⚠️ 标准 ICE + `icecache.go` + **自带 `stund` STUN 服务器**（`apps/cli/cmd/stund/main.go`） | ✅ | ✅ | ❌ **角色固定不可互换**：`Roles are fixed for the lifetime of a session and select the directional keys`（`protocol.md:18`），方向性密钥 + SPAKE2(P-256) | ✅ block/frame 两层，`DEFAULT_INFLIGHT_BLOCKS=8` |
| `perguth/peertransfer` | ⚠️ 用公共 `0.peerjs.com`，**无自建发现** | ⚠️ 标准 ICE | ✅ | ✅ | ❌ 单向（发件方→收件方） | ⚠️ 一次性发送整个文件 |
| `meduar/webrtc-web-proxy` | ❌ 无 | ❌ | ✅ | ✅ | ❌ 发随机字符串 | ❌ |

### 15.3 按"完成度"分组

**六步全通的只有 3 个**：`instant.io`（WebTorrent 生态完整，但⑥是最弱的——必须攒完）、
`trystero`（⑤⑥强，但它是消息库不是文件工具）、`tiramisu`（⑥最强，但⑤是 torrent 语义）。

**四步止步的**（无⑤互传或无⑥流式）：
- `filepizza` —— 缺⑤（单向），⑥很强
- `sendbeam` —— 缺⑤（角色固定），⑥很强
- `peertransfer` —— 缺⑤⑥
- `meepo` —— 缺⑤（根本不传文件）
- `p2p-media-loader` —— ⑤是媒体语义，⑥最强

**两步的**：`webrtc-web-proxy`（只做③④，发随机字符串）
**一步的**：`peerjs-server`（只做①）

### 15.4 对 peerfs 本身的一条不利发现（必须说）

peerfs 在②打洞上**全表唯一自研**，在①发现上**全表唯一自建轻量发现端点**，在⑥流式上
**全表唯一用 SW 做虚拟 HTTP 206**——这三点优势站得住。

但**⑤互传文件这一格 peerfs 并不满足**，实测：

- **`pkg/peerjs/peer.go:400` 是唯一的 `CreateOffer`**——**Go 端永远是 offerer，浏览器永远是
  answerer**，连接发起方向硬编码不对称。
- **`pkg/peerfs/web/bridge.js`（801 行）没有任何服务路径**：`grep "sendText|handleList|onRead|
  writeFile|serveFile"` 零命中，也没有 `upload` / `FileReader` / `files`。**浏览器端是纯消费者。**
- 结果是：**只有 Go 节点提供服务，浏览器节点只能读**。Go 对 Go 之间是双向的，
  但"浏览器往 Go 传文件"或"浏览器 A 从浏览器 B 传文件"目前做不到。

对照真正双向的两个：`instant.io` 靠 WebTorrent 的 piece 语义（每个 peer 既传也收，
下载完自动 seed）；`trystero` 靠对称的 DataChannel（两端都是 `send`/`onMessage`）。
peerfs 要达到同类能力，需要的是**给浏览器端补一条 serve 路径**（当前协议只有
`read`/`list` 两个请求头，且永远由浏览器发起）——这属于协议层改动，比 §8 清单里的
任何一项都大，**之前几节没有指出这一点，补在这里**。

### 15.5 打洞这一格的细节差异

11 个项目里**没有一个做自研 UDP 打洞**，其余全是标准 ICE + TURN 兜底。但兜底策略
分成三档，值得分开看：

| 档位 | 项目 | 具体做法 |
|---|---|---|
| **自研打洞** | **peerfs（唯一）** | 反向 STUN：从 Pion 监听 socket 主动发 3 次 STUN Binding Request（`PunchCandidate`），配合 `pendingCands` 早到候选缓存 |
| **动态 TURN 凭证** | `filepizza` | `coturn.ts` HMAC-MD5 + TTL + Redis（§14.2），**凭证可过期可撤销** |
| **静态 TURN 凭证** | `instant.io` | `secret/index-sample.js` 里 `username: TODO, credential: TODO` 写死下发 |
| **自带 STUN 服务器** | `sendbeam` | `apps/cli/cmd/stund/main.go` 附带一个 STUN 服务，配 `icecache.go` 的 `ICEConfigTTL=15min` |
| **堆 STUN 数量** | `meepo` | 16 个 STUN 服务器列表（`option.go:18-33`），用数量换成功率 |
| **纯标准 ICE** | `trystero`、`tiramisu`、`p2p-media-loader`、`peertransfer` | 无自研、无 TURN 特殊处理 |

peerfs 的自研反向 STUN 在这个维度上是**唯一的不同设计**，但它**没有 TURN 兜底**——
一旦自研打洞失败就无路可走。而 filepizza 那套动态 TURN 是 peerfs 缺的最直接的一块
（§10.3、§14.2 已给实现形状）。

---

## 16. 移植 filepizza 上传协议的具体改动清单

> 方案 A（全 Go，零 Node）。基于 `pkg/peerfs/node.go`、`pkg/peerfs/web/bridge.js`、
> `pkg/peerfs/signaling.go` 的实际代码行号，以及 filepizza `src/messages.ts`、
> `src/hooks/useUploaderConnections.ts` 的实测。

### 16.0 前提：现有协议已覆盖 filepizza 的大部分语义

对照 filepizza 的 9 种消息，wintools 当前已有 **5 种等价物**：

| filepizza 消息 | wintools 等价物 | 位置 |
|---|---|---|
| `RequestInfo` / `Info` | `list` / `entries` | `node.go:367-368`、`:409` |
| `Start{fileName}` | `read{path, offset, reqId}` | `node.go:369-370`、`:29` |
| `Chunk{offset, bytes:Blob, final}` | `meta` + 二进制帧 + `done` | `node.go:457-482` |
| `Error{msg}` | `err{msg, reqId}` | `node.go:486-488` |
| — | `hello`/`ping`/`pong`（peerjs 层已处理） | `node.go:348`、`:363-365` |

**`read` 已经支持 `offset`**（`node.go:29`），包括**负数 offset 读文件末尾**（`:437-443`，
用于非 faststart 视频的 moov box）。**断点续传的"从某 offset 开始读"这一半已做完**，
缺的只是"接收端确认收到哪一块"。

### 16.1 Phase 1：断点续传（借鉴 #3）——约 40 行

filepizza 的 `ChunkAck` 提供**应用层背压**：接收端确认每块，发送端收到 ack 才发下一块。
断线重连后可声明"我已有 [0, X)，从 X 继续"。

**Go 侧（`pkg/peerfs/node.go`）**：

1. `onMessage`（`:337`）加 `case "ack"`：解析 `ack{reqId, offset, bytesReceived}`，
   记录到当前 active read 的已确认位置（约 5 行）
2. `handleRead` 的发送循环（`:462-480`）改为"发一块 → 阻塞等 ack → 发下一块"。
   当前 `SendThrottled` 已带传输层流控（SCTP 缓冲满会阻塞），加应用层 ack 是**再加一层**
   ——好处是浏览器可精确告诉 Go"我确实收下了这块"，断线后能续传（约 20 行）
3. `Header`（`:45`）加 `AckOffset int64` 字段供 `ack` 消息用（约 1 行）

**浏览器侧（`pkg/peerfs/web/bridge.js`）**：

4. `_onWorkerData`（`:298`）二进制帧分支（`:347-349`）：每次收到 chunk 后计算累计 offset，
   发 `ack{reqId, offset, bytesReceived}`（约 10 行）
5. stream 对象加 `ackedOffset` 字段，断线重连时 `read` 请求带上这个值（约 5 行）

**不改的**：`meta`/`done`/`err` 帧格式不变；`reqId` 关联机制不变；SW 虚拟 HTTP 206 不变。

**一个决定**：filepizza 用 `MAX_CHUNK_SIZE = 256 KB`（`useUploaderConnections.ts:19`），
wintools 用 `chunkSize = 64 KB`（`node.go:40`，注释"SCTP 消息安全上限内"）。
**保持 64 KB 不动**——256 KB 可能超 SCTP 默认流控窗口。

### 16.2 Phase 2：浏览器上传（解决 §15.4 双向缺口）——约 150 行

**Go 侧**：

6. `Config`（`:85`）加 `UploadDir string`（可写目录；空 = 禁用）+ `UploadPassword string`
7. `onMessage`（`:337`）加 `case "upload"` → `handleUpload`
8. 新增 `handleUpload`：接收 `upload{path, offset, bytes, final}`，写入 `UploadDir`，
   回复 `uploadack{offset, final}`（约 40 行，模式照 `handleRead` 的 ack 等待循环）
9. 路径穿越防护：`UploadDir` 下用 `path.Clean` + 前缀校验（复用 `:415` 的模式）

**浏览器侧**：

10. 新增上传函数：选文件 → 分块（64 KB）→ 逐块发 `upload` → 等 `uploadack` → 下一块
    （约 60 行，**直接照搬 filepizza `useUploaderConnections.ts:236-275` 的
    `sendNextChunkAsync` 循环**，把 `conn.send(request)` 换成 bridge.js 的 send 封装）
11. filepizza 的 `bytes: file.slice(offset, end)` 是 `Blob`，DataChannel 的 structured
    clone 原生处理二进制——**不需要 base64**。`bridge.js:181` 的
    `send: function (msg) { rdc.send(msg) }` 已能传 Blob

**协议方向**：Phase 2 是**浏览器 → Go**，Phase 1 是 **Go → 浏览器**。两个方向独立，
可分开上线。

### 16.3 Phase 3：`/api/ice` 端点——约 15 行

filepizza 的 `WebRTCProvider.tsx:34-38` 在创建 `Peer` 前 `POST /api/ice` 拿
`{host, path, iceServers}`。你的信令服务器已是 peerjs 协议实现（`signaling.go:35`
挂在 `/peerjs`），所以只需返回**指向自己**的配置：

```go
mux.HandleFunc("/api/ice", func(w http.ResponseWriter, r *http.Request) {
    json.NewEncoder(w).Encode(map[string]any{
        "host":       r.Host,
        "path":       "/peerjs",
        "iceServers": iceServers,
    })
})
```

**注意**：这步是为 filepizza 浏览器端准备的。若不移植 filepizza 前端、继续改
`bridge.js`，**暂时不需要**——`bridge.js` 目前从 URL 参数读信令配置
（`index.html:157-160`），不需要额外端点。

### 16.4 Phase 4：密码保护上传——约 30 行

filepizza 的 `PasswordRequired`/`UsePassword`（`messages.ts:68-75`）用于保护上传链接。
wintools 当前 `Token`（`Config:88`）是**连接级**校验；filepizza 的是**文件级**。

- Go 侧：`Config` 加 `UploadPassword string`；收到 `upload` 且密码未验证时回复
  `PasswordRequired`；收到 `UsePassword` 后校验（约 20 行）
- 浏览器侧：收到 `PasswordRequired` 时弹密码输入框（约 10 行）

### 16.5 Channel store——按需，非必须

filepizza 用 Redis 存 `slug → uploaderPeerID`（`channel.ts:23-24`），用于"生成分享链接"。
**peerfs 目前不需要**——浏览器已直接知道 Go 节点 ID，不存在"通过 slug 找上传方"的需求。
除非要做"上传方生成短链发别人下载"，否则跳过。届时在 `signalserver.Server` 加内存 map
（`slug → uploaderPeerID` + TTL），~40 行，**不需要 Redis**。

### 16.6 不需要移植的东西

| filepizza 的东西 | 为什么不移植 |
|---|---|
| Next.js 服务器 + Redis | Go 信令服务器已讲 peerjs 协议（`signaling.go:35`），不需要平行 Node 栈 |
| React 前端 | 重写不是集成；`bridge.js` + `sw.js` 已有 SW 虚拟 HTTP 206，比 filepizza 的"无 SW"更优 |
| `zip-stream.ts` | 功能需求不明确，SW 已能按文件流式服务 |
| 分块 256 KB | wintools 刻意用 64 KB（SCTP 安全上限） |
| `updateConnection` 状态管理 | React hooks 范式，bridge.js 用自己的 `worker.pending` Map |

### 16.7 阶段总览

| 阶段 | 内容 | 改动量 | 依赖 | 解决 |
|---|---|---|---|---|
| **Phase 1** | ChunkAck 背压 + 断点续传 | **~40 行** | 无 | 借鉴 #3 |
| **Phase 2** | 浏览器上传（`upload`/`uploadack`） | **~150 行** | Phase 1 的 ack 模式 | §15.4 双向缺口 |
| **Phase 3** | `/api/ice` 端点 | ~15 行 | 无 | 对接 filepizza 前端（仅当移植其 UI） |
| **Phase 4** | 密码保护上传 | ~30 行 | Phase 2 | 文件级访问控制 |
| ~~Channel store~~ | slug → uploaderPeerID | ~40 行 | 无 | **按需跳过** |

**建议顺序**：Phase 1 → Phase 2 → Phase 4。Phase 3 只在移植 filepizza 前端时才需要。

---

## 附录：方法说明

- 10 个仓库 `git clone --depth 1` 到 `/tmp/peerfs-compare/`，未修改仓库任何文件。
- 结论来自 `read` / `grep` 实际读码；未读到的不写。
- `meduar/webrtc-web-proxy` 的"名不副实"结论基于全仓 677 行 Go + 128 行 JS 的通读。
- `instant.io` 的"无虚拟流"结论基于 `client/` 全量（286 行）+ 全仓 grep `MediaSource|addSourceBuffer|new Response(|206` 零命中。
- `filepizza` 的"无加密"结论基于全仓 grep `aes-gcm|CryptoJS|X25519|libsodium|tweetnacl` 零命中。

---

## 17. 目标架构评估：Go + Web 对称 P2P 网络

> 你的目标：**peerjs 支持的 P2P 网络，持久信令服务器，Go 和 Web 两种 peer，
> Go 做持久化部署，Web 作为 consumer 和临时节点，传输数据。**
> 下面逐个组件评估"已有 vs 缺什么"。

### 17.1 你已有的比预期多

查了实际代码，**信令服务器和发现系统已经是 P2P 的了**——没有 Go/Web 的类型区分：

| 组件 | 现状 | 证据 |
|---|---|---|
| **信令协议** | `{type, src, dst, payload}`，按 `dst` 转发，**不区分 peer 类型** | `signalserver.go:12` |
| **Web→Web 连接** | **已可用**——两个浏览器都能 `new Peer()` 拿到 ID，然后 `peer.connect(dstId)` | `bridge.js:239` |
| **Web peer ID** | 已有，由信令服务器分配 | `bridge.js:155` `self.peer.id` |
| **发现系统** | `HandleAnnounce` 接受 `peerId` + `collections` + `nodeType`，**任何 peer 都能注册** | `signalserver.go:356-388` |
| **离线队列** | `dst` 不在线入队（带过期），LEAVE/EXPIRE 不入队 | `signalserver.go:13` |
| **心跳保活** | 客户端每 5s 发 HEARTBEAT | `signalserver.go:15` |

**结论：网络基础设施已经支持对称 P2P。** Go 和 Web 在网络层是平等的。

### 17.2 缺的三样东西

**缺口 1：Web peer 不 announce 自己**

Go 节点有 announce 循环（`node.go:189-203`）：周期性 POST `/discover/announce`，
带 `peerId`、`collections`、`nodeType`、`uptime`、上下行字节数。

Web 端（`bridge.js`）**没有这个循环**——所以其他 peer 从 `/discover/nodes` 查不到它。
改动：bridge.js 加一个 announce 定时器，调 `POST /discover/announce`（约 20 行）。

**缺口 2：Web peer 没有 serve 路径**

`bridge.js`（801 行）是纯消费者——没有 `list`/`read` 处理器（§15.4 已确认：
`grep "handleList|onRead|serveFile"` 零命中）。其他 peer 连上来后无处可问。

改动：bridge.js 加 `list`/`read` 处理器，从浏览器的文件源（IndexedDB /
File System Access API / 用户选择的 File 对象）读数据（约 80 行）。

**缺口 3：Web peer 不 discover 其他 peer**

Go 节点通过 `/discover/nodes?coll=` 获取在线节点列表（`node.go:258-270` 的
`doAnnounce` 里同时拉取）。Web 端没有这个查询。

改动：bridge.js 加一个 discovery 查询，`GET /discover/nodes?coll=<collection>`
（约 10 行），结果用于"我能连到哪些 peer"。

### 17.3 目标网络拓扑

```
                    ┌─────────────────────────┐
                    │   信令服务器（持久）      │
                    │   /peerjs   信令转发     │
                    │   /peerjs/id 分配 ID    │
                    │   /discover/announce    │
                    │   /discover/nodes       │
                    └────────┬────────────────┘
                             │  仅信令（不传数据）
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼───┐   ┌──────▼─────┐  ┌────▼────────┐
     │ Go 节点 A   │   │ Go 节点 B   │  │ Web peer C  │
     │ 持久         │   │ 持久        │  │ 临时         │
     │ serve files │   │ serve files │  │ serve + read │
     │ 24/7        │   │ 24/7        │  │ tab 生命周期  │
     └──────┬──────┘   └──────┬──────┘  └──────┬──────┘
            │                 │                │
            └─────────────────┼────────────────┘
                              │  WebRTC DataChannel（数据）
                         ┌────▼────┐
                         │ Web peer D│
                         │ 临时      │
                         │ read + serve│
                         └───────────┘

  数据流：任何 peer ↔ 任何 peer（Go↔Go, Go↔Web, Web↔Web）
  信令流：所有 peer → 信令服务器（单向，仅握手）
```

**关键性质**：
- Go 节点 = 持久 + 服务端（24/7 提供文件）
- Web peer = 临时 + 服务端 + 客户端（tab 活着就是节点，关掉就消失）
- 信令服务器 = 只做发现 + 握手转发，**不参与数据传输**
- 任意两个 peer 之间可以直连（只要一方知道另一方的 ID）

### 17.4 需要的改动清单

| # | 改动 | 位置 | 行数 | 对应缺口 |
|---|---|---|---|---|
| 1 | Web peer announce 循环 | `bridge.js` 新增 | ~20 | 缺口 1 |
| 2 | Web peer `list` 处理器 | `bridge.js` 新增 | ~30 | 缺口 2 |
| 3 | Web peer `read` 处理器（从文件源读） | `bridge.js` 新增 | ~50 | 缺口 2 |
| 4 | Web peer discovery 查询 | `bridge.js` 新增 | ~10 | 缺口 3 |
| 5 | 文件源抽象（IndexedDB / FS Access / File 对象） | `bridge.js` 新增 | ~40 | 缺口 2 |
| 6 | 其他 peer 连接进来时的 `hello` 握手 | `bridge.js` 新增 | ~15 | 缺口 2 |
| 7 | Web peer 断连时的优雅 LEAVE | `bridge.js` 增强 | ~10 | 缺口 1 |

**总计：约 175 行**，全部在 `bridge.js`，**Go 侧零改动**。

### 17.5 浏览器文件源的选择

Web peer 要 serve 文件，需要一个浏览器侧的文件存储。三个选项：

| 方案 | 能力 | 限制 |
|---|---|---|
| **File System Access API** | 直接访问用户选择的目录，可读写 | 仅 Chromium；需用户显式授权；不可持久（session 级） |
| **IndexedDB** | 持久存储，tab 关了还在 | 需手动写入；读取大文件需分块；无原生范围读 |
| **File 对象** | 最简单，`input[type=file]` 选文件 | 不可持久；tab 关了文件消失；不能修改 |

**建议**：分阶段——先用 **File 对象**（零存储，选了就能 share，tab 关了自然消失，
符合"临时节点"语义），后续再加 IndexedDB 做持久化。

### 17.6 filepizza 在这个架构中的位置

filepizza 的浏览器端逻辑（`useUploaderConnections.ts` + `messages.ts`）是**缺口 2 的
参考实现**——它的 uploader 就是一个"Web peer 当服务端"的完整实现：

- `validateOffset` → 对应 `read` 的 offset 校验
- `sendNextChunkAsync` 循环 → 对应 `read` 的分块发送
- `ChunkAck` 背压 → 对应 Phase 1（§16.1）

**但不需要移植 filepizza 的服务器**——你的信令服务器 + 发现系统已经覆盖了它的
`channel.ts`（Redis 存 slug）和 `/api/ice` 的功能。

### 17.7 实施顺序

| 阶段 | 内容 | 行数 | 产出 |
|---|---|---|---|
| **P0** | Web peer announce + discovery（缺口 1 + 3） | ~30 | Web peer 可被其他 peer 发现 |
| **P1** | Web peer serve 路径（缺口 2，File 对象） | ~105 | Web peer 能向其他 peer 提供文件 |
| **P2** | ChunkAck 背压 + 断点续传（§16.1） | ~40 | 所有方向支持续传 |
| **P3** | IndexedDB 持久化（可选） | ~60 | Web peer 关掉 tab 后文件还在 |
| **P4** | 密码保护（§16.4） | ~30 | 文件级访问控制 |

**P0 + P1 = 你的目标架构**（约 135 行）。P2-P4 是增强。
