# peerfs-chat — 浏览器经 PeerJS/WebRTC 加载本地文件

让浏览器通过 PeerJS 信令 + WebRTC DataChannel 直连本地 Go 节点，
加载图片/视频等文件。全部内网自建，无需公网 IP。

## 架构

```
┌─────────────────────────────────────────────────────┐
│ 浏览器 (index.html)                                 │
│   peerjs@1.5.4 (CDN unpkg.com)                      │
│   serialization: "raw"                              │
├─────────────────────────────────────────────────────┤
│ ① HTTP GET / ──► 信令服务器 (server/main.go)       │
│   加载页面 + peerjs.min.js                           │
├─────────────────────────────────────────────────────┤
│ ② WS 信令 ──► ws://host:8000/peerjs                 │
│   注册为 web-peer                                   │
│   收发 OFFER/ANSWER/CANDIDATE                       │
│   信令服务器原样转发 src→dst                         │
├─────────────────────────────────────────────────────┤
│ ③ WebRTC DataChannel ◄── P2P ──► Go 客户端          │
│   序列化 "raw" 文本帧=JSON 二进制帧=文件块            │
│   文本帧: list/entries/meta/done/err                 │
│   二进制帧: [4B streamID][chunk]                     │
└─────────────────────────────────────────────────────┘
                         ▲
                         │
               ┌─────────┴─────────┐
               │ Go 客户端           │
               │ (goclient/main.go)  │
               │ 文件服务: list/read  │
               │ 目录: ~/Downloads   │
               │ 并发读取 (goroutine) │
               └─────────────────────┘
```

## 组件

| 组件 | 路径 | 说明 |
|------|------|------|
| 信令服务器 | `server/main.go` | 自建 PeerServer，WebSocket 信令 + REST 借 ID + 静态文件托管 |
| Go 客户端 | `goclient/main.go` | PION WebRTC，文件 list/read 服务，支持并发多路复用 |
| 前端页面 | `web/index.html` | 文件浏览器，目录列表 + 图片/视频预览 |
| 依赖 | `web/peerjs.min.js` | CDN 加载 (unpkg.com) |

## 信令协议

### 连接

```
ws://host:port/peerjs?key=peerjs&id=<myId>&token=<random>&version=1.5.4
```

参数的 `key` 必须与服务器 `-key` 一致，`id` 不能重复（重复且 token 不匹配则被拒绝）。

### 全部消息格式

所有消息都是 **JSON 文本帧**，基本结构：

```json
{
  "type":    "消息类型",
  "src":     "源 peer ID（服务器自动填充，客户端发时忽略）",
  "dst":     "目标 peer ID（客户端发时需要，服务器转发用）",
  "payload": {}
}
```

### 客户端 → 服务器

**HEARTBEAT** — 心跳，每 5 秒发送一次：
```json
{"type":"HEARTBEAT"}
```

**OFFER** — 发起 WebRTC 连接：
```json
{
  "type":"OFFER",
  "dst":"go-peer",
  "payload":{
    "type":"data",
    "connectionId":"dc_8f3a2b1c",
    "label":"dc_8f3a2b1c",
    "reliable":true,
    "serialization":"raw",
    "sdp":{
      "type":"offer",
      "sdp":"v=0\r\no=- ..."
    }
  }
}
```

**ANSWER** — 回复 OFFER：
```json
{
  "type":"ANSWER",
  "dst":"web-peer",
  "payload":{
    "type":"data",
    "connectionId":"dc_8f3a2b1c",
    "sdp":{
      "type":"answer",
      "sdp":"v=0\r\no=- ..."
    }
  }
}
```

**CANDIDATE** — 发送 ICE 候选：
```json
{
  "type":"CANDIDATE",
  "dst":"go-peer",
  "payload":{
    "type":"data",
    "connectionId":"dc_8f3a2b1c",
    "candidate":{
      "candidate":"candidate:1 1 UDP 2122252543 ...",
      "sdpMid":"0",
      "sdpMLineIndex":0
    }
  }
}
```

**LEAVE** — 断开连接，dst 为空时表示本机下线：
```json
{"type":"LEAVE", "dst":"go-peer"}
```

### 服务器 → 客户端

**OPEN** — 注册成功：
```json
{"type":"OPEN"}
```

**ID-TAKEN** — ID 被占用（相同 ID 但 token 不匹配）：
```json
{"type":"ID-TAKEN", "payload":{"msg":"ID is taken"}}
```

**ERROR** — 错误：
```json
{"type":"ERROR", "payload":{"msg":"No id, token, or key supplied"}}
```

**OFFER/ANSWER/CANDIDATE** — 转发自其他 peer，payload 原样透传，src 由服务器填充：
```json
{
  "type":"OFFER",
  "src":"web-peer",
  "dst":"go-peer",
  "payload":{...}
}
```

**LEAVE** — 对端断开：
```json
{"type":"LEAVE", "src":"web-peer"}
```

**EXPIRE** — 对端超时（90 秒无心跳）：
```json
{"type":"EXPIRE", "src":"web-peer"}
```

### 离线缓存

如果目标 peer 不在线，OFFER/ANSWER/CANDIDATE 会被缓存到队列中，
等目标上线后立即投递。LEAVE 和 EXPIRE 不缓存。

### REST 端点

| 端点 | 说明 | 返回格式 |
|------|------|----------|
| `GET /peerjs/id?ts=...&version=1.5.4` | 借随机 ID | `a3f8b2c1`（纯文本） |
| `GET /peerjs/peers` | 列出在线 peer | `["go-peer","web-peer"]`（JSON 数组） |

## 数据通道协议 (serialization: "raw")

文本帧 = JSON 控制头，二进制帧 = 文件块。

### 目录列表

```
→ {"type":"list","path":"/","reqId":"r1"}
← {"type":"entries","entries":[{"name":"img","dir":true},{"name":"a.jpg","dir":false,"size":12345}],"reqId":"r1"}
```

### 文件读取 (支持并发)

```
→ {"type":"read","path":"a.jpg","offset":0,"size":-1,"reqId":"r2"}
← {"type":"meta","total":12345,"streamId":1,"reqId":"r2"}
← <4B streamID=1 big-endian><chunk data 64KB>   二进制帧
← <4B streamID=1 big-endian><chunk data 64KB>   二进制帧
← ...
← {"type":"done","reqId":"r2"}
```

### 错误

```
← {"type":"err","msg":"file not found","reqId":"r1"}
```

### 并发支持

每个 `read` 请求分配唯一 `streamId`（uint32），二进制帧前 4 字节为 big-endian stream ID，
浏览器根据 stream ID 将数据块路由到正确的请求。多个 `read` 可同时进行。

## 启动

### 先决条件

- Go 1.26+
- 网络访问 unpkg.com（加载 peerjs.min.js）
- 可选：STUN 服务器（跨 NAT 需要，同机 localhost 不需要）

### 运行

```bash
cd /mnt/d/Workplace/wintools/peerfs-chat

# 终端 1：信令服务器
GOCACHE=/tmp/gocache go run ./server -addr 0.0.0.0:8000 -web ./web

# 终端 2：Go 客户端（文件服务）
GOCACHE=/tmp/gocache go run ./goclient \
  -dir ~/Downloads \
  -id go-peer \
  -server ws://127.0.0.1:8000/peerjs
```

### 浏览器访问

```
http://localhost:8000/
```

1. 点「注册」→ 绿点亮，显示"信令已连接"
2. 点「连接」→ 显示"已连接 -> go-peer"
3. 自动列出目录文件
4. 点文件夹进入，点图片「预览」查看

### 命令行参数

**信令服务器 (server/main.go)：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-addr` | `:8000` | 监听地址 |
| `-path` | `/` | 挂载路径 |
| `-key` | `peerjs` | API key |
| `-web` | `./web` | 静态文件目录 |

**Go 客户端 (goclient/main.go)：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-server` | `ws://127.0.0.1:8000/peerjs` | 信令服务器地址 |
| `-key` | `peerjs` | API key |
| `-id` | `go-peer` | 本机 peer ID |
| `-dir` | `.` | 服务目录 |
| `-stun` | `stun:stun.l.google.com:19302` | STUN/TURN 服务器 |

## 安全性

- 信令服务器可选 token 白名单（`-tokens` 参数）
- 数据通道可选 hello 认证（`-token` 参数），默认无认证
- 路径穿越防护：`path.Clean` + `filepath.Join` 限制在服务目录内
- 浏览器和客户端在同一内网时建议使用 `127.0.0.1`

## 已知限制

- 同一 DataChannel 支持并发读取，但发送端 `dc.Send` 加锁避免乱序
- 大文件整进内存（Blob），未做流式播放
- 不支持断点续传
- 仅支持 peerjs 1.5.4 的 `raw` 序列化

## 节点类型 (nodeType)

每个节点可以声明自己的 `nodeType`，用于区分功能：

| nodeType | 说明 |
|----------|------|
| `file` | 本地文件服务节点（goclient） |
| `twimg` | Twitter 图片代理节点（通过 ECH 从 video-cf.twimg.com 抓取） |
| `proxy` | 通用代理节点 |

启动时指定：
```bash
# 文件节点（自动 nodeType=file）
go run ./goclient -dir ~/Downloads -id node-media -server ws://host:8000/peerjs

# Twitter 代理节点（自动 nodeType=twimg）
go run ./cmd/peerfs-proxy -id twimg-proxy -shost 127.0.0.1 -sport 8000
```

## 节点统计

节点广播以下统计信息：

| 字段 | 说明 |
|------|------|
| `uptime` | 在线时长（秒） |
| `uploadBytes` | 上传字节数 |
| `downloadBytes` | 下载字节数 |

浏览器每 10 秒刷新节点列表，自动显示统计信息。

---

## peerfs-proxy：ECH 代理节点

`cmd/peerfs-proxy` 是一个通过 ECH（Encrypted Client Hello）域前置抓取 Twitter 媒体的代理节点，
同时支持任意 URL 抓取，并通过 WebRTC DataChannel 提供给浏览器。

### 虚拟路径

| 路径 | 说明 |
|------|------|
| `twimg/<path>` | 从 `https://video-cf.twimg.com/<path>` 抓取（Twitter 媒体 CDN） |
| `url/<encoded_url>` | 从任意 URL 抓取（需先 url encode） |

浏览器通过 `list` 看到两个虚拟目录 `twimg/` 和 `url/`，进入后为空（无子目录）。

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-id` | `twimg-proxy` | 本机 peer ID |
| `-shost` | `127.0.0.1` | 信令服务器 host |
| `-sport` | `8000` | 信令服务器端口 |
| `-collections` | `proxy,twitter,images` | 节点标签（逗号分隔） |
| `-token` | `""` | 数据通道 hello 认证 token，空=不认证 |
| `-debug` | `false` | 调试日志 |

### 使用示例

```bash
# 编译
cd cmd/peerfs-proxy
go build -o peerfs-proxy

# 启动（无认证，默认连 127.0.0.1:8000 信令）
./peerfs-proxy -id my-twimg -shost 127.0.0.1 -sport 8000

# 启动（带 token 认证）
./peerfs-proxy -id my-twimg -token SECRET -shost 127.0.0.1 -sport 8000
```

### 协议特性

- **64KB 分块发送**：使用 `SendThrottled` 流控，避免大文件打爆发送缓冲
- **Range 支持**：响应 `offset`/`size` 参数，发 HTTP Range 请求头，支持断点续传
- **ECH 域前置**：通过 `cloudflare-ech.com` 加密连接目标服务器
- **并发多流**：多个 `read` 请求可同时进行，通过 `streamId` 解复用
- **hello 认证**：可选 token 校验，防止未授权访问

---

## 前端使用说明

### 连接流程

1. 打开 `http://<信令服务器>:8000/`
2. 在连接栏输入信令 host（默认 127.0.0.1）和端口（默认 8000）
3. 点"连接信令"
4. 节点列表显示在线节点
5. 点击节点卡连接

### Token 认证

如果节点配置了 `-token`，浏览器需要输入对应 token：

1. 在连接栏的 token 输入框填入 token
2. 信令连接后，浏览器会自动发送 `hello` 帧附带 token
3. token 正确则连接成功，错误则状态栏显示错误

token 可通过 URL 参数传入：`http://host:8000/?token=SECRET`

### 数据通道协议

#### hello 帧（token 认证）

```
→ {"type":"hello","token":"SECRET"}
```

浏览器在 DataChannel 打开后立即发送 hello 帧（token 可空）。
服务端：
- 无 token 配置：忽略 hello，直接处理后续请求
- 有 token 配置：校验 token，匹配后放行；不匹配则返回 err 帧并关闭连接

#### 目录列表

```
→ {"type":"list","path":"/","reqId":"r1"}
← {"type":"entries","entries":[{"name":"twimg/","dir":true},{"name":"url/","dir":true}],"reqId":"r1"}
```

#### 文件读取

```
→ {"type":"read","path":"twimg/media/xxx.jpg","offset":0,"size":-1,"reqId":"r2"}
← {"type":"meta","total":12345,"streamId":1,"reqId":"r2"}
← <4B streamID=1 big-endian><chunk data 64KB>   二进制帧
← ...
← {"type":"done","reqId":"r2"}
```

#### 快捷 fetch

```
→ {"type":"fetch","path":"https://example.com/photo.jpg","reqId":"r3"}
← （同 read 协议，自动转换 path 为 url/<encoded_url>）
```

#### 错误

```
← {"type":"err","msg":"bad token","reqId":""}
← {"type":"err","msg":"file not found","reqId":"r1"}
```
