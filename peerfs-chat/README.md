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

- 信令服务器无 token 校验（演示用途）
- 数据通道无认证（可自行扩展 hello 帧）
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
| `file` | 本地文件服务节点 |
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
