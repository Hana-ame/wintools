# peerfs — 浏览器经 PeerJS/WebRTC 加载本地文件（图片/视频）

**日期**: 2026-08-24
**组成**: `pkg/peerfs` + `pkg/peerjs` 扩展 + `cmd/media-node`

## 目标

浏览器用标准 [peerjs](https://peerjs.com) 库直连任意后端 Go 节点，经 WebRTC
DataChannel 加载图片/视频等文件。信令服务器内嵌在后端 node 进程里，**可选开启关闭**。

## 现成组件复用（不重造轮子）

| 组件 | 来源 | 说明 |
|------|------|------|
| 信令服务器 | [`github.com/Hana-ame/go-peerserver` v0.1.0](https://pkg.go.dev/github.com/Hana-ame/go-peerserver) | peerdrive `back/signalserver` 拆出的独立模块，peerjs-server 协议子集，token 白名单、离线队列 |
| Go 信令客户端 + DataChannel | 本仓库 `pkg/peerjs` | 本次扩展（见下） |
| ~~go-webrtc-demo~~ | `/mnt/d/Workplace/go-webrtc-demo` | 自造信令协议（非 peerjs），仅作参考，未合并 |

```
浏览器 (peerjs npm, serialization:"raw")
   │ ① GET http://node:port/__peerfs/      ← 控制台页+config.json（go:embed）
   │ ② WS 信令 → 同进程 go-peerserver       ← -signal=true 时；false 则连外部信令/公共云
   │ ③ WebRTC DataChannel 直连（数据不经信令）
   ▼
后端 node（pkg/peerfs 包 pkg/peerjs）
```

## pkg/peerjs 本次改动

1. **文本/二进制区分**: 新增 `Message{IsText, Data}` + `OnMessageKind(cb)`。
   浏览器 raw 序列化下 string=文本帧、ArrayBuffer=二进制帧，pion 的 `IsString`
   旧版被丢弃，JSON 控制头与文件块无法路由。旧 `OnMessage(func([]byte))` 保留，
   webrtc-proxy 不受影响。
2. **发送面**: `SendText(s)`（文本帧）、`SendThrottled(b)`（二进制 + 高水位流控，
   轮询 `BufferedAmount()` 而非 pion 替换式 `OnBufferedAmountLow` 回调——并发注册
   会互相覆盖死等，go-peerjs README 记录过）。
3. **serialization 声明 "raw"**（原 "binary" 是 BinaryPack 编码，Go 裸字节对不上）。
4. **ICE 候选缓存**: 候选可能早于远端描述到达（信令无顺序保证），直接
   `AddICECandidate` 报错且候选永久丢失 → 连接卡 checking。现按浏览器 peerjs
   行为缓存并在 `SetRemoteDescription` 成功后回放。
5. **retrieveID scheme 修正**: 原写死 https，自托管 http 信令借 ID 必挂。
6. **ICEHook**: `Config.ICEHook func(*webrtc.Configuration, *webrtc.SettingEngine)`
   允许定制 PC 配置。

## 帧协议（勿破坏，与 go-peerjs 约定一致）

文本帧 = JSON 控制头，二进制帧 = 文件块：

```
→ {"type":"hello","token":"..."}                 连接后首帧必发，10s 未过认证关连接
→ {"type":"list","path":"/","reqId"}
← {"type":"entries","entries":[{name,dir,size}],"reqId"}    文本帧一次到齐
→ {"type":"read","path":"a.jpg","offset":0,"size":-1,"reqId"}
← {"type":"meta","total":N,"reqId"}              文本帧
← <N 字节二进制块，64KB/块>                       二进制帧 × k（无逐块头）
← {"type":"done","reqId"}                        文本帧
任何一步出错 ← {"type":"err","msg":"...","reqId"}
```

- 浏览器按 `meta.total` 计数收块（DataChannel 可靠有序，无需逐块分帧）；
- 同一连接严格 lock-step：服务端在消息回调内同步处理，上一响应发完才处理下一个头；
- 视频进度条 seek：整文件拉成本地 Blob 后随意 seek；超大文件后续可用 offset 分段 + MSE;
- 路径穿越防护由 `http.Dir.Open` 的 Clean 语义保证。

## 用法

### 独立命令

```bash
go run ./cmd/media-node -dir /path/to/media -listen :8090
# 内嵌信令默认开；浏览器开 http://localhost:8090/__peerfs/

# 关内嵌信令，连公共云：
go run ./cmd/media-node -signal=false -name mynode
# 关内嵌信令，连自托管 peerserver：
go run ./cmd/media-node -signal=false -shost sig.example.com -sport 9000

# 安全加固：
go run ./cmd/media-node -token <data-token> -stoken <sig-token>   # 数据面 hello 校验 + 信令白名单
```

### 库用法

```go
mux := http.NewServeMux()
peerfs.MountSignaling(mux, "peerjs", tokens) // 可选：内嵌信令
node := peerfs.New(peerfs.Config{
	PeerID: "wt-media-x", Root: "/data", Token: "pin",
	Signaling: peerfs.Signaling{Host: "", Port: 443, Key: "peerjs"}, // 空 host = 公共云
})
node.MountConsole(mux) // /__peerfs/ 控制台 + config.json
node.Start(ctx)
http.ListenAndServe(":8090", mux)
```

### 浏览器端（bridge.js，~150 行 UMD）

```html
<script src="https://unpkg.com/peerjs@1.5.4/dist/peerjs.min.js"></script>
<script src="bridge.js"></script>
<script>
const fs = await new PeerFS({ peerId: 'wt-media-x' /* +host/port/secure/key/token */ }).connect();
const entries = await fs.list('/');
const url = await fs.url('video.mp4');   // Blob objectURL，喂 <video src>
const blob = await fs.read('a.png', { offset: 0, size: -1, onProgress });
</script>
```

配置解析优先级：URL 参数（?peer=&host=&port=&secure=&key=） > 节点注入的
config.json > 默认公共云。页面可脱离节点静态托管（此时手动填信令坐标）。

## 测试

```bash
go test ./pkg/peerfs/ -count=1                          # 单元：hello 门禁/list/read/range/穿越/控制台
go test -tags integration ./pkg/peerfs/ -count=1        # 端到端：真信令(httptest)+真WebRTC，Go↔Go 全链路
```

集成测试 ICE 注意（WSL2 沙箱踩坑）：pion/ice **跳过 loopback 接口**（lo 上一个
候选都不产）；只设 `SetNAT1To1IPs` 不够——它改宣告地址不改 socket 绑定。正确
姿势 = `SetInterfaceFilter(eth0)` + 宣告 eth0 真实 IP（本机自发自收走内核本地路由）。

## 已知限制 / 后续

- 同一连接串行处理请求（lock-step）；画廊多图并发加载可在 bridge 层开多连接。
- 大视频整文件进内存（Blob）；MSE 流式播放留待需要时做。
- 信令 token 白名单开启后浏览器需带 token 连信令（stock peerjs 支持 `token` 选项）。
