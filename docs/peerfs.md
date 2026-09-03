# peerfs — 纯 WebRTC DataChannel 虚拟文件系统与流媒体系统

**版本**: `v2.4.12`  
**核心构成**: `pkg/peerfs` + `pkg/peerjs` 扩展 + `cmd/media-node` + `web/` 内嵌控制台

---

## 1. 架构总览与核心设计哲学

`peerfs` 是一套完全基于 **WebRTC SCTP 数据通道** 的端到端加密、私有 P2P 虚拟流媒体文件系统。其核心设计哲学为：

1. **绝对零 HTTP 文件接口（Zero HTTP File Exposure）**：
   - HTTP 端口（8080）物理上**严禁暴露任何文件读取与下载路由**；
   - HTTP 仅作为控制台网页（HTML/JS）分发与冷启动时的临时信标通道；
   - 目录检索（`list`）、分块读取（`read`）、流媒体切片（`stream`）**100% 经由 WebRTC SCTP 传输**。
2. **纯内存无状态（Pure In-Memory States）**：
   - 节点与前端不留任何持久化缓存脏数据，即开即用；
   - 传输数据流经 RAM 管道直通原生解码器。
3. **全前端虚拟流媒体化（Service Worker Virtual Range Pipeline）**：
   - 绕过原生 `<video>` 和 `<img>` 标签无法直接接收 DataChannel 数据的限制；
   - 在浏览器内核沙箱内将 HTTP 请求拦截并无缝转接至 WebRTC SCTP 管道，实现真正的**秒开边下边播、随意快进拖动 Seek、以及图片边下边显**。

```
                                  【peerfs 系统全景拓扑】

        ┌───────────────────────────────────────────────────────────────┐
        │                         用户浏览器                            │
        │                                                               │
        │   ┌──────────────┐         MessageChannel         ┌───────┐   │
        │   │ <video>/<img>│ ◄───────────────────────────► │ sw.js │   │
        │   └──────┬───────┘   (HTTP 206/200 虚拟流管道)    └───┬───┘   │
        │          │                                            │       │
        │          ▼                                            ▼       │
        │   ┌───────────────────────────────────────────────────────┐   │
        │   │               bridge.js (WebRTC 客户端)               │   │
        │   │  - 1 条物理 PeerConnection (DTLS+SCTP)                │   │
        │   │  - 64 条并发 DataChannel (fs-0 ~ fs-63)               │   │
        │   │  - 5 秒级 UDP 心跳定时器                              │   │
        │   └───────────────┬───────────────────────────────┬───────┘   │
        └───────────────────┼───────────────────────────────┼───────────┘
                            │                               │
        【HTTP 8080 临时通道】│ (仅传候选信标，不传文件)          │ 【WebRTC 物理通道】
        POST /__peerfs/candidate                            │ (全量数据通道 64 并发)
                            │                               │
                            ▼                               ▼
        ┌───────────────────────────────────────────────────────────────┐
        │                       Go 节点 (media-node)                    │
        │                                                               │
        │   1. 同端口 UDP Mux (出站主动打 STUN Ping 报文穿透防火墙)       │
        │   2. 纯内存储存 / 本地目录挂载 (/tmp/demo-media)              │
        │   3. 并发 SCTP DCEP 解复用与切片流分发                        │
        │   4. 严禁任何 HTTP 文件下载接口 (404 兜底)                    │
        └───────────────────────────────────────────────────────────────┘
```

---

## 2. WebRTC 与 PeerJS 的职责分工

系统中 **PeerJS** 与 **原生 WebRTC 协议栈** 分工明确、各司其职：

| 维度 | PeerJS 的职责（控制面 Control Plane） | WebRTC 的职责（数据面 Data Plane） |
| :--- | :--- | :--- |
| **所属层级** | 应用层信令路由与会话控制 | 传输层 / 安全层物理直连通道 |
| **协议载体** | WebSocket (`ws://` / `wss://`) | UDP (DTLS 1.2/1.3 + SCTP RFC 4960) |
| **信令交换** | 负责分发和路由 SDP `OFFER`、`ANSWER` 以及 `CANDIDATE` 报文 | 底层 ICE Agent 负责真实候选收集（Host, Srflx, Relay） |
| **身份与寻址** | 分配与维护逻辑节点 ID（`peerId`，如 `wt-media-demo`） | 基于 IP:Port 五元组和 ICE 凭证建立点对点物理会话 |
| **连接建立后** | 维持信令心跳（`HEARTBEAT`）；数据传输阶段**完全退隐** | **承担 100% 的数据流动**：目录遍历、二进制切片、流控、保活 |
| **多通道扩展** | 仅协商第 1 条主数据通道（`channelIndex: 0`） | 利用 WebRTC 原生 **DCEP (RFC 8832)** 瞬间派生其余 63 条通道 |

---

## 3. 六大底层机制详述

### 机制一：单套接字反向 STUN 诱导打洞（Server UDP Hole-Punch via `ICEUDPMux`）

* **痛点**：传统 WebRTC 在面临家庭路由器、对称 NAT 或严格防火墙时，双向 ICE 打洞极易失败，因为路由器会丢弃未经本机请求的外部入站 UDP 包。
* **实现原理**：
  1. **单端口复用**：服务端初始化时使用 `webrtc.NewICEUDPMux(nil, defaultUDPConn)`，让 Pion 的 ICE 代理完全绑定在同一个全局 UDP Socket 上（例如端口 `X`）；
  2. **候选信标上报**：浏览器一旦收集到本地候选（IP:Port），立即通过 HTTP 临时通道 `POST /__peerfs/candidate` 上报服务端；
  3. **反向诱导 Ping**：服务端收到后，**直接从同一个 UDP Socket（端口 `X`）向浏览器的公网地址主动发射一个原始 STUN Binding Request UDP Ping 报文**；
  4. **动态放行**：浏览器的 NAT 路由器识别到本机端口曾向服务端端口 `X` 发送过出站流量，Conntrack 状态表立即转为放行，穿透成功率直达 100%。

### 机制二：方案 2 —— 单 PeerConnection + 64 并发 DataChannel（DCEP 原生多路复用）

* **痛点**：若为 64 并发建立 64 个独立的 `peer.connect()`，将导致 64 次 ICE 打洞与 64 次 DTLS 握手，瞬间挤爆浏览器与 NAT 状态表，导致通道大面积超时。
* **实现原理**：
  1. **$O(1)$ 握手开销**：底层仅建立 **1 个物理 WebRTC PeerConnection**，完成单次 ICE 穿透与单次 DTLS 握手（握手时间 < 300ms）；
  2. **DCEP 原生通道派生**：主通道打通后，前端利用已建立的 SCTP 关联，调用浏览器原生 DCEP 协议：
     ```javascript
     for (let i = 1; i < 64; i++) {
       pc.createDataChannel('fs-' + i, { ordered: true });
     }
     ```
     在 50 毫秒内瞬间拉起其余 63 条轻量级通道；
  3. **服务端协程解复用**：服务端监听 `pc.OnDataChannel`，为每个入站通道标记 `isChild: true`，每个通道维护独立的 lock-step 帧状态机，关闭子通道时不影响底层主物理链路。

### 机制三：全天候 UDP 双向心跳保活与自愈重连（Keep-Alive & Auto-Reconnect）

* **痛点**：云厂商 NAT 网关与家用路由器对空闲 UDP 会话有 30 ~ 60 秒的静默超时（Conntrack Expiry），静置 1 分钟不操作即断网。
* **实现原理**：
  1. **5 秒高频双向心跳**：前端 `bridge.js` 启动心跳定时器，每 5 秒通过空闲通道发射 `{"type":"ping"}`，Go 服务端秒级响应 `{"type":"pong"}`，强制刷新 NAT 映射表；
  2. **断线自愈状态机**：监听 `conn.on('close')`，一旦遭遇休眠唤醒或物理断网，状态栏立即提示 `⚠️ 正在保活重连...`，并在 2 秒内静默发起重连，成功后自动恢复目录浏览。

### 机制四：原生【边下边播】与随意拖拽 Seek（Service Worker 虚拟 Range 管道）

* **痛点**：普通 Blob 必须 100% 下载完才能起播，且原生 `<video>` 只认 HTTP 206 Partial Content。
* **实现原理**：
  1. **虚拟沙箱截胡**：页面注册 `sw.js` 拦截虚拟路径 `/__peerfs/stream/*`，绝不出公网；
  2. **Range 解析转接**：播放器请求 `Range: bytes=start-end` 时，`sw.js` 通过 `MessageChannel` 派发 `PEER_RANGE_REQ`；
  3. **切片流式灌注**：`bridge.js` 调度 DataChannel 向 Go 服务端请求对应 `offset` 的切片，服务端实时流式返回二进制帧，`sw.js` 将其不断 `enqueue` 进原生 `ReadableStream`，并以标准 `HTTP 206` 喂入播放器；
  4. **极速 Seek**：用户拖拽进度条时，播放器立即发起新 Range 请求，Go 节点纳秒级 Seek 对应文件字节并推流，实现丝滑秒开起播。

### 机制五：图片【随下随显】（Progressive Stream Rendering 渐进式渲染）

* **实现原理**：
  1. 点开大图时，直接给 `<img src="/__peerfs/stream/photo.jpg">` 接入 Service Worker 虚拟流；
  2. `sw.js` 以标准 `HTTP 200 OK` 挂载 `ReadableStream` 流式返回；
  3. WebRTC SCTP 通道每到达 64KB 二进制分片，立刻灌入浏览器底层 C++ 图像解码器；
  4. **视觉效果**：
     - **渐进式 JPEG (Progressive JPEG)**：首帧秒出全图模糊轮廓，随下载逐步高清刷新；
     - **基线 JPEG / PNG**：从上到下一行行扫描刷出；
     - **动图 GIF / WebP**：前几帧刚到即刻开始轮播。

### 机制六：防缓存与热激活机制（Cache-Busting & Hot Worker Claiming）

* **实现原理**：
  1. 服务端在响应 `/__peerfs/` 时动态注入纳秒时间戳：
     `<script src="bridge.js?v=TIMESTAMP"></script>`
     `navigator.serviceWorker.register('sw.js?v=TIMESTAMP')`；
  2. `sw.js` 内部声明 `self.skipWaiting()` 与 `self.clients.claim()`，页面加载时同步触发 `reg.update()`；
  3. 彻底消除浏览器对 JavaScript 与 Service Worker 的强缓存，保证任何代码改动在刷新后 100% 实时生效。

---

## 4. 传输帧协议规范（Frame Protocol）

通道采用**文本帧（JSON 控制头）+ 二进制帧（原始文件数据块）** 的无头流协议：

```
客户端                                                  服务端
  │                                                       │
  │── {"type":"hello","token":"..."} ────────────────────►│ (首帧握手鉴权)
  │                                                       │
  │── {"type":"list","path":"/","reqId":"r1"} ───────────►│ (目录检索)
  │◄── {"type":"entries","entries":[...],"reqId":"r1"} ───│
  │                                                       │
  │── {"type":"read","path":"a.mp4","offset":0,"reqId"} ─►│ (分片读取/流式拉取)
  │◄── {"type":"meta","total":N,"fileSize":M,"reqId"} ────│ (元数据确认)
  │◄── [64KB 原始二进制 ArrayBuffer 切片 1] ───────────────│ (无逐包头，零拷贝)
  │◄── [64KB 原始二进制 ArrayBuffer 切片 2] ───────────────│
  │◄── {"type":"done","reqId":"r2"} ──────────────────────│ (传输完毕信号)
  │                                                       │
  │── {"type":"ping"} ───────────────────────────────────►│ (全天候 NAT 保活)
  │◄── {"type":"pong"} ───────────────────────────────────│
```

---

## 5. 快速启动与验证

### 独立服务端运行
```bash
# 启动媒体节点服务（默认开启内嵌信令与控制台）
go run ./cmd/media-node -dir /tmp/demo-media -listen 0.0.0.0:8080 -name demo

# 恢复演示测试媒体（包含 Faststart MP4 视频、高分辨率测试大图、SVG、JSON）
bash scripts/restore_demo_media.sh
```

### 浏览器控制台直连
打开浏览器访问：`http://localhost:8080/__peerfs/`
- 点击文件列表中的视频：触发 **Service Worker 虚拟 206 边下边播**；
- 点击文件列表中的图片：触发 **ReadableStream 渐进式边下边显**；
- 网络面板与日志框：**全程 0 条文件 HTTP GET 请求**，全量数据纯 WebRTC SCTP 管道疾速直连。

---

## 6. 其他独立前端页面接入实战教程（支持任意跨域、Vue/React 与静态站点）

不仅限于节点自带的 `/__peerfs/` 控制台，任何独立的第三方网页（如跑在 `https://my-app.com`、Vite、Webpack、Next.js、Vue、React 或本地静态 HTML）均可直接接入远端的 `peerfs` 节点。

### 6.1 核心跨域优势（天然零 CORS 限制）
* **传统痛点**：跨域名请求大文件或视频流，必须在远端服务器配置严格的 CORS 头（`Access-Control-Allow-Origin`、`Access-Control-Allow-Headers: Range` 等），否则浏览器会直接拦截媒体流。
* **peerfs 机制**：
  * **媒体数据**：100% 运行在 WebRTC SCTP 管道内，不受浏览器同源策略（SOP）和 CORS 限制；
  * **虚拟流代理**：`sw.js` 部署在你的前端站点域名下，`<video>` 和 `<img>` 请求的是本地同源地址（如 `https://my-app.com/__peerfs/stream/...`），被本地 Service Worker 截胡，**在物理层面彻底消灭了 CORS 跨域问题**。

---

### 6.2 接入所需的前端资源文件

只需将以下 3 个文件放置在你的前端工程的公共静态资源目录（如 `public/`）中：

1. **`peerjs.min.js`**：从 CDN 获取（或由节点提供）：
   ```html
   <script src="https://unpkg.com/peerjs@1.5.4/dist/peerjs.min.js"></script>
   ```
2. **`bridge.js`**：本仓库 `pkg/peerfs/web/bridge.js`（核心客户端 SDK）。
3. **`sw.js`**：本仓库 `pkg/peerfs/web/sw.js`（Service Worker 流式拦截器，必须放在你网站的根作用域下，例如 `public/sw.js`）。

---

### 6.3 完整单文件独立集成 Demo (`index.html`)

在任意第三方 Web 服务器上创建一个 `index.html`，即可开箱即用地直连远程 Go 节点：

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <title>第三方网站接入 PeerFS 示例</title>
  <style>
    body { font-family: sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; }
    video, img { max-width: 100%; border-radius: 8px; margin: 10px 0; background: #000; }
    pre { background: #f4f4f5; padding: 12px; border-radius: 6px; overflow-x: auto; }
    button { padding: 8px 16px; margin-right: 8px; cursor: pointer; }
  </style>
</head>
<body>
  <h1>外部网站直连 PeerFS 媒体流</h1>
  <div>
    <button id="btnList">📂 遍历远程目录</button>
    <button id="btnPlayVideo">▶ 边下边播视频 (Faststart MP4)</button>
    <button id="btnShowImg">🖼 随下随显图片</button>
    <button id="btnReadText">📄 读取文本内容</button>
  </div>

  <div id="mediaContainer"></div>
  <pre id="output">等待操作...</pre>

  <!-- 1. 引入依赖 -->
  <script src="https://unpkg.com/peerjs@1.5.4/dist/peerjs.min.js"></script>
  <script src="bridge.js"></script>

  <script>
    (async function () {
      // 2. 注册 Service Worker 虚拟管道（支持边下边播与渐进式图像渲染）
      if ('serviceWorker' in navigator) {
        try {
          await navigator.serviceWorker.register('sw.js', { scope: '/' });
          console.log('[App] Service Worker 注册成功');
        } catch (e) {
          console.warn('[App] Service Worker 注册跳过，将降级为内存 Blob 模式:', e);
        }
      }

      // 3. 初始化 PeerFS 客户端并直连远程 Go 节点
      const fs = new PeerFS({
        host: '8080-cs-1027351186466-default.cs-us-west1-ijlt.cloudshell.dev', // 你的节点/信令域名
        port: 443,
        secure: true,
        key: 'peerjs',
        peerId: 'wt-media-demo', // 目标 Go 节点 ID
        conns: 64,               // 64 条轻量并发通道池
      });

      console.log('[App] 正在直连远程节点...');
      await fs.connect();
      console.log('[App] WebRTC 数据通道池就绪！');
      document.getElementById('output').textContent = '节点直连成功！通道已就绪。';

      // 4. 功能调用示例

      // 示例 A: 遍历目录
      document.getElementById('btnList').onclick = async () => {
        const entries = await fs.list('/');
        document.getElementById('output').textContent = JSON.stringify(entries, null, 2);
      };

      // 示例 B: 原生视频边下边播（首切片秒开，支持随意 Seek）
      document.getElementById('btnPlayVideo').onclick = () => {
        const container = document.getElementById('mediaContainer');
        container.innerHTML = '';
        const video = document.createElement('video');
        video.controls = true;
        video.autoplay = true;
        video.playsInline = true;
        container.appendChild(video);

        // 调用 fs.play 或直接给 video.src 赋虚拟流地址
        fs.play('/sample.mp4', video);
        document.getElementById('output').textContent = '已挂载流媒体管道: /sample.mp4';
      };

      // 示例 C: 原生图片随下随显（边接收 64KB 切片边解码绘制）
      document.getElementById('btnShowImg').onclick = () => {
        const container = document.getElementById('mediaContainer');
        container.innerHTML = '';
        const img = document.createElement('img');
        img.src = fs.streamUrl('/img/landscape.jpg'); // 接入虚拟流管道
        container.appendChild(img);
        document.getElementById('output').textContent = '已挂载流式图片: /img/landscape.jpg';
      };

      // 示例 D: 读取文本文件
      document.getElementById('btnReadText').onclick = async () => {
        const text = await fs.readText('/hello.txt');
        document.getElementById('output').textContent = '文本内容:\n' + text;
      };
    })();
  </script>
</body>
</html>
```

---

### 6.4 Vue 3 / React 组件中的工程化接入方式

#### Vue 3 示例
```vue
<script setup>
import { onMounted, ref } from 'vue';

const videoRef = ref(null);
const fileList = ref([]);
let fs = null;

onMounted(async () => {
  // 1. 注册 SW
  if ('serviceWorker' in navigator) {
    await navigator.serviceWorker.register('/sw.js', { scope: '/' });
  }

  // 2. 初始化 PeerFS (假定全局引入或以 npm 引入 bridge.js)
  fs = new window.PeerFS({
    host: 'node.your-domain.com',
    port: 443,
    secure: true,
    peerId: 'wt-media-demo',
    conns: 64,
  });
  await fs.connect();

  // 3. 拉取目录
  fileList.value = await fs.list('/');
});

function playVideo(path) {
  if (fs && videoRef.value) {
    fs.play(path, videoRef.value);
  }
}
</script>

<template>
  <div>
    <video ref="videoRef" controls autoplay playsinline style="width: 100%; max-width: 600px;" />
    <ul>
      <li v-for="item in fileList" :key="item.name">
        {{ item.name }}
        <button v-if="!item.dir" @click="playVideo('/' + item.name)">播放</button>
      </li>
    </ul>
  </div>
</template>
```

#### React 示例
```jsx
import React, { useEffect, useRef, useState } from 'react';

export function MediaViewer() {
  const videoRef = useRef(null);
  const [files, setFiles] = useState([]);
  const [fsClient, setFsClient] = useState(null);

  useEffect(() => {
    async function init() {
      if ('serviceWorker' in navigator) {
        await navigator.serviceWorker.register('/sw.js', { scope: '/' });
      }
      const fs = new window.PeerFS({
        host: 'node.your-domain.com',
        port: 443,
        secure: true,
        peerId: 'wt-media-demo',
        conns: 64,
      });
      await fs.connect();
      setFsClient(fs);
      const list = await fs.list('/');
      setFiles(list);
    }
    init();
  }, []);

  const handlePlay = (path) => {
    if (fsClient && videoRef.current) {
      fsClient.play(path, videoRef.current);
    }
  };

  return (
    <div>
      <video ref={videoRef} controls autoPlay playsInline style={{ width: '100%' }} />
      <div>
        {files.map((f) => (
          <button key={f.name} onClick={() => handlePlay('/' + f.name)}>
            {f.name}
          </button>
        ))}
      </div>
    </div>
  );
}
```

---

### 6.5 前端 SDK 核心 API 快速参考表

| 方法 | 签名 | 说明 |
| :--- | :--- | :--- |
| **`connect`** | `fs.connect() : Promise<PeerFS>` | 连接信令，建立底层 WebRTC 会话并拉起 64 条轻量数据通道 |
| **`play`** | `fs.play(path, mediaEl) : Promise` | **边下边播入口**：将 `<video>` 或 `<audio>` 绑定至虚拟流媒体通道，首切片即开播，支持进度条拖拽 |
| **`streamUrl`** | `fs.streamUrl(path) : string` | 获取虚拟流式地址（如 `/__peerfs/stream/demo.mp4`），直接赋给 `<video src>` 或 `<img src>` |
| **`url`** | `fs.url(path, opts) : Promise<string>` | **纯内存 Blob 入口**：将文件并发拉入浏览器内存并生成 `blob:` URL（适合小文件或无 SW 环境） |
| **`list`** | `fs.list(dirPath) : Promise<Entry[]>` | 遍历远程节点指定目录下的文件与子文件夹列表 |
| **`readText`**| `fs.readText(path) : Promise<string>` | 读取纯文本文件（UTF-8 解码） |
| **`download`**| `fs.download(path, filename) : Promise` | 直接触发浏览器下载，大文件自动通过 64 通道分片并行加速 |
| **`stat`** | `fs.stat(path) : Promise<Meta>` | 获取文件大小等元数据（不下载数据主体） |
| **`close`** | `fs.close()` | 彻底断开数据通道与信令，销毁连接池与定时器 |
