# PeerJS DataChannel 加载图片 — 开发者指南

## 协议概述

浏览器 ↔ Go 节点之间通过 WebRTC DataChannel 传输文件。

**信令**：WebSocket 连接信令服务器，交换 OFFER/ANSWER/CANDIDATE 建立 P2P 连接。

**数据通道**：`serialization: "raw"`，文本帧 = JSON 控制头，二进制帧 = 文件块。

**帧协议**：

```
→ {"type":"list","path":"/","reqId":"r1"}
← {"type":"entries","entries":[{name,dir,size}],"reqId":"r1"}

→ {"type":"read","path":"a.jpg","offset":0,"size":-1,"reqId":"r2"}
← {"type":"meta","total":12345,"streamId":1,"reqId":"r2"}
← <4B streamID big-endian><chunk data 64KB>
← <4B streamID big-endian><chunk data 64KB>
← {"type":"done","reqId":"r2"}
```

---

## 一、Vanilla JS 示例

```html
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>PeerFS 示例</title>
</head>
<body>
  <h1>PeerFS 图片加载</h1>
  <p>节点 ID: <input id="peerId" value="node-media"></p>
  <p>信令服务器: <input id="host" value="localhost">
    <input id="port" value="8000" style="width:60px"></p>
  <button onclick="connect()">连接</button>
  <div id="status"></div>
  <div id="files"></div>

  <script src="https://unpkg.com/peerjs@1.5.4/dist/peerjs.min.js"></script>
  <script>
    // 等待连接状态的 Promise
    function waitFor(emitter, event, timeout) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('timeout')), timeout);
        emitter.once(event, () => { clearTimeout(timer); resolve(); });
      });
    }

    // 生成请求 ID
    function genReqId() {
      return 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
    }

    async function connect() {
      const host = document.getElementById('host').value;
      const port = parseInt(document.getElementById('port').value) || 8000;
      const peerId = document.getElementById('peerId').value;

      // 1. 创建 Peer 对象，连接信令服务器
      const peer = new Peer(undefined, {
        host, port, path: '/', key: 'peerjs',
        secure: false,
        config: { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] },
      });

      await waitFor(peer, 'open', 20000);
      document.getElementById('status').textContent = '信令已连接';

      // 2. 连接目标节点
      const conn = peer.connect(peerId, {
        serialization: 'raw',  // 必须 raw 才能传二进制
        reliable: true,
      });

      await waitFor(conn, 'open', 30000);
      document.getElementById('status').textContent = '已连接 ' + peerId;

      // 3. 发送 list 请求，获取文件列表
      const entries = await list(conn, '/');
      renderFiles(conn, entries, '/');
    }

    // 收数据
    function onData(conn, pending, streams) {
      return function(d) {
        if (typeof d === 'string') {
          const h = JSON.parse(d);
          if (h.type === 'meta') {
            streams.set(h.streamId, { chunks: [], received: 0, total: h.total });
            return;
          }
          const p = pending.get(h.reqId);
          if (!p) return;
          pending.delete(h.reqId);
          const st = streams.get(h.streamId);
          streams.delete(h.streamId);
          if (h.type === 'done') {
            p.resolve(new Blob(st ? st.chunks : []));
          } else {
            p.reject(new Error(h.msg || h.type));
          }
          return;
        }
        // 二进制帧：[4B streamID][data]
        const view = new DataView(d);
        const streamId = view.getUint32(0, false);
        const chunk = d.slice(4);
        const st = streams.get(streamId);
        if (st) { st.chunks.push(chunk); st.received += chunk.byteLength; }
      };
    }

    // list 请求
    function list(conn, path) {
      const pending = new Map();
      const streams = new Map();
      conn.on('data', onData(conn, pending, streams));

      return new Promise((resolve, reject) => {
        const reqId = genReqId();
        pending.set(reqId, { resolve, reject });
        conn.send(JSON.stringify({ type: 'list', path, reqId }));
      });
    }

    // read 请求 → 返回 Blob
    function read(conn, pending, streams, filePath) {
      return new Promise((resolve, reject) => {
        const reqId = genReqId();
        pending.set(reqId, { resolve, reject });
        conn.send(JSON.stringify({
          type: 'read', path: filePath,
          offset: 0, size: -1, reqId,
        }));
      });
    }

    // 渲染文件列表
    function renderFiles(conn, entries, path) {
      const el = document.getElementById('files');
      el.innerHTML = '';
      entries.forEach(e => {
        const div = document.createElement('div');
        if (e.dir) {
          div.textContent = '📁 ' + e.name;
        } else {
          div.textContent = '📄 ' + e.name + ' (' + fmtSize(e.size) + ') ';
          const btn = document.createElement('button');
          btn.textContent = '预览';
          btn.onclick = async () => {
            const pending = new Map();
            const streams = new Map();
            conn.on('data', onData(conn, pending, streams));
            try {
              const blob = await read(conn, pending, streams, (path === '/' ? '' : path) + '/' + e.name);
              const url = URL.createObjectURL(blob);
              const img = document.createElement('img');
              img.src = url;
              img.style.maxWidth = '500px';
              document.getElementById('files').appendChild(img);
            } catch(err) {
              alert('加载失败: ' + err.message);
            }
          };
          div.appendChild(btn);
        }
        el.appendChild(div);
      });
    }

    function fmtSize(n) {
      if (!n) return '';
      const u = ['B','KB','MB','GB'];
      let i = 0;
      while (n >= 1024 && i < u.length-1) { n /= 1024; i++; }
      return (i ? n.toFixed(1) : n) + ' ' + u[i];
    }
  </script>
</body>
</html>
```

---

## 二、React 示例 (TypeScript)

```tsx
// PeerFS.tsx
import React, { useState, useEffect, useRef } from 'react';
import Peer from 'peerjs';

interface NodeEntry {
  name: string;
  dir?: boolean;
  size?: number;
}

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (reason: any) => void;
}

function genReqId(): string {
  return 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}

async function readFile(
  conn: Peer.DataConnection,
  pending: Map<string, PendingRequest>,
  streams: Map<number, { chunks: BlobPart[]; received: number; total: number }>,
  filePath: string
): Promise<Blob> {
  return new Promise((resolve, reject) => {
    const reqId = genReqId();
    pending.set(reqId, { resolve, reject });
    conn.send(JSON.stringify({
      type: 'read', path: filePath, offset: 0, size: -1, reqId,
    }));
  });
}

async function listDir(
  conn: Peer.DataConnection,
  pending: Map<string, PendingRequest>,
  streams: Map<number, { chunks: BlobPart[]; received: number; total: number }>,
  path: string
): Promise<NodeEntry[]> {
  return new Promise((resolve, reject) => {
    const reqId = genReqId();
    pending.set(reqId, {
      resolve: (data: any) => resolve(data.entries || []),
      reject,
    });
    conn.send(JSON.stringify({ type: 'list', path, reqId }));
  });
}

function setupDataHandler(
  conn: Peer.DataConnection,
  pending: Map<string, PendingRequest>,
  streams: Map<number, { chunks: BlobPart[]; received: number; total: number }>
) {
  conn.on('data', (data: any) => {
    if (typeof data === 'string') {
      const h = JSON.parse(data);
      if (h.type === 'meta') {
        streams.set(h.streamId, { chunks: [], received: 0, total: h.total });
        return;
      }
      const p = pending.get(h.reqId);
      if (!p) return;
      pending.delete(h.reqId);
      const st = streams.get(h.streamId);
      streams.delete(h.streamId);
      if (h.type === 'done') {
        p.resolve(new Blob(st ? st.chunks : []));
      } else {
        p.reject(new Error(h.msg || h.type));
      }
      return;
    }
    if (data.byteLength < 4) return;
    const view = new DataView(data);
    const streamId = view.getUint32(0, false);
    const chunk = data.slice(4);
    const st = streams.get(streamId);
    if (st) {
      st.chunks.push(chunk);
      st.received += chunk.byteLength;
    }
  });
}

function PeerFSBrowser() {
  const [connected, setConnected] = useState(false);
  const [entries, setEntries] = useState<NodeEntry[]>([]);
  const [currentPath, setCurrentPath] = useState('/');
  const [images, setImages] = useState<string[]>([]);
  const peerRef = useRef<Peer | null>(null);
  const connRef = useRef<Peer.DataConnection | null>(null);
  const pendingRef = useRef<Map<string, PendingRequest>>(new Map());
  const streamsRef = useRef(new Map());

  const connect = async (host: string, port: number, targetId: string) => {
    const peer = new Peer(undefined, {
      host, port, path: '/', key: 'peerjs',
      secure: false,
      config: { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] },
    });

    peer.on('open', async () => {
      const conn = peer.connect(targetId, {
        serialization: 'raw', reliable: true,
      });
      conn.on('open', async () => {
        peerRef.current = peer;
        connRef.current = conn;
        setConnected(true);
        setupDataHandler(conn, pendingRef.current, streamsRef.current);
        const entries = await listDir(conn, pendingRef.current, streamsRef.current, '/');
        setEntries(entries);
      });
    });
  };

  const loadImage = async (filePath: string) => {
    if (!connRef.current) return;
    try {
      const blob = await readFile(
        connRef.current, pendingRef.current, streamsRef.current, filePath
      );
      setImages(prev => [...prev, URL.createObjectURL(blob)]);
    } catch (err) {
      console.error('Failed to load image:', err);
    }
  };

  return (
    <div>
      <h1>PeerFS 图片浏览器</h1>
      {!connected && (
        <div>
          <p>
            信令: <input id="host" defaultValue="localhost" />
            <input id="port" defaultValue="8000" style={{ width: 60 }} />
            节点: <input id="targetId" defaultValue="node-media" />
          </p>
          <button onClick={() => {
            const host = (document.getElementById('host') as HTMLInputElement).value;
            const port = parseInt((document.getElementById('port') as HTMLInputElement).value);
            const targetId = (document.getElementById('targetId') as HTMLInputElement).value;
            connect(host, port, targetId);
          }}>
            连接
          </button>
        </div>
      )}

      {connected && (
        <div>
          <h3>文件列表</h3>
          <ul>
            {entries.map(e => (
              <li key={e.name}>
                {e.dir ? '📁' : '📄'} {e.name}
                {!e.dir && (
                  <button onClick={() => loadImage(currentPath + '/' + e.name)}>
                    预览
                  </button>
                )}
              </li>
            ))}
          </ul>
          <h3>图片预览</h3>
          {images.map((url, i) => (
            <img key={i} src={url} style={{ maxWidth: 300, margin: 4 }} />
          ))}
        </div>
      )}
    </div>
  );
}

export default PeerFSBrowser;
```

---

## 三、协议细节

### 信令服务器

```
ws://host:port/peerjs?key=peerjs&id=<myId>&token=<random>&version=1.5.4
```

| 消息 | 方向 | 说明 |
|------|------|------|
| `OPEN` | 服务器 → 客户端 | 注册成功 |
| `OFFER` | 双向 | WebRTC 会话描述 |
| `ANSWER` | 双向 | WebRTC 应答 |
| `CANDIDATE` | 双向 | ICE 候选 |
| `HEARTBEAT` | 客户端 → 服务器 | 心跳（每 5s） |
| `LEAVE` | 双向 | 断开连接 |

### 数据通道协议

**序列化**：`serialization: 'raw'`

**文本帧**（JSON 控制头）：

| 类型 | 方向 | 说明 |
|------|------|------|
| `list` | → | 请求目录列表 |
| `entries` | ← | 目录列表响应 |
| `read` | → | 请求文件内容 |
| `meta` | ← | 文件元信息（含 streamId） |
| `done` | ← | 文件传输完成 |
| `err` | ← | 错误消息 |

**二进制帧**：
```
[4B streamID 大端序][64KB 数据块]
```

### 启动 Go 节点

```bash
# 信令服务器
go run ./server -addr 0.0.0.0:8000 -web ./web

# 文件节点
go run ./goclient -dir ~/Downloads -id node-media \
  -server ws://127.0.0.1:8000/peerjs
```

---

## 四、关键点

1. **`serialization: 'raw'`** — 必须用 raw，否则二进制数据会被编码错
2. **stream ID** — 每个 read 请求分配唯一 streamId，二进制帧前 4 字节标识属于哪个请求
3. **lock-step 对 list 不适用** — list 在同一连接上串行没问题，但 read 要并发
4. **Blob URL** — 图片预览用 `URL.createObjectURL(blob)`，用完记得 `revokeObjectURL`
5. **跨标签页** — blob URL 只在本页有效，不能右键开新标签页
