# PeerFS 中心化服务器（Central Hub Server）部署实战教程

本文档介绍如何编译、部署和运维 **PeerFS 独立中心化服务器（Central Hub Server）**。

中心化服务器负责为所有的边缘媒体节点（`media-node`）和浏览器客户端提供 **WebSocket 信令交换、节点集中注册、心跳维护、以及全网节点分发发现**。

---

## 1. 架构定位

在 PeerFS 体系中，数据传输 100% 走 WebRTC SCTP 直连通道（0 字节经中心服务器），中心化服务器仅承担**控制面（Control Plane）**服务：

```
                    ┌───────────────────────────────────┐
                    │ PeerFS 中心化服务器 (Central Hub)   │
                    │   - WS /peerjs     (信令路由交换)  │
                    │   - GET /peerjs/id (PeerID 分配)  │
                    │   - POST /discover/announce (注册)│
                    │   - GET /discover/nodes     (发现)│
                    └───────────────▲───────────────────┘
                                    │
               ┌────────────────────┴───────────────────┐
               │ 集中注册与信令协商                      │ 节点发现与信令拉取
               ▼                                        ▼
    ┌───────────────────────┐              ┌────────────────────────┐
    │  边缘节点 media-node   │ ◄══════════► │    前端用户网页 / APP   │
    │  (-config-url 指定中心)│   WebRTC P2P │  (拉取全网列表直连播放) │
    └───────────────────────┘   (100% 数据) └────────────────────────┘
```

---

## 2. 部署方案对比

| 方案 | 适用场景 | 对应程序入口 |
| :--- | :--- | :--- |
| **方案 A：独立纯信令与发现中心（推荐）** | 生产环境独立部署在轻量云主机（如 1 核 1G），专职负责信令与发现 | [peerfs-chat/server/main.go](file:///home/luminovoez/wintools/peerfs-chat/server/main.go) |
| **方案 B：一体化 Hub 节点（媒体+中心）** | 既想当中心信令服务器，又同时挂载了本地媒体文件供他人播放 | [cmd/media-node/main.go](file:///home/luminovoez/wintools/cmd/media-node/main.go) |

---

## 3. 方案 A：独立纯信令服务器部署（推荐）

### 3.1 编译二进制文件
在服务器或本地执行编译：
```bash
# 进入 wintools 仓库根目录
go build -o peerfs-server ./peerfs-chat/server
```

### 3.2 命令行启动与参数详解
```bash
./peerfs-server -addr 0.0.0.0:9000 -key peerjs -tokens "token1,token2"
```

| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `-addr` | `0.0.0.0:8000` | HTTP 与 WebSocket 监听地址（例如 `0.0.0.0:9000`） |
| `-key` | `peerjs` | 信令 API Key（客户端与边缘节点配置必须一致） |
| `-tokens`| 空 | **安全白名单**：用逗号分隔合法的 token。设置后仅允许白名单客户端注册，防恶意冒充 |
| `-web` | `./web` | 静态页面目录（可选，若不需要托管网页可留空或忽略） |

---

## 4. 生产环境部署实践（Systemd + Nginx SSL）

在真实的互联网公网环境中，现代浏览器要求 WebRTC 必须运行在 **HTTPS / WSS** 安全上下文下。推荐采用 **Systemd 后台常驻 + Nginx 反向代理配置 SSL 证书**。

### 4.1 配置 Systemd 服务
创建服务单元文件 `/etc/systemd/system/peerfs-server.service`：
```ini
[Unit]
Description=PeerFS Central Signaling and Discovery Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/peerfs
ExecStart=/opt/peerfs/peerfs-server -addr 127.0.0.1:9000 -key peerjs
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

启动并设置开机自启：
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now peerfs-server
sudo systemctl status peerfs-server
```

### 4.2 配置 Nginx 反向代理与 SSL (HTTPS + WSS)
为你的域名（例如 `hub.your-domain.com`）配置 Nginx：

```nginx
# /etc/nginx/conf.d/peerfs-hub.conf

upstream peerfs_backend {
    server 127.0.0.1:9000;
    keepalive 32;
}

server {
    listen 80;
    server_name hub.your-domain.com;
    # 强制跳转 HTTPS
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name hub.your-domain.com;

    # SSL 证书配置（可用 Certbot 免费申请）
    ssl_certificate /etc/letsencrypt/live/hub.your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/hub.your-domain.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    # 1. PeerJS WebSocket 信令端点（支持 WSS 协议升级）
    location /peerjs {
        proxy_pass http://peerfs_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # 2. 节点发现与集中注册 REST 端点
    location /discover {
        proxy_pass http://peerfs_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
    }

    # 3. 根目录 / 可选前端控制台托管
    location / {
        proxy_pass http://peerfs_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

测试配置并重载 Nginx：
```bash
sudo nginx -t && sudo systemctl reload nginx
```

---

## 5. 边缘节点接入与使用示例

中心服务器上线后（假设公网地址为 `hub.your-domain.com`）：

### 5.1 边缘媒体节点连接中心（集中注册）
在你的家庭 NAS、本地工作站、或者其他云服务器上启动 `media-node`：
```bash
# 启动时指定 -config-url 指向中心服务器的发现与信令端点
./media-node \
  -dir /data/movies \
  -name nas-home \
  -signal=false \
  -shost hub.your-domain.com \
  -sport 443 \
  -ssecure=true \
  -config-url https://hub.your-domain.com/discover
```
* 节点每 20 秒向中心发送一次心跳；
* 中心的 `/discover/nodes?coll=media` 将立刻包含此 `nas-home` 节点。

### 5.2 前端网页自动拉取全网节点直连
在前端页面中，只需两行代码即可向中心拉取当前所有在线的节点列表并自由直连：
```javascript
// 1. 向中心查询全网在线媒体节点
const resp = await fetch('https://hub.your-domain.com/discover/nodes?coll=media');
const { nodes } = await resp.json();
console.log('当前全网在线节点:', nodes);
// 输出: [{ peerId: "wt-media-nas-home", lastSeen: 1725371234 }, ...]

// 2. 选择任意一个节点建立 WebRTC 直连
const targetNode = nodes[0];
const fs = new PeerFS({
  host: 'hub.your-domain.com',
  port: 443,
  secure: true,
  peerId: targetNode.peerId,
  conns: 64,
});

await fs.connect();
// 3. 边下边播该节点上的视频
fs.play('/sample.mp4', document.querySelector('video'));
```

---

## 6. 运维与排错要点

1. **端口放行**：
   - 宿主机安全组 / 防火墙需放行 **TCP 80, 443**（Nginx 代理端口）；
   - WebRTC 数据传输在边缘节点与客户端之间走 UDP，中心服务器无需开放 UDP 数据端口。
2. **WebSocket 握手 400 Bad Request**：
   - 检查客户端 `key` 参数是否与中心服务器启动参数 `-key` 完全匹配（默认为 `peerjs`）。
3. **心跳保活机制**：
   - 中心服务器内置心跳过期时间为 90 秒，如果节点超过 90 秒未向 `/discover/announce` 上报心跳，中心会自动将其从 `/discover/nodes` 在线列表中剔除。
