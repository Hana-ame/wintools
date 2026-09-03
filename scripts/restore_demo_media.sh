#!/usr/bin/env bash
# scripts/restore_demo_media.sh
# 快速初始化 /tmp/demo-media 演示媒体目录（文本、SVG矢量图、JSON、MDN Faststart MP4等）

set -euo pipefail

TARGET_DIR="${1:-/tmp/demo-media}"
echo "==> 准备初始化演示媒体目录: ${TARGET_DIR}"

mkdir -p "${TARGET_DIR}/img"

# 1. hello.txt
cat << 'EOF' > "${TARGET_DIR}/hello.txt"
Hello from PeerFS P2P file system!
WebRTC DataChannel streaming is ready.
EOF

# 2. sample.svg
cat << 'EOF' > "${TARGET_DIR}/sample.svg"
<svg xmlns="http://www.w3.org/2000/svg" width="400" height="200" viewBox="0 0 400 200">
  <rect width="100%" height="100%" fill="#1a1a24"/>
  <circle cx="100" cy="100" r="60" fill="#4caf50" opacity="0.8"/>
  <text x="180" y="110" font-family="sans-serif" font-size="24" fill="#ffffff">PeerFS Active</text>
</svg>
EOF

# 3. img/logo.svg
cat << 'EOF' > "${TARGET_DIR}/img/logo.svg"
<svg xmlns="http://www.w3.org/2000/svg" width="200" height="200" viewBox="0 0 200 200">
  <rect width="100%" height="100%" rx="30" fill="#3b82f6"/>
  <path d="M50 100 L90 140 L150 60" stroke="#ffffff" stroke-width="16" fill="none" stroke-linecap="round"/>
</svg>
EOF

# 4. sample.json
cat << 'EOF' > "${TARGET_DIR}/sample.json"
{
  "project": "PeerFS",
  "version": "v2.4.11",
  "concurrency": 64,
  "transport": "WebRTC DataChannel",
  "features": [
    "SCTP raw framing",
    "Service Worker Range streaming",
    "Zero HTTP file server exposure",
    "STUN UDP hole punch"
  ]
}
EOF

# 5. sample.md
cat << 'EOF' > "${TARGET_DIR}/sample.md"
# PeerFS 演示说明

这是一个完全基于 **WebRTC DataChannel** 的 P2P 浏览器私有流媒体文件系统。

- **零 HTTP 文件服务泄露**：文件数据仅在数据通道上流转
- **Service Worker 边下边播**：利用虚拟 Range 代理实现无等待秒开
- **64 并行工作连接池**：多通道分片下载加速
EOF

# 6. sample.mp4 (用户指定标准 Faststart H.264 MP4 视频，moov 在头部，秒开播放)
echo "==> 正在下载标准 Faststart MP4 示例视频..."
if ! curl -sL --connect-timeout 5 --max-time 15 \
  "https://img.wangmoyu.com/large/71a2fa67d8fed80ca900a37fafe01933.mp4" \
  -o "${TARGET_DIR}/sample.mp4"; then
  echo "--> 主源超时，切换备用源 (MDN rabbit320.mp4)..."
  curl -sL --connect-timeout 5 --max-time 15 \
    "https://raw.githubusercontent.com/mdn/learning-area/main/html/multimedia-and-embedding/video-and-audio-content/rabbit320.mp4" \
    -o "${TARGET_DIR}/sample.mp4" || true
fi

chmod -R 755 "${TARGET_DIR}"

echo "==> 演示媒体目录恢复完成: ${TARGET_DIR}"
ls -lh "${TARGET_DIR}"
ls -lh "${TARGET_DIR}/img"
