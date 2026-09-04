#!/usr/bin/env bash
# run-peerjs-demo.sh — 一键拉起当前仓库里所有 peerjs 相关项目(演示模式)
#
# 启动的组件:
#   1. peerfs-chat server     信令服务器 + 浏览器页面      :8000
#   2. peerfs-chat goclient   文件服务节点 (go-peer)       ws://127.0.0.1:8000/peerjs
#   3. peerfs-node             单二进制演示 (内嵌信令)      :8100 -> /__peerfs/
#   4. peerfs-proxy           twimg ECH 代理节点          ws://127.0.0.1:8000/peerjs
#   5. webrtc-proxy serve     经 PeerJS 公共云 serve 端    (target http://127.0.0.1:8000)
#   6. webrtc-proxy client    经 PeerJS 公共云 client 端   http://127.0.0.1:8080
#
# 用法:
#   scripts/run-peerjs-demo.sh [--build-only] [--no-proxy] [--no-webrtc] [-d DIR]
#   Ctrl+C 或 `kill` 会统一回收所有子进程。
#
# 注意:
#   - 构建缓存默认 /tmp/gocache(工作区外不可写时的既定解法,README 同款)。
#   - webrtc-proxy 两端走 0.peerjs.com 公共云,需要能访问公网;
#     --no-webrtc 可跳过。
#   - peerfs-proxy(twimg 节点)需要公网 ECH;--no-proxy 可跳过。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-/tmp/peerjs-build}"
MEDIA_DIR="${MEDIA_DIR:-/tmp/demo-media}"
GOCACHE="${GOCACHE:-/tmp/gocache}"

SIG_PORT=8000
MN_PORT=8100
WR_CLIENT_PORT=8080
WR_NAME=p2pdemo

BUILD_ONLY=0
NO_PROXY=0
NO_WEBRTC=0

# ---- 参数解析 ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-only) BUILD_ONLY=1 ;;
    --no-proxy)   NO_PROXY=1 ;;
    --no-webrtc)  NO_WEBRTC=1 ;;
    -d)           MEDIA_DIR="$2"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

# ---- 端口占用检查 ----
check_port() {
  local port=$1 name=$2
  if ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${port}\$"; then
    echo "!! 端口 ${port}(${name}) 已被占用,先停掉占用进程再试" >&2
    return 1
  fi
}
check_port $SIG_PORT "peerfs-chat server" || exit 1
check_port $MN_PORT "peerfs-node" || exit 1
[[ $NO_WEBRTC -eq 0 ]] && (check_port $WR_CLIENT_PORT "webrtc-proxy client" || exit 1)

# ---- 构建 ----
mkdir -p "$BUILD_DIR" "$GOCACHE"
echo "==> 构建 5 个 peerjs 相关二进制 (GOCACHE=$GOCACHE)"
(cd "$ROOT" && \
  GOCACHE="$GOCACHE" go build -o "$BUILD_DIR/peerfs-node"   ./cmd/peerfs-node        && \
  GOCACHE="$GOCACHE" go build -o "$BUILD_DIR/peerfs-proxy" ./cmd/peerfs-proxy      && \
  GOCACHE="$GOCACHE" go build -o "$BUILD_DIR/webrtc-proxy" ./cmd/webrtc-proxy      && \
  GOCACHE="$GOCACHE" go build -o "$BUILD_DIR/server"       ./cmd/peerfs-server    && \
  GOCACHE="$GOCACHE" go build -o "$BUILD_DIR/goclient"     ./cmd/peerfs-node)

# ---- 演示媒体 ----
if [[ ! -d "$MEDIA_DIR/img" ]]; then
  echo "==> 初始化演示媒体目录: $MEDIA_DIR"
  bash "$ROOT/scripts/restore_demo_media.sh" "$MEDIA_DIR" >/dev/null
fi

if [[ $BUILD_ONLY -eq 1 ]]; then
  echo "OK 构建完成: $BUILD_DIR"
  exit 0
fi

# ---- 启动并登记 PID ----
PIDS=()
trap 'echo; echo "==> 停止所有演示进程"; kill "${PIDS[@]}" 2>/dev/null || true; wait 2>/dev/null || true' EXIT INT TERM

run() { # name logfile args...
  local name=$1 log=$2; shift 2
  "$@" >"$log" 2>&1 &
  PIDS+=($!)
  echo "   *$name  (pid $!)  log: $log"
}

echo "==> 启动组件"
run "peerfs-chat server" "$BUILD_DIR/server.log" \
  "$BUILD_DIR/server" -addr "0.0.0.0:$SIG_PORT" -web "$ROOT/pkg/peerfs/web"
run "goclient(go-peer)" "$BUILD_DIR/goclient.log" \
  "$BUILD_DIR/goclient" -dir "$MEDIA_DIR" -id go-peer -server "ws://127.0.0.1:$SIG_PORT/peerjs"
run "peerfs-node" "$BUILD_DIR/peerfs-node.log" \
  "$BUILD_DIR/peerfs-node" -listen "0.0.0.0:$MN_PORT" -dir "$MEDIA_DIR" -name demo
[[ $NO_PROXY -eq 0 ]] && run "peerfs-proxy(twimg)" "$BUILD_DIR/peerfs-proxy.log" \
  "$BUILD_DIR/peerfs-proxy" -id twimg-proxy -shost 127.0.0.1 -sport "$SIG_PORT"
if [[ $NO_WEBRTC -eq 0 ]]; then
  run "webrtc-proxy serve" "$BUILD_DIR/webrtc-serve.log" \
    "$BUILD_DIR/webrtc-proxy" -mode serve -name "$WR_NAME" -target "http://127.0.0.1:$SIG_PORT"
  run "webrtc-proxy client" "$BUILD_DIR/webrtc-client.log" \
    "$BUILD_DIR/webrtc-proxy" -mode client -name "$WR_NAME" -listen "127.0.0.1:$WR_CLIENT_PORT"
fi

# ---- 就绪检查 ----
echo "==> 等待服务就绪"
ok=0
for i in $(seq 1 20); do
  sleep 0.5
  if curl -sf -o /dev/null "http://127.0.0.1:$SIG_PORT/" 2>/dev/null \
     && curl -sf -o /dev/null "http://127.0.0.1:$MN_PORT/__peerfs/" 2>/dev/null \
     && { [[ $NO_WEBRTC -eq 1 ]] || curl -sf -o /dev/null "http://127.0.0.1:$WR_CLIENT_PORT/" 2>/dev/null; }; then
    ok=1; break
  fi
done
[[ $ok -eq 1 ]] || { echo "!! 服务未在 10 秒内就绪,查看 $BUILD_DIR/*.log" >&2; exit 1; }

echo
echo "================================================================"
echo " 所有 peerjs 项目已就绪,在 Windows 浏览器打开:"
echo "--------------------------------------------------------------"
echo " 1) peerfs-chat 全家桶(K8S 五合一演示)"
echo "    http://localhost:$SIG_PORT/      (信令+页面,可连 go-peer / twimg-proxy)"
echo " 2) peerfs-node 单二进制演示(内嵌信令,离线零外网依赖)"
echo "    http://localhost:$MN_PORT/__peerfs/"
[[ $NO_WEBRTC -eq 0 ]] && echo " 3) webrtc-proxy 隧道(经 PeerJS 公共云打通 :$SIG_PORT 服务)"
[[ $NO_WEBRTC -eq 0 ]] && echo "    http://localhost:$WR_CLIENT_PORT/   (curl 直测: curl http://127.0.0.1:$WR_CLIENT_PORT/)"
echo "--------------------------------------------------------------"
echo " 节点在线状态: curl http://127.0.0.1:$SIG_PORT/discover/nodes"
echo " Ctrl+C 停止全部。日志: $BUILD_DIR/*.log"
echo "================================================================"

# 前台等待,便于 Ctrl+C 统一回收
while :; do sleep 3600; done