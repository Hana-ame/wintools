#!/bin/bash
# deploy_api.sh - 分步部署 API server 到 BWH
# 每步执行后按 Enter 继续下一步
# 用法: bash deploy_api.sh

set -euo pipefail

SSH="/mnt/d/WorkPlace/wintools/ssh_bwh.sh"
BIN_SRC="/tmp/api-server"
BASE="http://97.64.30.221:8080"

step() {
  local n="$1" desc="$2"
  echo ""
  echo "=========================================="
  echo " Step $n: $desc"
  echo "=========================================="
}

confirm() {
  echo ""
  echo "--- 执行完毕，按 Enter 继续, Ctrl+C 中止 ---"
  read -r
}

# Step 1
step 1 "编译 api-server 为 linux/amd64"
echo 'GOOS=linux GOARCH=amd64 go build -o /tmp/api-server ./cmd/api-server'
GOOS=linux GOARCH=amd64 go build -o /tmp/api-server ./cmd/api-server
confirm

# Step 2
step 2 "上传二进制到服务器 ~/api-server"
echo '${SSH} "cat > ~/api-server" < /tmp/api-server'
${SSH} "cat > ~/api-server" < /tmp/api-server
echo '${SSH} chmod +x ~/api-server'
${SSH} chmod +x ~/api-server
ls -lh /tmp/api-server
confirm

# Step 3
step 3 "在服务器上启动 API server（端口 8080）"
echo '${SSH} "pkill api-server 2>/dev/null; nohup ~/api-server > ~/api-server.log 2>&1 &"'
${SSH} "pkill api-server 2>/dev/null; nohup ~/api-server > ~/api-server.log 2>&1 &"
sleep 3
echo '${SSH} "cat ~/api-server.log"'
${SSH} "cat ~/api-server.log"
confirm

# Step 4
step 4 "测试 healthz"
echo "curl -sk --socks5 172.29.80.1:10808 ${BASE}/healthz"
HEALTH=$(curl -sk --socks5 172.29.80.1:10808 "${BASE}/healthz")
echo ">>> $HEALTH"
confirm

# Step 5
step 5 "测试 KV 写/读/删"
echo "# 写入"
curl -sk --socks5 172.29.80.1:10808 -X POST "${BASE}/kv/test" -d '{"hello":"world"}'
echo ""
echo "# 读取"
curl -sk --socks5 172.29.80.1:10808 "${BASE}/kv/test"
echo ""
echo "# 删除"
curl -sk --socks5 172.29.80.1:10808 -X DELETE "${BASE}/kv/test"
echo ""
echo "# 验证 404"
curl -sk --socks5 172.29.80.1:10808 -w "\nHTTP: %{http_code}\n" "${BASE}/kv/test"
confirm

# Step 6
step 6 "测试长轮询"
echo "后台读取(wait=15)，2s 后写入"
curl -sk --socks5 172.29.80.1:10808 --max-time 20 "${BASE}/kv/lp_test?wait=15" &
CURL_PID=$!
sleep 2
curl -sk --socks5 172.29.80.1:10808 -X POST "${BASE}/kv/lp_test" -d '{"longpoll":"ok"}'
echo ""
wait $CURL_PID
echo "长轮询 OK"
confirm

echo ""
echo "=========================================="
echo " 全部完成！API server 已在 BWH 上运行。"
echo " 内部地址: http://localhost:8080"
echo " 外部测试: socks5://172.29.80.1:10808 → ${BASE}"
echo "=========================================="
