#!/bin/bash
# ============================================================
# Signaling Server — 本地测试脚本 (KV 后端)
# 用法:  SERVER=http://localhost:8080 PORT=8080 ./test_local.sh
# 默认:  http://localhost:8080 :8080
# ============================================================
set -euo pipefail
export no_proxy='*'
export NO_PROXY='*'

SERVER="${SERVER:-http://localhost:8080}"
PORT="${PORT:-8080}"
PASS=0
FAIL=0
ROOM=""  # 公共房间 ID，供后续测试复用

ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
fail() { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

json_pick() {
  python3 -c "import sys,json; v=json.load(sys.stdin).get('$1',''); print(v if not isinstance(v,bool) else str(v).lower())"
}

# ---- 编译 ----
echo "=== Build ==="
go build -o /tmp/sig-server ./cmd/signaling-server/

# ---- 起服务 ----
/tmp/sig-server &>/dev/null &
PID=$!
trap "kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -f /tmp/sig-server" EXIT
sleep 2

curl -s --noproxy '*' "$SERVER/" > /dev/null 2>&1 || { echo "FATAL: server not responding"; exit 1; }
echo "Server up on $SERVER"

# ============================================================
# 房间测试
# ============================================================
echo ""
echo "=== Room Tests ==="

ROOM_JSON=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room")
ROOM=$(echo "$ROOM_JSON" | json_pick room_id)
[ -n "$ROOM" ] && ok "Create room -> $ROOM" || fail "Create room"

P1=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/join" | json_pick peer)
[ "$P1" = "p1" ] && ok "Join p1" || fail "Join p1 (got $P1)"

P2=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/join" | json_pick peer)
[ "$P2" = "p2" ] && ok "Join p2" || fail "Join p2 (got $P2)"

JOIN_ERR=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/join" | json_pick err)
[ "$JOIN_ERR" = "room full" ] && ok "Room full rejected" || fail "Room full (got $JOIN_ERR)"

NOEXIST=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/nonexistent/join" | json_pick err)
[ "$NOEXIST" = "room not found" ] && ok "Nonexistent room" || fail "Nonexistent (got $NOEXIST)"

# ============================================================
# SDP 测试
# ============================================================
echo ""
echo "=== SDP Tests ==="

SDP_OK=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/sdp?peer=p1" -d '{"type":"offer","sdp":"fake_offer"}' | json_pick ok)
[ "$SDP_OK" = "true" ] && ok "P1 sends offer" || fail "P1 sends offer"

P2_SDP=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/sdp?peer=p2" | json_pick type)
[ "$P2_SDP" = "offer" ] && ok "P2 gets offer" || fail "P2 gets offer (got $P2_SDP)"

SDP_OK=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/sdp?peer=p2" -d '{"type":"answer","sdp":"fake_answer"}' | json_pick ok)
[ "$SDP_OK" = "true" ] && ok "P2 sends answer" || fail "P2 sends answer"

P1_ANS=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/sdp?peer=p1" | json_pick type)
[ "$P1_ANS" = "answer" ] && ok "P1 gets answer" || fail "P1 gets answer (got $P1_ANS)"

NO_DATA=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/sdp?peer=p1" | json_pick err)
[ "$NO_DATA" = "no data" ] && ok "SDP consumed (no data)" || fail "SDP not consumed (got $NO_DATA)"

ERR_MSG=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/sdp" -d '{}' | json_pick err)
[ "$ERR_MSG" = "?peer=p1|p2 required" ] && ok "Missing peer param rejected" || fail "Missing peer (got $ERR_MSG)"

# ============================================================
# ICE 测试
# ============================================================
echo ""
echo "=== ICE Tests ==="

ICE_OK=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/ice?peer=p1" -d '{"candidate":"c1"}' | json_pick ok)
[ "$ICE_OK" = "true" ] && ok "P1 sends ICE" || fail "P1 sends ICE"

P2_ICE=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/ice?peer=p2" | json_pick candidate)
[ "$P2_ICE" = "c1" ] && ok "P2 pops ICE" || fail "P2 pops ICE (got $P2_ICE)"

NO_ICE=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/ice?peer=p2" | json_pick err)
[ "$NO_ICE" = "no data" ] && ok "ICE consumed (no data)" || fail "ICE not consumed (got $NO_ICE)"

curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/ice?peer=p1" -d '{"candidate":"c2"}' > /dev/null
curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/ice?peer=p1" -d '{"candidate":"c3"}' > /dev/null
ALL_COUNT=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/ice?peer=p2&all" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
[ "$ALL_COUNT" = "2" ] && ok "ICE all (got $ALL_COUNT)" || fail "ICE all (got $ALL_COUNT)"

# ============================================================
# KV 通用操作测试
# ============================================================
echo ""
echo "=== KV Tests ==="

KV_ID=$(curl -s --noproxy '*' -X POST "$SERVER/kv/create" | json_pick id)
[ -n "$KV_ID" ] && ok "KV create -> $KV_ID" || fail "KV create"

KV_NULL=$(curl -s --noproxy '*' "$SERVER/kv/$KV_ID")
[ "$KV_NULL" = "null" ] && ok "KV get (null)" || fail "KV get (got $KV_NULL)"

curl -s --noproxy '*' -X PUT "$SERVER/kv/$KV_ID" -d '{"hello":"world"}' > /dev/null
KV_VAL=$(curl -s --noproxy '*' "$SERVER/kv/$KV_ID" | json_pick hello)
[ "$KV_VAL" = "world" ] && ok "KV PUT + get" || fail "KV PUT (got $KV_VAL)"

curl -s --noproxy '*' -X PUT "$SERVER/kv/$KV_ID" -d '{"num":42}' > /dev/null
KV_HELLO=$(curl -s --noproxy '*' "$SERVER/kv/$KV_ID" | json_pick hello)
KV_NUM=$(curl -s --noproxy '*' "$SERVER/kv/$KV_ID" | json_pick num)
[ "$KV_HELLO" = "world" ] && [ "$KV_NUM" = "42" ] && ok "KV merge" || fail "KV merge (hello=$KV_HELLO num=$KV_NUM)"

curl -s --noproxy '*' -X DELETE "$SERVER/kv/$KV_ID" > /dev/null
KV_404=$(curl -s --noproxy '*' "$SERVER/kv/$KV_ID" | json_pick err)
[ "$KV_404" = "not found" ] && ok "KV delete + 404" || fail "KV delete (got $KV_404)"

DEL_ERR=$(curl -s --noproxy '*' -X DELETE "$SERVER/kv/nonexistent" | json_pick err)
[ "$DEL_ERR" = "not found" ] && ok "KV delete nonexistent" || fail "KV delete nonexistent (got $DEL_ERR)"

# ============================================================
# 汇总
# ============================================================
echo ""
echo "=============================="
TOTAL=$((PASS+FAIL))
echo "  Pass: $PASS / $TOTAL"
echo "  Fail: $FAIL / $TOTAL"
echo "=============================="
if [ "$FAIL" -gt 0 ]; then
  echo "SOME TESTS FAILED"
  exit 1
else
  echo "ALL TESTS PASSED"
fi
