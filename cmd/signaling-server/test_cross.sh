#!/bin/bash
# ============================================================
# Signaling Server — 跨机测试脚本 (KV 后端)
# 用法:
#   SERVER=http://bwh.moonchan.xyz:8080 ./test_cross.sh
# ============================================================
set -euo pipefail
export no_proxy='*'
export NO_PROXY='*'

SERVER="${SERVER:-http://bwh.moonchan.xyz:8080}"
PASS=0
FAIL=0

ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
fail() { FAIL=$((FAIL+1)); echo "  ❌ $1"; }

json_pick() {
  python3 -c "import sys,json; v=json.load(sys.stdin).get('$1',''); print(v if not isinstance(v,bool) else str(v).lower())"
}

curl -s --noproxy '*' "$SERVER/" > /dev/null 2>&1 || { echo "FATAL: server not reachable"; exit 1; }
echo "Target: $SERVER"
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
[ "$JOIN_ERR" = "room full" ] && ok "Room full" || fail "Room full (got $JOIN_ERR)"
NOEXIST=$(curl -s --noproxy '*' -X POST "$SERVER/kv/room/nonexistent/join" | json_pick err)
[ "$NOEXIST" = "room not found" ] && ok "Nonexistent" || fail "Nonexistent (got $NOEXIST)"

echo ""
echo "=== SDP Tests ==="
curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/sdp?peer=p1" -d '{"type":"offer","sdp":"fake_offer"}' > /dev/null
P2_SDP=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/sdp?peer=p2" | json_pick type)
[ "$P2_SDP" = "offer" ] && ok "P2 gets offer" || fail "P2 gets offer (got $P2_SDP)"
curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/sdp?peer=p2" -d '{"type":"answer","sdp":"fake_answer"}' > /dev/null
P1_ANS=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/sdp?peer=p1" | json_pick type)
[ "$P1_ANS" = "answer" ] && ok "P1 gets answer" || fail "P1 gets answer (got $P1_ANS)"
NO_DATA=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/sdp?peer=p1" | json_pick err)
[ "$NO_DATA" = "no data" ] && ok "SDP consumed" || fail "SDP not consumed"

echo ""
echo "=== ICE Tests ==="
curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/ice?peer=p1" -d '{"candidate":"c1"}' > /dev/null
P2_ICE=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/ice?peer=p2" | json_pick candidate)
[ "$P2_ICE" = "c1" ] && ok "P2 pops ICE" || fail "P2 pops ICE (got $P2_ICE)"
curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/ice?peer=p1" -d '{"candidate":"c2"}' > /dev/null
curl -s --noproxy '*' -X POST "$SERVER/kv/room/$ROOM/ice?peer=p1" -d '{"candidate":"c3"}' > /dev/null
ALL_COUNT=$(curl -s --noproxy '*' "$SERVER/kv/room/$ROOM/ice?peer=p2&all" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
[ "$ALL_COUNT" = "2" ] && ok "ICE all (got $ALL_COUNT)" || fail "ICE all (got $ALL_COUNT)"

echo ""
echo "=== KV Tests ==="
KV_ID=$(curl -s --noproxy '*' -X POST "$SERVER/kv/create" | json_pick id)
[ -n "$KV_ID" ] && ok "KV create" || fail "KV create"
curl -s --noproxy '*' -X PUT "$SERVER/kv/$KV_ID" -d '{"hello":"world"}' > /dev/null
KV_VAL=$(curl -s --noproxy '*' "$SERVER/kv/$KV_ID" | json_pick hello)
[ "$KV_VAL" = "world" ] && ok "KV PUT + get" || fail "KV PUT"
curl -s --noproxy '*' -X DELETE "$SERVER/kv/$KV_ID" > /dev/null
KV_404=$(curl -s --noproxy '*' "$SERVER/kv/$KV_ID" | json_pick err)
[ "$KV_404" = "not found" ] && ok "KV delete + 404" || fail "KV delete"

echo ""
TOTAL=$((PASS+FAIL))
echo "  SERVER: $SERVER"
echo "  Pass: $PASS / $TOTAL  Fail: $FAIL / $TOTAL"
[ "$FAIL" -gt 0 ] && echo "FAILED" && exit 1 || echo "ALL PASSED"
