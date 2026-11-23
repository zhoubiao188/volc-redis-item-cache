#!/usr/bin/env bash
# 本地一键起：Redis(6390) + 应用(18090)
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
JAVA=${JAVA:-java}

redis-cli -p 6390 ping >/dev/null 2>&1 || redis-server --port 6390 --daemonize yes --save '' --appendonly no
sleep 1
mkdir -p "$ROOT/logs"
nohup "$JAVA" -jar "$ROOT/target/volc-redis-item-cache.jar" --port=18090 --redis-port=6390 > "$ROOT/logs/app.log" 2>&1 &
sleep 3
curl -s "http://127.0.0.1:18090/health" && echo
curl -s "http://127.0.0.1:18090/admin/bloom/rebuild?from=1&to=200000" && echo
