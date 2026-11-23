#!/usr/bin/env bash
# 商品详情接口缓存改造 · 全场景压测
#
# 前置：本地 redis-server 已起（端口 6390），应用已起（端口 18090）
# 用法：./bench/bench.sh
#
# 每个场景只比上一次多开一个开关（见 CacheFlags）：
#   S0 v=0      裸奔：直连四个下游
#   S1 v=1      只加 cache-aside
#   S2 v=1/v=2  缓存穿透：随机不存在的 id
#   S3 v=2/v=3  缓存击穿：热 Key 过期瞬间
#   S4 v=3/v=4  缓存雪崩：400 个 Key 同时过期
#   S5 v=4/v=5  热 Key：单 Key 高并发
#   S6 大 Key：1MB 全量 JSON vs Hash 摘要

set -uo pipefail

HOST=${HOST:-http://127.0.0.1:18090}
REDIS_CLI=${REDIS_CLI:-"redis-cli -p 6390"}
JAVA=${JAVA:-java}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
LG="$ROOT/bench/LoadGen.java"
OUT="$ROOT/logs/bench-result.txt"
mkdir -p "$ROOT/logs"

c() { curl -s "$@"; }
reset() { c "$HOST/admin/reset" >/dev/null; }
flush() { c "$HOST/admin/flush" >/dev/null; }
bloom() { c "$HOST/admin/bloom/rebuild?from=1&to=200000" >/dev/null; }
preload() { c "$HOST/admin/preload?start=$1&n=$2&ttl=$3&jitter=${4:-0}" >/dev/null; }
# 预编译压测器：源码模式每次启动要 0.6s+，会让"到期瞬间"错位
BENCH_CLASSES="$ROOT/target/bench-classes"
mkdir -p "$BENCH_CLASSES"
"${JAVA_HOME:-/usr}/bin/javac" -d "$BENCH_CLASSES" "$LG" 2>/dev/null || javac -d "$BENCH_CLASSES" "$LG"
run() { "$JAVA" -cp "$BENCH_CLASSES" LoadGen --host="$HOST" "$@" 2>/dev/null | grep -v '^SLF4J'; }

hr() { printf '=%.0s' {1..78}; echo; }

{
hr
echo "商品详情接口 · 火山引擎缓存数据库 Redis 版改造压测"
echo "机器：$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)  JDK：$("$JAVA" -version 2>&1 | head -1 | sed 's/.*"\(.*\)".*/\1/')"
echo "Redis：$($REDIS_CLI info server 2>/dev/null | grep redis_version | tr -d '\r')"
echo "时间：$(date '+%Y-%m-%d %H:%M:%S')"
hr

echo
echo "【S0】改造前：直连四个下游，串行聚合（单请求 / 20 并发）"
flush; reset
run --mode=burst --threads=1 --ids=1234 --path='/api/item/%d?v=0'
echo "  ↑ 一个请求 8.6 秒，线上超时就是这么来的"
run --mode=load --threads=20 --seconds=8 --ids=1-200000 --path='/api/item/%d?v=0'

echo
echo "【S1】接入 Redis：cache-aside，TTL 300s"
flush; bloom; reset
preload 1 1000 300000
echo "  预热点检 1000 个商品，keys=$($REDIS_CLI dbsize | tr -d '\r')  内存=$($REDIS_CLI info memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')"
run --mode=burst --threads=1 --ids=1234 --path='/api/item/%d?v=1'
run --mode=load --threads=50 --seconds=10 --ids=1-800 --path='/api/item/%d?v=1'

echo
echo "【S2】缓存穿透：每次随机一个不存在的 id，猛打 6 秒（10 万 id 空间，几乎不重复）"
flush; bloom
echo "  -- 未加布隆（v=1）"
reset
run --mode=load --threads=50 --seconds=6 --ids=900001-999999 --path='/api/item/%d?v=1'
echo "  -- 加布隆 + 空值缓存（v=2）"
flush; bloom; reset
run --mode=load --threads=50 --seconds=6 --ids=900001-999999 --path='/api/item/%d?v=2'

echo
echo "【S3】缓存击穿：热 Key 过期瞬间 300 并发"
flush; bloom; reset
preload 12346 1 2000
echo "  热 Key 已预热（TTL=2s），等待过期…"
sleep 2.2
echo "  -- 只有空值缓存 + 布隆，无单飞（v=2）"
reset
run --mode=burst --threads=300 --ids=12346 --path='/api/item/%d?v=2'
echo "  -- 加单飞 + 分布式锁（v=3）"
flush; bloom; reset
preload 12346 1 2000
sleep 2.2
reset
run --mode=burst --threads=300 --ids=12346 --path='/api/item/%d?v=3'

echo
echo "【S4】缓存雪崩：400 个 Key 同时到期，400 并发同一瞬间涌进来"
for v in 3 4; do
  flush; bloom; reset
  if [ "$v" = "3" ]; then
    preload 1 500 5000 0
    echo "  -- TTL 固定 5s，无抖动（v=3）"
  else
    preload 1 500 5000 1
    echo "  -- TTL 抖动 ±20%（v=4）"
  fi
  sleep 5.0
  run --mode=burst --threads=400 --seq --ids=1-400 --path="/api/item/%d?v=$v"
done

echo
echo "【S5】热 Key：单商品 200 并发打 5 秒"
flush; bloom; reset
preload 54321 1 300000
echo "  -- 无本地 L1，每次都打 Redis（v=4）"
reset
run --mode=load --threads=200 --seconds=5 --ids=54321 --path='/api/item/%d?v=4'
echo "  -- 加进程内 L1，500ms TTL（v=5）"
reset
run --mode=load --threads=200 --seconds=5 --ids=54321 --path='/api/item/%d?v=5'

echo
echo "【S6】大 Key：1MB 详情 JSON vs Hash 摘要"
flush; bloom
c "$HOST/api/item/97?v=5" >/dev/null
curl -s -o /dev/null -w "  full   code=%{http_code} time=%{time_total}s bytes=%{size_download}\n" "$HOST/api/item/97?v=5"
curl -s -o /dev/null -w "  sum    code=%{http_code} time=%{time_total}s bytes=%{size_download}\n" "$HOST/api/item/97?v=6&view=summary"
echo "  Redis 中：full=$($REDIS_CLI strlen 'volc:item:full:97' | tr -d '\r') bytes  sum-hash=$($REDIS_CLI hlen 'volc:item:sum:97' | tr -d '\r') fields"

echo
hr
echo "备注：全部为本机实跑数据，未做任何修饰；P50/P95/P99 由 bench/LoadGen.java 统计，"
echo "      QPS = 完成请求数 / 实际耗时（含收尾排空），与 并发数 ÷ 单请求耗时 基本一致。"
hr
} | tee "$OUT"

echo
echo "结果已写入 $OUT"
