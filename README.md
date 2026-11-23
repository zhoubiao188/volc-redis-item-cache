# volc-redis-item-cache

> 一个 8.6 秒的商品详情接口，接入**火山引擎缓存数据库 Redis 版**后，命中缓存 **2ms** 返回；
> 再把缓存穿透、击穿、雪崩、热 Key、大 Key 五个坑逐个打掉。全部数据为本机实跑，脚本可复现。

```
改造前：GET /api/item/{id}  →  库存 1400ms + 价格 2600ms + 评论 1900ms + 推荐 2700ms = 8644ms
改造后：GET /api/item/{id}  →  p50 2ms / p99 15ms，15481 QPS
```

## 一、这个项目做了什么

| 场景 | 开关 | 结果 |
| --- | --- | --- |
| S0 裸奔：直连四个下游 | `v=0` | 单请求 8644ms，20 并发 2.4 QPS |
| S1 只加 cache-aside | `v=1` | **15481 QPS，p50 2ms，p99 15ms** |
| S2 缓存穿透（10 万随机无效 id / 6 秒） | `v=1` | 6 秒内 55495 次回源 |
| S2 加布隆 + 空值缓存 | `v=2` | 回源 155 次（-99.7%），布隆拦截 69233 次 |
| S3 缓存击穿（热 Key 过期瞬间 300 并发） | `v=2` | 300 次回源，源站瞬时并发 300 |
| S3 加单飞 + 分布式锁 | `v=3` | **回源 1 次**，299 个请求被合并 |
| S4 缓存雪崩（400 Key 同时到期） | `v=3` | 源站瞬时并发 320 |
| S4 加 TTL 抖动 ±20% | `v=4` | 瞬时并发 202（-37%），118 个请求命中未过期 Key |
| S5 热 Key（单商品 200 并发 5 秒） | `v=4` | 20232 QPS，p99 51ms |
| S5 加进程内 L1（500ms TTL） | `v=5` | **34191 QPS（+69%），p99 30ms** |
| S6 大 Key 首屏拉全量 | `view=full` | 1000621 字节 / 3.5ms |
| S6 大 Key 改 Hash 摘要 | `view=summary` | **165 字节 / 1.5ms（流量 -99.98%）** |

完整原始输出见 [`logs/bench-result.txt`](logs/bench-result.txt)。

## 二、架构

```
                    ┌──────────────────────────────┐
   HTTP 请求 ─────▶ │  ItemCacheService            │
                    │  L1 本地缓存（LRU + 500ms）   │
                    └──────────────┬───────────────┘
                                   │ miss
                    ┌──────────────▼───────────────┐
                    │  火山引擎缓存数据库 Redis 版   │
                    │  item:full:{id}  String(JSON) │
                    │  item:sum:{id}   Hash（首屏）  │
                    │  item:null:{id}  空值（60s）   │
                    │  bloom:item      Bitmap 布隆  │
                    │  lock:item:{id}  SET NX 锁    │
                    └──────────────┬───────────────┘
                                   │ miss
                    ┌──────────────▼───────────────┐
                    │ 单飞（in-flight 合并）        │
                    └──────────────┬───────────────┘
                                   │
        ┌──────────┬───────────────┼──────────────┬───────────┐
     库存服务    价格中心        评论服务        推荐召回
    1400ms     2600ms          1900ms          2700ms
```

## 三、快速开始

```bash
# 1. 起一个 Redis（本地用开源 6.x 即可，命令与火山引擎 Redis 版一致）
redis-server --port 6390 --daemonize yes

# 2. 打包
export JAVA_HOME=$(/usr/libexec/java_home -v 19)
mvn -q package -DskipTests

# 3. 启动（默认 :18090）
java -jar target/volc-redis-item-cache.jar --port=18090 --redis-port=6390

# 4. 建布隆位图（线上放在发布流程里跑，见第五节）
curl "http://127.0.0.1:18090/admin/bloom/rebuild?from=1&to=200000"

# 5. 一把梭压测
./bench/bench.sh
```

单条命令看效果：

```bash
curl -o /dev/null -w "%{time_total}s %{size_download}B\n" "http://127.0.0.1:18090/api/item/1234?v=0"   # 8.6s
curl "http://127.0.0.1:18090/api/item/1234?v=1" > /dev/null                                             # 首次回源
curl -o /dev/null -w "%{time_total}s\n" "http://127.0.0.1:18090/api/item/1234?v=1"                       # 0.002s
```

## 四、目录

```
src/main/java/com/example/volcredis/
├── App.java                     启动、HTTP Server
├── cache/
│   ├── CacheFlags.java          v=0..5 逐级开关
│   ├── ItemCacheService.java    ★ 核心：L1/Redis/空值/布隆/单飞/锁/抖动
│   ├── BloomFilter.java         Bitmap 布隆，244KB 管 20 万商品
│   └── LocalCache.java          LRU + 500ms TTL 进程内缓存
├── redis/Redis.java             Jedis 封装：SET NX、HMGET、Lua 位图查询
├── source/ItemRepo.java         模拟四个慢下游（1400/2600/1900/2700ms）
├── model/ItemDetail.java        商品详情
└── web/
    ├── ApiHandler.java          /api/item/{id}、/admin/*
    └── Stats.java               命中率、回源次数、源站瞬时并发
bench/
├── bench.sh                     六个场景一键跑
└── LoadGen.java                 零依赖压测器（QPS / P50 / P95 / P99 / 并发曲线）
```

## 五、接火山引擎缓存数据库 Redis 版

代码里只需要换连接方式，命令与开源 Redis 6.0 完全一致：

```java
JedisPooled pool = new JedisPooled(
        new HostAndPort("redis-xxxxxxxx.redis-cn-beijing.volces.com", 6379),
        DefaultJedisClientConfig.builder()
                .password("your-password")
                .connectionTimeoutMillis(2000)
                .socketTimeoutMillis(2000)
                .build());
```

接入清单：

1. **白名单**：把 ECS/容器所在 VPC 网段加到实例白名单，不要用 0.0.0.0/0。
2. **同可用区**：应用和 Redis 放同一可用区，跨 AZ 的 RTT 会从 0.3ms 涨到 1~2ms，热 Key 场景很致命。
3. **连接数**：`maxTotal` 按「实例连接数上限 ÷ 副本数」算，别让 200 个 Pod 各开 200 连接。
4. **慢查询**：`slowlog get 20`，缓存本身不该出现在慢日志里；出现就是大 Key 或 `KEYS *`。
5. **监控**：命中率、QPS、连接数、大 Key 扫描、内存使用率，四项以上报警。

## 六、踩过的两个真坑

1. **布隆过滤器冷启动把全站打成 404**：位图为空时 `mightContain` 一律返回 false，所有正常商品都被判成"不存在"。修复方式是位图未建好时 **fail-open**（直接放行），并用 `bloom:ready` 标记控制。
2. **TTL 抖动被取整抹平**：`(int)(5 * (0.8 + rand*0.4))` 只会得到 4 或 5，5 秒 TTL 的抖动完全失效。改成毫秒级 `SET...PX` 后，峰值才真的从 320 降到 202。

## 七、许可

MIT
