# 8.6 秒的商详接口，我把它压到 2 毫秒：火山引擎 Redis 版缓存穿透 / 击穿 / 雪崩实战

商品详情接口，改造前单次调用 **8644ms**。

四个下游：库存 1400ms、价格 2600ms、评论 1900ms、推荐召回 2700ms，串行加起来就是 8.6 秒。20 并发压上去只有 **2.4 QPS**，P99 8653ms——线上的网关 3 秒就超时了，用户看到的是空白页。

这篇文章记录我怎么用**火山引擎缓存数据库 Redis 版**把它压到 **P50 2ms / P99 15ms**，以及过程中为了扛住穿透、击穿、雪崩、热 Key、大 Key 这五个坑，写的每一行代码和踩的每一个坑。所有数字都是本机实跑，压测脚本和工程都在文末仓库里，可以逐条复现。

## 一、先量化问题，别急着加缓存

我把改造拆成六个可独立开关的档位（`v=0` 到 `v=5`），每加一个开关就压一轮。这样你能清楚看到「哪一行代码换来了多少 QPS」。

| 场景 | 开关 | 单请求 / 吞吐 | P99 |
| --- | --- | --- | --- |
| 直连下游，无缓存 | `v=0` | 8644ms，20 并发 2.4 QPS | 8653ms |
| 只加 cache-aside | `v=1` | **15481 QPS** | **15ms** |
| + 布隆 + 空值缓存 | `v=2` | 无效 id 回源 55495 → 155 次 | — |
| + 单飞 + 分布式锁 | `v=3` | 热 Key 过期瞬间回源 300 → **1** 次 | — |
| + TTL 抖动 | `v=4` | 雪崩瞬时并发 320 → 202 | — |
| + 本地 L1 | `v=5` | 热 Key 20232 → **34191 QPS** | 51ms → **30ms** |

先看最扎心的一张对比：同一个接口，`v=0` 和 `v=1` 各打 10 秒。

```
v=0  threads=20  n=21      qps=2.4      p50=8648ms  p99=8653ms
v=1  threads=50  n=155112  qps=15480    p50=2ms     p99=15ms
```

注意 `v=1` 的 15 万请求里，只有 161 次真的回到了源站——命中率 99.9%。

## 二、为什么选火山引擎缓存数据库 Redis 版

选型上有三个硬要求：协议兼容（现有代码全是 Jedis，不想改）、能扛热 Key、有现成的监控告警。

火山引擎 Redis 版这三点都满足：**完全兼容 Redis 6.0 协议**，Jedis / Lettuce / redis-cli 直接连；支持主从、集群两种形态；控制台上命中率、QPS、连接数、内存、慢日志都能直接看。代码侧只改连接串：

```java
JedisPooled pool = new JedisPooled(
        new HostAndPort("redis-xxxxxxxx.redis-cn-beijing.volces.com", 6379),
        DefaultJedisClientConfig.builder()
                .password("your-password")
                .connectionTimeoutMillis(2000)
                .socketTimeoutMillis(2000)
                .build());
```

三个接入细节，踩过才知道疼：

1. **必须和应用同可用区**。跨 AZ 的 RTT 从 0.3ms 涨到 1~2ms，热 Key 场景下这 1ms 就是吞吐的生死线。
2. **白名单写 VPC 网段，别写 0.0.0.0/0**。Redis 没有 TLS 的实例尤其注意。
3. **连接池按实例上限反推**。200 个 Pod × maxTotal=200 就是 4 万连接，实例连接数先爆。我按「实例上限 ÷ 副本数 ÷ 1.5」留余量。

## 三、第一刀：cache-aside，8.6 秒 → 2 毫秒

最朴素的做法：读缓存，没有就回源并回填。

```java
public ItemDetail get(long id, CacheFlags f) {
    ItemDetail cached = readFull(id);          // GET volc:item:full:{id}
    if (cached != null) {
        stats.cacheHit.incrementAndGet();
        return cached;
    }
    ItemDetail v = ItemRepo.loadSerial(id);    // 8.6 秒的四个下游
    redis.setPx(fullKey(id), TTL_BASE_MS, gson.toJson(v));
    return v;
}
```

两个设计决定：

**Key 设计**：`volc:item:full:{id}`。前缀带业务名，方便 `SCAN` 定位和按前缀清缓存；`{id}` 用大括号包住，将来迁到集群版时同一个商品的所有 Key 落在同一个 slot，`MGET`、Lua 才不会跨 slot 报错。

**序列化**：JSON 而不是 Java 原生序列化。原生序列化体积大 30%、跨语言不可读、还绑死了类结构；JSON 换来的是能在 redis-cli 里直接 `GET` 出来看，排查问题时省太多事。代价是单次反序列化 1~2ms，可以用 `detailHtml` 这类大字段不参与序列化来省（见第八节）。

效果：**8644ms → 2ms**。Redis 本地 RTT 0.3ms，加上 JSON 反序列化，P50 就落在 2ms。

但这一版上线，只过了 3 分钟我就把它回滚了——因为第一个坑立刻来了。

## 四、坑 1：穿透 —— 6 秒 55495 次回源

缓存穿透是「查一个根本不存在的数据」。缓存永远不命中，请求全部落到源站。攻击者拿 10 万个不存在的 id 刷你，源站就穿了。

实测：50 并发，每次随机一个不存在的 id，打 6 秒。

```
v=1（无布隆）  n=81268  delta: backsource=55495
v=2（有布隆）  n=69442  delta: backsource=155    bloomReject=69233
```

**6 秒 55495 次回源**，等于 9249 QPS 直接压到数据库上。加上布隆之后只剩 155 次，降了 99.7%。

### 4.1 布隆过滤器放在 Redis 的 Bitmap 里

不用额外组件，一个 String 类型的 Bitmap 就够：20 万商品 × 10 bit = 200 万 bit ≈ **244KB**，k=6 时误判率约 0.8%。

```java
private long[] offsets(long id) {
    long a = h1(id);                       // splitmix64
    long b = h1(id ^ 0x5DEECE66DL) | 1L;   // 步长取奇数，才能遍历整个位空间
    long[] out = new long[k];
    for (int i = 0; i < k; i++) {
        out[i] = Math.floorMod(a + i * b, bits);
    }
    return out;
}
```

双哈希（一个起点 + 一个步长）省掉 6 次独立哈希。查询时如果把 6 个 `GETBIT` 一条条发，6 个 RTT 比缓存本身还贵，所以压成一个 Lua：

```lua
local c = 0
for i = 1, #ARGV do
  if redis.call('GETBIT', KEYS[1], ARGV[i]) == 1 then c = c + 1 end
end
return c
```

`c == 6` 才「可能存在」，否则「一定不存在」，直接返回 404，**连缓存都不用查**。

### 4.2 这里出了一个线上级事故：布隆没建好，全站 404

第一版代码写的是：

```java
if (f.bloom() && !bloom.mightContain(id)) {
    return null;   // ← 灾难
}
```

位图是空的，`mightContain` 对所有 id 都返回 false，所有正常商品全被判成「不存在」。**一个空位图 = 一次 100% 的 404 故障。**

正确的写法是 **fail-open**：位图没建好就放行，宁可放过可疑请求，也不能误杀正常流量。

```java
public boolean isReady() {
    long now = System.currentTimeMillis();
    if (now - lastCheck > 2000) {          // 2 秒一次的轻量探活
        ready = redis.exists("volc:bloom:ready");
        lastCheck = now;
    }
    return ready;
}

// 调用侧
if (f.bloom() && bloom.isReady() && !bloom.mightContain(id)) { ... }
```

同时把重建做成一个可以放进发布流程的动作：本地拼出 244KB 的 byte[]，一条 `SET` 写进去，20 万商品重建一次 **19ms**。

```java
public void rebuild(long from, long to, LongPredicate exists) {
    byte[] bitmap = new byte[(int) ((bits + 7) / 8)];
    for (long id = from; id <= to; id++) {
        if (!exists.test(id)) continue;
        for (long off : offsets(id)) {
            bitmap[(int) (off >> 3)] |= (byte) (1 << (7 - (off & 7)));
        }
    }
    redis.setBytes(key, bitmap, 86_400);
    redis.setex("volc:bloom:ready", 86_400, "ok");
}
```

> 顺带说一句：一开始我用 Jedis 的 `pipelined()` 逐条 `SETBIT`，单批 12000 条时直接卡住不返回。改成「本地拼 byte[] + 一条 SET」之后，200 万 bit 一次写完，19ms。批量写位图千万别走 pipeline。

### 4.3 空值缓存兜底

布隆还有 0.8% 误判，误判的请求依然会回源。所以再加一层：查完确认不存在，就往 Redis 写一个 `volc:item:null:{id}`，TTL 60 秒。

```java
if (v == null) {
    redis.setex(nullKey(id), TTL_NULL, "1");
    return null;
}
```

TTL 给 60 秒是有讲究的：太长会在商品上架后持续返回 404；太短等于没缓存。60 秒是可接受的「新商品最多延迟一分钟可见」。

## 五、坑 2：击穿 —— 300 并发打一个刚过期的 Key

热 Key 到期的瞬间，所有请求同时发现缓存没了，同时回源。这叫击穿。

实测：一个热 Key，TTL 设 2 秒，等它过期，然后 300 个请求同时打进来。

```
v=2（无单飞）  backsource=300   peakSrc(first1s)=300   p50=8689ms
v=3（单飞+锁） backsource=1     peakSrc(first1s)=1     inflightJoin=299
```

**300 次回源变成 1 次，299 个请求被合并。** 源站那 4 个下游，本来要被调用 1200 次，现在只调 4 次。

两层防护：

**第一层：进程内单飞（in-flight 合并）**。同一个 JVM 里，相同 id 的并发请求只放一个去回源，其余挂在 `CompletableFuture` 上等结果。

```java
CompletableFuture<ItemDetail> mine = new CompletableFuture<>();
CompletableFuture<ItemDetail> running = inflight.putIfAbsent("sf:" + id, mine);
if (running != null) {
    stats.inflightJoin.incrementAndGet();
    return running.get(20, TimeUnit.SECONDS);   // 等领头的那个回填
}
```

**第二层：Redis 分布式锁**，管跨实例。100 个 Pod 同时发现缓存没了，单飞只能拦住单个 Pod 内部。

```java
// SET lock:item:{id} <uuid> NX PX 5000  —— 一条命令完成互斥 + 防死锁
boolean locked = redis.tryLock(lockKey, token, 5000);
if (!locked) {
    for (int i = 0; i < 25; i++) {           // 拿不到锁就等别人回填，最多 400ms
        sleep(20);
        ItemDetail v = readFull(id);
        if (v != null) return v;
    }
}
```

两个必须注意的点：

1. **锁的 value 必须是唯一 token，释放时要校验**。不然 A 的超时锁被 B 拿到，A 回填完把 B 的锁删了，互斥就形同虚设。释放用 Lua 保证「比对 + 删除」原子：

```lua
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
```

2. **TTL 要大于回源耗时**。我们回源 8.6 秒，锁只给 5 秒——压测里 `lockContend=0` 是因为单飞已经在同一个 JVM 内合并掉了，跨实例才需要靠锁。如果锁 TTL 短于回源时间，会出现「锁过期 → 第二个实例也回源」的假互斥。

## 六、坑 3：雪崩 —— 400 个 Key 同一秒失效

批量的 Key 用同一个 TTL 写入，就会在同一秒集体失效。再叠上一个定时任务同时刷新，源站瞬间被打穿。

实测：400 个 Key，TTL 统一 5 秒，等 5 秒后 400 个并发同时进来。

```
v=3（固定 TTL）  peakSrc=320   backsource=320
v=4（抖动 ±20%） peakSrc=202   backsource=202   cacheHit=118
```

**瞬时并发 320 → 202，降了 37%，另外 118 个请求直接命中了还没过期的 Key。**

实现就是给 TTL 加 ±20% 的随机：

```java
public static long jittered(long baseMs) {
    double factor = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4;
    return (long) Math.max(1, baseMs * factor);
}
```

### 这里有个坑：抖动被整数取整抹平了

第一版我写的是 `(int)(ttlSeconds * (0.8 + rand * 0.4))`，5 秒的 TTL 算出来只有 4 或 5 两个值，实际只差 1 秒，抖动几乎没生效——压测里 `v=3` 和 `v=4` 的峰值一模一样，都是 320。

**根因是秒级精度不够。** 改成毫秒级 `SET key value PX ttlMs`，5 秒 TTL 才能真的散到 4000~6000ms。改完峰值立刻从 320 降到 202。

生产上 TTL 300 秒，±20% 就是 240~360 秒，效果比这个 5 秒的实验更明显。

### 但抖动治不了根

要诚实地说：抖动只是把「同一毫秒 400 个失效」摊成「2 秒内 400 个失效」。当**回源本身要 8.6 秒**时，前面那批还没回来，后面那批又到期了，并发会重新堆起来——所以 `peakSrc(all)` 在抖动的场景下依然能冲到 320。

真正兜底要靠三件事，缺一不可：

- **单飞 + 分布式锁**：把重复回源合并掉（第五节）
- **限流**：给回源链路一个并发上限，超了就直接返回兜底数据，别让源站被打死
- **预热**：大促前把热点商品批量灌进缓存，别等自然失效

抖动是第一道防线，不是唯一一道。

## 七、坑 4：热 Key —— 20232 → 34191 QPS

一个爆款商品，200 个并发持续打 5 秒：

```
v=4（只打 Redis）      qps=20232  p50=7ms   p99=51ms
v=5（加进程内 L1）      qps=34191  p50=4ms   p99=30ms
```

**吞吐涨 69%，P99 从 51ms 降到 30ms。** 而且 Redis 侧的命令数从 10 万降到 113——热 Key 的读流量被挡在了进程里。

做法是经典 L1 + L2：

```java
private final LocalCache l1 = new LocalCache(10_000, 500);  // LRU + 500ms TTL

if (f.local()) {
    ItemDetail hit = l1.get(localKey);
    if (hit != null) {
        stats.localHit.incrementAndGet();
        return hit;      // 一次 Redis 都不走
    }
}
```

**500ms 是有意选的**：它意味着最坏情况下数据有 500ms 不一致。商品详情这种场景完全能接受——价格有单独的实时接口，库存本来就是异步同步的。

如果业务不能接受这 500ms，就把 TTL 换成 **Pub/Sub 广播失效**：更新时 `PUBLISH item:invalidate {id}`，各实例收到后删本地 Key。代价是要维护长连接和断线重连，复杂度上一个台阶。原则是：**能接受短暂不一致就用 TTL，不能接受才上广播**。

## 八、大 Key：1,000,621 字节 → 165 字节

商品详情的 `detailHtml` 图文详情有 1MB 以上。首屏其实只需要标题、价格、库存、评分、评论数、封面 7 个字段，但代码里 `GET` 了一次全量 JSON：

```
view=full     bytes=1000621   time=3.5ms
view=summary  bytes=165       time=1.5ms
```

**流量降 99.98%。** 1MB 的 Key 在网关、内网、反序列化上都要走一遍，高并发下这部分带宽和 CPU 全是白烧的。

改法是把「首屏字段」单独存成一个 Hash：

```java
redis.hsetAll(sumKey(id), v.summary(), ttl);   // HSET + PEXPIRE
// 读的时候只取需要的字段
List<String> f = redis.hmget(sumKey(id), "id", "title", "price", "stock", "rating", "reviewCount", "cover");
```

一个 Key 拆成两种形态：`item:full:{id}` 给详情页，`item:sum:{id}` 给列表页和首屏。Hash 的额外好处是可以只更新单个字段（`HSET item:sum:97 price 9900`），不用整份 JSON 重写。

大 Key 的三条经验：

- **单 Key 别超过 10KB**，超过就拆分。1MB 的 Key 在集群迁移时会直接卡住 slot 搬迁。
- **别用 `KEYS *` 找大 Key**，用 `SCAN` + `MEMORY USAGE key`。
- **列表类数据用 Hash 分片**（`item:desc:{id}:p1`...），别塞进一个 String。

## 九、上线检查清单

改造完准备上线前，我把这份清单贴在需求单上逐条勾：

1. 缓存 Key 有统一前缀，集群版下用 `{}` 保证同 slot
2. 所有 `SET` 都带 TTL，**没有任何一处裸 `SET`**
3. 空值缓存 TTL（60s）短于业务可接受的「新数据可见延迟」
4. 布隆位图有重建入口，且**未就绪时 fail-open**
5. 热点接口有单飞，跨实例有 `SET NX` 锁，锁释放用 token 校验
6. TTL 全部带抖动，且精度足够（毫秒级）
7. 大 Key 已拆分，首屏只读摘要字段
8. 监控四项：命中率、QPS、连接数、大 Key 扫描
9. 降级开关：Redis 挂了要能直接回源（加超时 + 熔断），不能整个接口 500
10. 压测报告进发布单：S0/S1 的 P50、P99、QPS 三组数字

第 9 条特别说一下：`socketTimeoutMillis` 一定不能设太大。缓存超时应该 **50~100ms 就放弃**，然后走源站。缓存是为了快，不是为了正确性——它挂了，接口可以慢，但不能不可用。

## 十、完整工程

所有代码、压测脚本和原始数据都在仓库里，`./bench/bench.sh` 一把梭复现上面全部数字：

```
https://github.com/zhoubiao188/volc-redis-item-cache
```

工程结构：

```
cache/ItemCacheService.java   ★ 主流程：L1 → Redis → 空值 → 布隆 → 单飞 → 锁 → 抖动
cache/BloomFilter.java        Bitmap 布隆，244KB 管 20 万商品，重建 19ms
cache/LocalCache.java         LRU + 500ms TTL
redis/Redis.java              SET NX、HMGET、Lua 位图查询、毫秒级 TTL
bench/bench.sh                六个场景一键压测
bench/LoadGen.java            零依赖压测器，含源站瞬时并发采样
```

一句话总结这次改造：**缓存的难点从来不是「把数据放进 Redis」，而是「Redis 里没有数据的那一瞬间，你的系统会发生什么」**。穿透、击穿、雪崩、热 Key、大 Key，全都是那一瞬间的不同变体。把那一瞬间想清楚，8.6 秒到 2 毫秒只是顺带的结果。
