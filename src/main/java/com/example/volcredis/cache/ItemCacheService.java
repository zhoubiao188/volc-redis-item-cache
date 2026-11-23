package com.example.volcredis.cache;

import com.example.volcredis.model.ItemDetail;
import com.example.volcredis.redis.Redis;
import com.example.volcredis.source.ItemRepo;
import com.example.volcredis.web.Stats;
import com.google.gson.Gson;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 商品详情缓存的核心。所有"坑"都在这个类里，用 CacheFlags 逐级打开。
 */
public class ItemCacheService {

    /** 基础 TTL 300s */
    public static final long TTL_BASE_MS = 300_000L;   // 300s
    /** 空值缓存 TTL 60s */
    public static final int TTL_NULL = 60;

    private static final String[] SUM_FIELDS = {"id", "title", "price", "stock", "rating", "reviewCount", "cover"};

    private final Gson gson = new Gson();
    private final Redis redis;
    private final Stats stats;
    private final LocalCache l1 = new LocalCache(10_000, 500);
    private final ConcurrentHashMap<String, CompletableFuture<ItemDetail>> inflight = new ConcurrentHashMap<>();
    private final BloomFilter bloom;

    public ItemCacheService(Redis redis, Stats stats) {
        this.redis = redis;
        this.stats = stats;
        this.bloom = new BloomFilter(redis, "volc:bloom:item", 200_000, 6);
    }

    // ---------------- key 设计 ----------------

    public static String fullKey(long id) {
        return "volc:item:full:" + id;
    }

    public static String sumKey(long id) {
        return "volc:item:sum:" + id;
    }

    public static String nullKey(long id) {
        return "volc:item:null:" + id;
    }

    // ---------------- 主流程 ----------------

    public ItemDetail get(long id, CacheFlags f, boolean summaryOnly) {
        stats.total.incrementAndGet();

        // v=0：改造前，直连下游
        if (!f.cache()) {
            stats.backsource.incrementAndGet();
            ItemDetail v = ItemRepo.loadSerial(id);
            if (v == null) {
                stats.notFound.incrementAndGet();
            }
            return v;
        }

        String localKey = summaryOnly ? sumKey(id) : fullKey(id);

        // 1) 本地 L1
        if (f.local()) {
            if (l1.isNegative(localKey)) {
                stats.nullHit.incrementAndGet();
                stats.notFound.incrementAndGet();
                return null;
            }
            ItemDetail hit = l1.get(localKey);
            if (hit != null) {
                stats.localHit.incrementAndGet();
                return hit;
            }
        }

        // 2) Redis
        ItemDetail cached = summaryOnly ? readSummary(id) : readFull(id);
        if (cached != null) {
            stats.cacheHit.incrementAndGet();
            if (f.local()) {
                l1.put(localKey, cached);
            }
            return cached;
        }

        // 3) 空值缓存：上次已经确认不存在，直接回 404，不再打下游
        if (redis.exists(nullKey(id))) {
            stats.nullHit.incrementAndGet();
            stats.notFound.incrementAndGet();
            if (f.local()) {
                l1.putNegative(localKey);
            }
            return null;
        }

        // 4) 布隆过滤器：一定不存在的 id，连 Redis 都不用查第二遍
        // 位图没建好就跳过判断（fail-open），否则会把正常商品全部判成不存在
        if (f.bloom() && bloom.isReady() && !bloom.mightContain(id)) {
            stats.bloomReject.incrementAndGet();
            stats.notFound.incrementAndGet();
            if (f.local()) {
                l1.putNegative(localKey);
            }
            return null;
        }

        // 5) 回源（可带单飞 + 分布式锁）
        return f.singleflight() ? loadWithLock(id, f, summaryOnly) : loadFresh(id, f, summaryOnly);
    }

    // ---------------- 读 ----------------

    private ItemDetail readFull(long id) {
        String raw = redis.get(fullKey(id));
        if (raw == null) {
            return null;
        }
        try {
            return gson.fromJson(raw, ItemDetail.class);
        } catch (RuntimeException e) {
            redis.del(fullKey(id)); // 脏数据直接删掉，下次重建
            return null;
        }
    }

    private ItemDetail readSummary(long id) {
        List<String> f = redis.hmget(sumKey(id), SUM_FIELDS);
        if (f == null || f.get(0) == null) {
            return null;
        }
        ItemDetail d = new ItemDetail();
        d.id = Long.parseLong(f.get(0));
        d.title = f.get(1);
        d.price = Long.parseLong(f.get(2));
        d.stock = Integer.parseInt(f.get(3));
        d.rating = Double.parseDouble(f.get(4));
        d.reviewCount = Integer.parseInt(f.get(5));
        d.images.add(f.get(6) == null ? "" : f.get(6));
        return d;
    }

    // ---------------- 回源 ----------------

    private ItemDetail loadFresh(long id, CacheFlags f, boolean summaryOnly) {
        stats.backsource.incrementAndGet();
        stats.enterSource();
        ItemDetail v;
        try {
            v = ItemRepo.loadSerial(id);
        } finally {
            stats.exitSource();
        }
        if (v == null) {
            stats.notFound.incrementAndGet();
            redis.setex(nullKey(id), TTL_NULL, "1");
            if (f.local()) {
                l1.putNegative(summaryOnly ? sumKey(id) : fullKey(id));
            }
            return null;
        }
        writeCache(id, v, f.jitter());
        if (f.local()) {
            l1.put(summaryOnly ? sumKey(id) : fullKey(id), v);
        }
        return v;
    }

    /**
     * 单飞（in-flight 合并）+ Redis 分布式锁：
     * 同一个 JVM 内所有并发请求只放一个去回源，其余等待结果；
     * 跨实例再靠 SET NX 互斥，拿不到锁的实例短暂轮询缓存。
     */
    private ItemDetail loadWithLock(long id, CacheFlags f, boolean summaryOnly) {
        String sfKey = "sf:" + id;
        CompletableFuture<ItemDetail> mine = new CompletableFuture<>();
        CompletableFuture<ItemDetail> running = inflight.putIfAbsent(sfKey, mine);
        if (running != null) {
            stats.inflightJoin.incrementAndGet();
            try {
                return running.get(20, TimeUnit.SECONDS);
            } catch (Exception e) {
                stats.error.incrementAndGet();
                return loadFresh(id, f, summaryOnly);
            }
        }

        String lockKey = "volc:lock:item:" + id;
        String token = UUID.randomUUID().toString();
        boolean locked = false;
        try {
            locked = redis.tryLock(lockKey, token, 5000);
            if (!locked) {
                stats.lockContend.incrementAndGet();
                for (int i = 0; i < 25; i++) {
                    sleep(20);
                    ItemDetail v = summaryOnly ? readSummary(id) : readFull(id);
                    if (v != null) {
                        stats.cacheHit.incrementAndGet();
                        mine.complete(v);
                        return v;
                    }
                }
            }
            ItemDetail v = loadFresh(id, f, summaryOnly);
            mine.complete(v);
            return v;
        } catch (RuntimeException e) {
            mine.completeExceptionally(e);
            stats.error.incrementAndGet();
            throw e;
        } finally {
            inflight.remove(sfKey, mine);
            if (locked) {
                redis.unlock(lockKey, token);
            }
        }
    }

    // ---------------- 写 ----------------

    public void writeCache(long id, ItemDetail v, boolean jitter) {
        long ttl = jitter ? jittered(TTL_BASE_MS) : TTL_BASE_MS;
        redis.setPx(fullKey(id), ttl, gson.toJson(v));
        redis.hsetAll(sumKey(id), v.summary(), ttl);
        bloom.add(id);
    }

    /** TTL 抖动 ±20%：300s → 240~360s，把批量失效的尖峰摊平 */
    public static long jittered(long baseMs) {
        double factor = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4;
        return (long) Math.max(1, baseMs * factor);
    }

    // ---------------- 预热 / 运维 ----------------

    public int warm(long id, long ttlMs, boolean jitter) {
        ItemDetail v = ItemRepo.buildFast(id);
        if (v == null) {
            return 0;
        }
        long ttl = jitter ? jittered(ttlMs) : ttlMs;
        redis.setPx(fullKey(id), ttl, gson.toJson(v));
        redis.hsetAll(sumKey(id), v.summary(), ttl);
        bloom.add(id);
        return 1;
    }

    public BloomFilter bloom() {
        return bloom;
    }

    public void clearLocal() {
        l1.clear();
    }

    public int localSize() {
        return l1.size();
    }

    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
