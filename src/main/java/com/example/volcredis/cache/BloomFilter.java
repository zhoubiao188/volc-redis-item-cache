package com.example.volcredis.cache;

import com.example.volcredis.redis.Redis;

import java.util.function.LongPredicate;

/**
 * 基于 Redis Bitmap 的布隆过滤器。
 *
 * 200 万个 bit ≈ 244KB，就能挡住 20 万商品的"不存在"查询，误判率约 0.8%。
 * 它只会说两句话：一定不存在 / 可能存在 —— 所以只能挡穿透，不能当缓存用。
 */
public class BloomFilter {

    private static final String READY_KEY = "volc:bloom:ready";

    private final Redis redis;
    private final String key;
    private final long bits;
    private final int k;

    private volatile boolean ready = false;
    private volatile long lastCheck = 0L;

    public BloomFilter(Redis redis, String key, long expectedItems, int k) {
        this.redis = redis;
        this.key = key;
        this.k = k;
        // 每元素 10 bit，k=6 时误判率 ≈ (1-e^(-kn/m))^k ≈ 0.8%
        this.bits = Math.max(1024, expectedItems * 10);
    }

    // ---------- 双哈希：h1 决定起点，h2 决定步长 ----------

    private long h1(long id) {
        long x = id * 0x9E3779B97F4A7C15L;
        x ^= (x >>> 30);
        x *= 0xBF58476D1CE4E5B9L;
        x ^= (x >>> 27);
        x *= 0x94D049BB133111EBL;
        x ^= (x >>> 31);
        return x;
    }

    private long[] offsets(long id) {
        long a = h1(id);
        long b = h1(id ^ 0x5DEECE66DL) | 1L;   // 保证步长为奇数，可遍历整个位空间
        long[] out = new long[k];
        for (int i = 0; i < k; i++) {
            out[i] = Math.floorMod(a + i * b, bits);
        }
        return out;
    }

    // ---------- 写 ----------

    public void add(long id) {
        redis.addBit(key, offsets(id));
    }

    /**
     * 全量重建：本地拼出 244KB 位图，一条 SET 写进 Redis。
     * 20 万商品重建一次只要几百毫秒，适合放在发布流程里跑。
     */
    public void rebuild(long from, long to, LongPredicate exists) {
        byte[] bitmap = new byte[(int) ((bits + 7) / 8)];
        int count = 0;
        for (long id = from; id <= to; id++) {
            if (!exists.test(id)) {
                continue;
            }
            for (long off : offsets(id)) {
                bitmap[(int) (off >> 3)] |= (byte) (1 << (7 - (off & 7)));
            }
            count++;
        }
        redis.setBytes(key, bitmap, 86_400);
        redis.setex(READY_KEY, 86_400, String.valueOf(count));
        this.ready = true;
        this.lastCheck = System.currentTimeMillis();
    }

    // ---------- 读 ----------

    public boolean mightContain(long id) {
        return redis.countSetBits(key, offsets(id)) == k;
    }

    /**
     * 位图是否已建好。没建好时必须"放行"（fail-open），
     * 否则空位图会把所有正常商品判成不存在 —— 这是上线最容易踩的一脚。
     */
    public boolean isReady() {
        long now = System.currentTimeMillis();
        if (now - lastCheck > 2000) {
            ready = redis.exists(READY_KEY);
            lastCheck = now;
        }
        return ready;
    }
}
