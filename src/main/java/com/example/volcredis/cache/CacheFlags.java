package com.example.volcredis.cache;

/**
 * 一档一档往上加的保护措施，方便压测时逐级开关，也方便你回顾"每一步到底解决了什么"。
 */
public record CacheFlags(boolean cache, boolean bloom, boolean singleflight, boolean jitter, boolean local) {

    public static CacheFlags of(int v) {
        return switch (v) {
            case 0 -> new CacheFlags(false, false, false, false, false); // 裸奔：直连下游
            case 1 -> new CacheFlags(true, false, false, false, false);  // 只加 cache-aside
            case 2 -> new CacheFlags(true, true, false, false, false);   // + 布隆 + 空值缓存（治穿透）
            case 3 -> new CacheFlags(true, true, true, false, false);    // + 单飞 + 分布式锁（治击穿）
            case 4 -> new CacheFlags(true, true, true, true, false);     // + TTL 抖动（治雪崩）
            default -> new CacheFlags(true, true, true, true, true);     // + 本地 L1（治热 Key）
        };
    }
}
