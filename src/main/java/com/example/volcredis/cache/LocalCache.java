package com.example.volcredis.cache;

import com.example.volcredis.model.ItemDetail;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 进程内 L1 缓存：LRU + 短 TTL。
 * 热 Key 场景下把 Redis 的读流量挡在进程里，500ms TTL 换取"最多 500ms 不一致"。
 * 生产环境若不能接受，可换成 Redis Pub/Sub 广播失效（代码见 README）。
 */
public class LocalCache {

    private static final Object NEGATIVE = new Object();

    private record Node(Object value, long expireAt) {
    }

    private final int maxSize;
    private final long ttlMs;
    private final Map<String, Node> map;

    public LocalCache(int maxSize, long ttlMs) {
        this.maxSize = maxSize;
        this.ttlMs = ttlMs;
        final int cap = maxSize;
        this.map = new LinkedHashMap<>(1024, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(java.util.Map.Entry<String, Node> eldest) {
                return size() > cap;
            }
        };
    }

    public synchronized ItemDetail get(String key) {
        Node e = map.get(key);
        if (e == null || e.expireAt() < System.currentTimeMillis()) {
            return null;
        }
        return e.value() == NEGATIVE ? null : (ItemDetail) e.value();
    }

    public synchronized boolean isNegative(String key) {
        Node e = map.get(key);
        return e != null && e.expireAt() >= System.currentTimeMillis() && e.value() == NEGATIVE;
    }

    public synchronized void put(String key, ItemDetail value) {
        map.put(key, new Node(value, System.currentTimeMillis() + ttlMs));
    }

    public synchronized void putNegative(String key) {
        map.put(key, new Node(NEGATIVE, System.currentTimeMillis() + ttlMs));
    }

    public synchronized void clear() {
        map.clear();
    }

    public synchronized int size() {
        return map.size();
    }
}
