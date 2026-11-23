package com.example.volcredis.web;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 只有压测需要的几个计数器：谁命中了、谁回源了、回源了几次。
 * 真实项目里这些数字应该上报到 TLS（日志服务）或 Prometheus。
 */
public class Stats {

    public final AtomicLong total = new AtomicLong();
    public final AtomicLong cacheHit = new AtomicLong();
    public final AtomicLong localHit = new AtomicLong();
    public final AtomicLong nullHit = new AtomicLong();
    public final AtomicLong backsource = new AtomicLong();
    public final AtomicLong bloomReject = new AtomicLong();
    public final AtomicLong notFound = new AtomicLong();
    public final AtomicLong inflightJoin = new AtomicLong();
    public final AtomicLong lockContend = new AtomicLong();
    public final AtomicLong error = new AtomicLong();

    /** 源站瞬时并发：雪崩场景看的不是总回源量，而是同一瞬间打过去多少 */
    private final java.util.concurrent.atomic.AtomicInteger curSource = new java.util.concurrent.atomic.AtomicInteger();
    public final java.util.concurrent.atomic.AtomicInteger maxSource = new java.util.concurrent.atomic.AtomicInteger();

    public void enterSource() {
        maxSource.accumulateAndGet(curSource.incrementAndGet(), Math::max);
    }

    public void exitSource() {
        curSource.decrementAndGet();
    }

    private final long[] latencies = new long[100_000];
    private int cursor = 0;

    public void recordLatency(long ms) {
        synchronized (latencies) {
            latencies[cursor++ % latencies.length] = ms;
        }
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("total", total.get());
        m.put("cacheHit", cacheHit.get());
        m.put("localHit", localHit.get());
        m.put("nullHit", nullHit.get());
        m.put("backsource", backsource.get());
        m.put("bloomReject", bloomReject.get());
        m.put("notFound", notFound.get());
        m.put("inflightJoin", inflightJoin.get());
        m.put("lockContend", lockContend.get());
        m.put("error", error.get());
        m.put("maxConcurrentSource", maxSource.get());
        m.put("curSource", curSource.get());
        m.put("hitRate", total.get() == 0 ? 0.0
                : Math.round((cacheHit.get() + localHit.get() + nullHit.get()) * 10000.0 / total.get()) / 100.0);
        return m;
    }

    public void reset() {
        total.set(0);
        cacheHit.set(0);
        localHit.set(0);
        nullHit.set(0);
        backsource.set(0);
        bloomReject.set(0);
        notFound.set(0);
        inflightJoin.set(0);
        lockContend.set(0);
        error.set(0);
        maxSource.set(0);
        synchronized (latencies) {
            cursor = 0;
            java.util.Arrays.fill(latencies, 0L);
        }
    }
}
