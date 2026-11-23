package com.example.volcredis.redis;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

import java.util.List;

/**
 * 火山引擎「缓存数据库 Redis 版」的连接封装。
 * 线上接入只需要换掉 host / port / password 三件事：
 *
 *    JedisPooled pool = new JedisPooled(
 *        new HostAndPort("redis-xxxx.redis-cn-beijing.volces.com", 6379),
 *        DefaultJedisClientConfig.builder().password("***").ssl(true).build());
 *
 * 其余命令与开源 Redis 6.0 完全一致。
 */
public class Redis {

    private final JedisPooled pool;

    public Redis(String host, int port) {
        this.pool = new JedisPooled(host, port);
    }

    /** 火山引擎 Redis 版实例默认开启密码 + 可选 TLS，走这个构造器 */
    public Redis(String host, int port, String password) {
        this.pool = new JedisPooled(
                new redis.clients.jedis.HostAndPort(host, port),
                redis.clients.jedis.DefaultJedisClientConfig.builder()
                        .password(password)
                        .connectionTimeoutMillis(2000)
                        .socketTimeoutMillis(2000)
                        .build());
    }

    public String ping() {
        return pool.ping();
    }

    // ---------- String ----------

    public String get(String key) {
        return pool.get(key);
    }

    public void setex(String key, int seconds, String value) {
        pool.setex(key, seconds, value);
    }

    /** 毫秒级过期，TTL 抖动要精确到毫秒才有意义（5s ±20% 不能被取整抹平） */
    public void setPx(String key, long ttlMs, String value) {
        pool.set(key, value, SetParams.setParams().px(ttlMs));
    }

    public void del(String... keys) {
        pool.del(keys);
    }

    public boolean exists(String key) {
        return pool.exists(key);
    }

    // ---------- 分布式锁 ----------

    /** SET key token NX PX ttl —— 一条命令完成"互斥 + 防死锁" */
    public boolean tryLock(String key, String token, int ttlMs) {
        String r = pool.set(key, token, SetParams.setParams().nx().px(ttlMs));
        return "OK".equals(r);
    }

    /** 释放锁必须校验 token，否则会删掉别人的锁 */
    public boolean unlock(String key, String token) {
        Object r = pool.eval(
                "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
                List.of(key), List.of(token));
        return Long.valueOf(1L).equals(r);
    }

    // ---------- Hash（分片存大 Key） ----------

    public void hsetAll(String key, java.util.Map<String, String> map, long ttlMs) {
        pool.hset(key, map);
        pool.pexpire(key, ttlMs);
    }

    public List<String> hmget(String key, String... fields) {
        return pool.hmget(key, fields);
    }

    // ---------- Bitmap（布隆过滤器） ----------

    public void addBit(String key, long[] offsets) {
        for (long off : offsets) {
            pool.setbit(key, off, true);
        }
    }

    /**
     * 一次 RTT 查 k 个 bit：把 6 次 GETBIT 塞进一个 Lua 脚本，
     * 避免"布隆过滤器比缓存本身还慢"的尴尬。
     */
    private static final String BLOOM_SCRIPT =
            "local c=0 "
          + "for i=1,#ARGV do if redis.call('GETBIT', KEYS[1], ARGV[i]) == 1 then c=c+1 end end "
          + "return c";

    public long countSetBits(String key, long[] offsets) {
        List<String> args = new java.util.ArrayList<>(offsets.length);
        for (long off : offsets) {
            args.add(String.valueOf(off));
        }
        Object r = pool.eval(BLOOM_SCRIPT, List.of(key), args);
        return ((Number) r).longValue();
    }

    /** 整张位图一次写入：本地拼 byte[]，SET 一条命令搞定，避免大批量 pipeline */
    public void setBytes(String key, byte[] value, int ttlSeconds) {
        byte[] k = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        pool.set(k, value);
        if (ttlSeconds > 0) {
            pool.expire(k, ttlSeconds);
        }
    }

    // ---------- 运维 ----------

    public long dbsize() {
        return pool.dbSize();
    }

    public void flushAll() {
        pool.flushAll();
    }

    public void close() {
        pool.close();
    }
}
