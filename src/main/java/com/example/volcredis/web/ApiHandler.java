package com.example.volcredis.web;

import com.example.volcredis.cache.CacheFlags;
import com.example.volcredis.cache.ItemCacheService;
import com.example.volcredis.model.ItemDetail;
import com.example.volcredis.redis.Redis;
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ApiHandler implements HttpHandler {

    private final Gson gson = new Gson();
    private final ItemCacheService svc;
    private final Redis redis;
    private final Stats stats;

    public ApiHandler(ItemCacheService svc, Redis redis, Stats stats) {
        this.svc = svc;
        this.redis = redis;
        this.stats = stats;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        long t0 = System.nanoTime();
        int code = 200;
        String body;
        try {
            String path = ex.getRequestURI().getPath();
            Map<String, String> q = query(ex.getRequestURI().getRawQuery());

            if (path.equals("/health")) {
                body = "{\"status\":\"UP\",\"redis\":\"" + redis.ping() + "\"}";
            } else if (path.equals("/admin/stats")) {
                body = gson.toJson(stats.snapshot());
            } else if (path.equals("/admin/reset")) {
                stats.reset();
                body = "{\"ok\":true}";
            } else if (path.equals("/admin/flush")) {
                redis.flushAll();
                svc.clearLocal();
                stats.reset();
                body = "{\"ok\":true}";
            } else if (path.equals("/admin/bloom/rebuild")) {
                long from = Long.parseLong(q.getOrDefault("from", "1"));
                long to = Long.parseLong(q.getOrDefault("to", "200000"));
                long t = System.currentTimeMillis();
                svc.bloom().rebuild(from, to, com.example.volcredis.source.ItemRepo::exists);
                body = "{\"ok\":true,\"from\":" + from + ",\"to\":" + to
                        + ",\"costMs\":" + (System.currentTimeMillis() - t) + "}";
            } else if (path.equals("/admin/preload")) {
                body = preload(q);
            } else if (path.equals("/admin/localcache")) {
                body = "{\"size\":" + svc.localSize() + "}";
            } else if (path.startsWith("/api/item/")) {
                Object[] r = item(path, q);
                code = (Integer) r[0];
                body = (String) r[1];
            } else {
                code = 404;
                body = "{\"error\":\"no_route\"}";
            }
        } catch (Exception e) {
            code = 500;
            stats.error.incrementAndGet();
            body = "{\"error\":\"" + e.getClass().getSimpleName() + "\",\"msg\":\"" + String.valueOf(e.getMessage()).replace("\"", "'") + "\"}";
        }
        stats.recordLatency((System.nanoTime() - t0) / 1_000_000);
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private Object[] item(String path, Map<String, String> q) {
        String rest = path.substring("/api/item/".length());
        boolean summaryOnly = rest.endsWith("/summary");
        if (summaryOnly) {
            rest = rest.substring(0, rest.length() - "/summary".length());
        }
        if (q.getOrDefault("view", "").equals("summary")) {
            summaryOnly = true;
        }
        long id;
        try {
            id = Long.parseLong(rest);
        } catch (NumberFormatException e) {
            return new Object[]{400, "{\"error\":\"bad_id\"}"};
        }
        int v = Integer.parseInt(q.getOrDefault("v", "5"));
        ItemDetail d = svc.get(id, CacheFlags.of(v), summaryOnly);
        if (d == null) {
            return new Object[]{404, "{\"error\":\"item_not_found\",\"id\":" + id + "}"};
        }
        return new Object[]{200, gson.toJson(summaryOnly ? d.summary() : d)};
    }

    private String preload(Map<String, String> q) throws Exception {
        long start = Long.parseLong(q.getOrDefault("start", "1"));
        int n = Integer.parseInt(q.getOrDefault("n", "400"));
        long ttl = Long.parseLong(q.getOrDefault("ttl", "300000")); // 毫秒
        boolean jitter = "1".equals(q.getOrDefault("jitter", "0"));
        ExecutorService pool = Executors.newFixedThreadPool(32);
        List<Callable<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            long id = start + i;
            tasks.add(() -> svc.warm(id, ttl, jitter));
        }
        int ok = 0;
        for (Future<Integer> f : pool.invokeAll(tasks)) {
            ok += f.get();
        }
        pool.shutdown();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("warmed", ok);
        m.put("ttl", ttl);
        m.put("jitter", jitter);
        return gson.toJson(m);
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> m = new HashMap<>();
        if (raw == null || raw.isEmpty()) {
            return m;
        }
        for (String kv : raw.split("&")) {
            int i = kv.indexOf('=');
            if (i > 0) {
                m.put(URLDecoder.decode(kv.substring(0, i), StandardCharsets.UTF_8),
                        URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
            }
        }
        return m;
    }
}
