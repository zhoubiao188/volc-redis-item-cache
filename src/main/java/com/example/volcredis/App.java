package com.example.volcredis;

import com.example.volcredis.cache.ItemCacheService;
import com.example.volcredis.redis.Redis;
import com.example.volcredis.web.ApiHandler;
import com.example.volcredis.web.Stats;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * 启动方式：
 *   java -jar volc-redis-item-cache.jar --port=18090 --redis-host=127.0.0.1 --redis-port=6379
 */
public class App {

    public static void main(String[] args) throws Exception {
        int port = intArg(args, "--port", 18090);
        String host = strArg(args, "--redis-host", "127.0.0.1");
        int redisPort = intArg(args, "--redis-port", 6379);
        String password = strArg(args, "--redis-password", null);

        Redis redis = password == null || password.isEmpty()
                ? new Redis(host, redisPort)
                : new Redis(host, redisPort, password);

        Stats stats = new Stats();
        ItemCacheService svc = new ItemCacheService(redis, stats);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 512);
        server.createContext("/", new ApiHandler(svc, redis, stats));
        server.setExecutor(Executors.newFixedThreadPool(400));
        server.start();

        System.out.printf("[volc-redis-item-cache] up on :%d  redis=%s:%d  ping=%s%n",
                port, host, redisPort, redis.ping());
    }

    private static int intArg(String[] args, String key, int def) {
        String v = strArg(args, key, null);
        return v == null ? def : Integer.parseInt(v);
    }

    private static String strArg(String[] args, String key, String def) {
        for (String a : args) {
            if (a.startsWith(key + "=")) {
                return a.substring(key.length() + 1);
            }
            if (a.equals(key)) {
                return "true";
            }
        }
        return def;
    }
}
