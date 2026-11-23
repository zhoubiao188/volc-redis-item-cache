import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 零依赖压测器。编译运行：java bench/LoadGen.java --help
 *
 * 两种模式：
 *   load  —— N 个线程持续打 D 秒，报 QPS / P50 / P95 / P99
 *   burst —— N 个请求同时发出（用 CountDownLatch 对齐起跑），专门测"缓存失效瞬间"
 */
public class LoadGen {

    public static void main(String[] args) throws Exception {
        String host = "http://127.0.0.1:18090";
        String path = "/api/item/%d";
        int threads = 50;
        int seconds = 10;
        String mode = "load";
        String ids = "1-200000";
        boolean seq = false;

        for (String a : args) {
            int i = a.indexOf('=');
            if (i < 0) {
                if ("--seq".equals(a)) {
                    seq = true;
                }
                continue;
            }
            String k = a.substring(0, i);
            String v = a.substring(i + 1);
            switch (k) {
                case "--host" -> host = v;
                case "--path" -> path = v;
                case "--threads" -> threads = Integer.parseInt(v);
                case "--seconds" -> seconds = Integer.parseInt(v);
                case "--mode" -> mode = v;
                case "--ids" -> ids = v;
                default -> {
                }
            }
        }

        long idLo;
        long idHi;
        if (ids.contains("-")) {
            String[] p = ids.split("-");
            idLo = Long.parseLong(p[0]);
            idHi = Long.parseLong(p[1]);
        } else {
            idLo = Long.parseLong(ids);
            idHi = idLo;
        }

        java.util.Map<String, Long> before = fetchStats(host);
        List<Long> lat = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errors = new AtomicInteger();
        AtomicLong bytes = new AtomicLong();
        AtomicLong count = new AtomicLong();

        final String fmode = mode;
        final boolean fseq = seq;
        final String fhost = host;
        final String fpath = path;
        long deadline = System.currentTimeMillis() + seconds * 1000L;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        long wallStart = System.nanoTime();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            new Thread(() -> {
                try {
                    start.await();
                    if ("burst".equals(fmode)) {
                        long id = pick(idLo, idHi, tid, fseq);
                        call(fhost, fpath, id, lat, errors, bytes, count);
                    } else {
                        while (System.currentTimeMillis() < deadline) {
                            long id = pick(idLo, idHi, tid, fseq);
                            call(fhost, fpath, id, lat, errors, bytes, count);
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "lg-" + t).start();
        }

        final long sampleStart = System.nanoTime();
        final java.util.concurrent.atomic.AtomicBoolean sampling = new java.util.concurrent.atomic.AtomicBoolean(true);
        final java.util.concurrent.atomic.AtomicLong peakAll = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicLong peak1s = new java.util.concurrent.atomic.AtomicLong();
        Thread mon = new Thread(() -> {
            while (sampling.get()) {
                long el = (System.nanoTime() - sampleStart) / 1_000_000;
                long cur = fetchStats(fhost).getOrDefault("curSource", 0L);
                peakAll.accumulateAndGet(cur, Math::max);
                if (el <= 1000) {
                    peak1s.accumulateAndGet(cur, Math::max);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "monitor");
        mon.setDaemon(true);
        mon.start();

        start.countDown();
        done.await();
        sampling.set(false);
        mon.join(300);
        double wallSec = (System.nanoTime() - wallStart) / 1e9;

        List<Long> sorted = new ArrayList<>(lat);
        Collections.sort(sorted);
        java.util.Map<String, Long> after = fetchStats(host);

        // QPS = 完成数 / 实际总耗时（含收尾排空），与利特尔法则一致
        double win = wallSec;
        System.out.printf("mode=%s threads=%d wall=%.2fs win=%.2fs n=%d qps=%.1f p50=%dms p95=%dms p99=%dms max=%dms avgBytes=%d err=%d%n",
                mode, threads, wallSec, win, count.get(), count.get() / win,
                pct(sorted, 50), pct(sorted, 95), pct(sorted, 99),
                sorted.isEmpty() ? 0 : sorted.get(sorted.size() - 1),
                count.get() == 0 ? 0 : bytes.get() / count.get(),
                errors.get());
        System.out.printf("peakSrc(all)=%d peakSrc(first1s)=%d  ", peakAll.get(), peak1s.get());
        System.out.printf("delta: backsource=%d cacheHit=%d localHit=%d nullHit=%d bloomReject=%d inflightJoin=%d lockContend=%d notFound=%d%n",
                after.getOrDefault("backsource", 0L) - before.getOrDefault("backsource", 0L),
                after.getOrDefault("cacheHit", 0L) - before.getOrDefault("cacheHit", 0L),
                after.getOrDefault("localHit", 0L) - before.getOrDefault("localHit", 0L),
                after.getOrDefault("nullHit", 0L) - before.getOrDefault("nullHit", 0L),
                after.getOrDefault("bloomReject", 0L) - before.getOrDefault("bloomReject", 0L),
                after.getOrDefault("inflightJoin", 0L) - before.getOrDefault("inflightJoin", 0L),
                after.getOrDefault("lockContend", 0L) - before.getOrDefault("lockContend", 0L),
                after.getOrDefault("notFound", 0L) - before.getOrDefault("notFound", 0L));
    }

    private static long pick(long lo, long hi, int tid, boolean seq) {
        if (lo == hi) {
            return lo;
        }
        if (seq) {
            return lo + (tid % (hi - lo + 1));
        }
        return lo + (long) (Math.random() * (hi - lo + 1));
    }

    private static void call(String host, String path, long id, List<Long> lat,
                             AtomicInteger errors, AtomicLong bytes, AtomicLong count) {
        long t0 = System.nanoTime();
        HttpURLConnection c = null;
        try {
            URL u = new URL(host + String.format(path, id));
            c = (HttpURLConnection) u.openConnection();
            c.setConnectTimeout(5000);
            c.setReadTimeout(30000);
            c.setRequestMethod("GET");
            int code = c.getResponseCode();
            long n = 0;
            try (BufferedReader r = new BufferedReader(new InputStreamReader(
                    code >= 400 ? c.getErrorStream() : c.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                    while ((line = r.readLine()) != null) {
                        n += line.length();
                    }
            }
            bytes.addAndGet(n);
            if (code >= 500) {
                errors.incrementAndGet();
            }
            count.incrementAndGet();
        } catch (Exception e) {
            errors.incrementAndGet();
        } finally {
            if (c != null) {
                c.disconnect();
            }
            lat.add((System.nanoTime() - t0) / 1_000_000);
        }
    }

    private static long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int idx = (int) Math.ceil(sorted.size() * p / 100.0) - 1;
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, idx)));
    }

    private static java.util.Map<String, Long> fetchStats(String host) {
        java.util.Map<String, Long> m = new java.util.HashMap<>();
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(host + "/admin/stats").openConnection();
            c.setConnectTimeout(3000);
            c.setReadTimeout(3000);
            StringBuilder sb = new StringBuilder();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    sb.append(line);
                }
            }
            Matcher mt = Pattern.compile("\"(\\w+)\":(\\d+)").matcher(sb);
            while (mt.find()) {
                m.put(mt.group(1), Long.parseLong(mt.group(2)));
            }
        } catch (Exception ignored) {
        }
        return m;
    }
}
