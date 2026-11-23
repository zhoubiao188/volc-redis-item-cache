package com.example.volcredis.source;

import com.example.volcredis.model.ItemDetail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 模拟"下游四个服务"的慢实现：这是改造前接口要 8.6 秒的根因。
 * 换成真实代码，这里就是 4 个 HTTP / RPC 调用。
 */
public final class ItemRepo {

    public static final int LAT_STOCK  = 1400;
    public static final int LAT_PRICE  = 2600;
    public static final int LAT_REVIEW = 1900;
    public static final int LAT_RECO   = 2700;

    private static final String FILLER = "A".repeat(1_000_000);

    private ItemRepo() {
    }

    /** 商品是否存在：1..200000 且不是 5 的倍数（模拟 20% 下架商品） */
    public static boolean exists(long id) {
        return id >= 1 && id <= 200_000 && id % 5 != 0;
    }

    /** 大 Key 商品：id 为 97 的倍数，详情 HTML 约 1MB */
    public static boolean isBigItem(long id) {
        return id % 97 == 0;
    }

    /**
     * 串行聚合四个下游 —— 1400 + 2600 + 1900 + 2700 = 8600ms。
     * 这就是第一步什么都不做时接口的真实耗时。
     */
    public static ItemDetail loadSerial(long id) {
        if (!exists(id)) {
            return null;   // 源站查无此商品，下游直接返回空
        }
        sleep(LAT_STOCK);
        int stock = (int) (id % 500) + 3;
        sleep(LAT_PRICE);
        long price = 9900 + (id % 700) * 100;
        sleep(LAT_REVIEW);
        int reviews = (int) (id % 3000);
        sleep(LAT_RECO);
        List<Long> reco = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            reco.add(id + i + 1);
        }
        return build(id, stock, price, reviews, reco);
    }

    /**
     * 缓存预热走这条路：不走下游 RPC，直接从数据库 / 离线宽表构建，几毫秒一个。
     * 真实项目里这就是预热点检任务读 DB 的行为。
     */
    public static ItemDetail buildFast(long id) {
        if (!exists(id)) {
            return null;
        }
        List<Long> reco = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            reco.add(id + i + 1);
        }
        return build(id, (int) (id % 500) + 3, 9900 + (id % 700) * 100, (int) (id % 3000), reco);
    }

    private static ItemDetail build(long id, int stock, long price, int reviews, List<Long> reco) {
        ItemDetail d = new ItemDetail();
        d.id = id;
        d.title = "云原生实战季 · 商品 " + id;
        d.subTitle = "限时秒杀 · 库存实时同步";
        d.brand = "VolcDemo";
        d.category = (id % 3 == 0) ? "数码" : "家居";
        d.price = price;
        d.originPrice = price + 3000;
        d.stock = stock;
        d.soldCount = id * 7 % 100000;
        d.rating = 4.5 + (id % 5) / 10.0;
        d.reviewCount = reviews;
        d.recommendIds = reco;
        for (int i = 0; i < 5; i++) {
            d.images.add("https://img.example.com/item/" + id + "/" + i + ".jpg");
        }
        d.specs.put("color", "深空灰");
        d.specs.put("size", "L");
        d.specs.put("warranty", "12 个月");
        d.detailHtml = isBigItem(id)
                ? "<div class=\"detail\">" + FILLER + "</div>"
                : "<div class=\"detail\">商品 " + id + " 图文详情</div>";
        // 娱乐一下随机性，避免 JIT 把整个方法当作纯函数优化掉
        if (ThreadLocalRandom.current().nextInt(1_000_000) == -1) {
            d.soldCount++;
        }
        return d;
    }

    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
