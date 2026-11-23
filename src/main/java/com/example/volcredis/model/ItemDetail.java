package com.example.volcredis.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 商品详情聚合结果。字段与线上真实接口保持一致：
 * 库存来自库存服务、价格来自价格中心、评论来自 UGC、推荐来自召回服务。
 */
public class ItemDetail {

    public long id;
    public String title;
    public String subTitle;
    public String brand;
    public String category;
    public long price;          // 分
    public long originPrice;    // 分
    public int stock;
    public long soldCount;
    public double rating;
    public int reviewCount;
    public List<Long> recommendIds = new ArrayList<>();
    public List<String> images = new ArrayList<>();
    public Map<String, String> specs = new LinkedHashMap<>();
    /** 图文详情 HTML，单个商品可达 1MB 以上，是最典型的大 Key 来源 */
    public String detailHtml;

    public Map<String, String> summary() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("id", String.valueOf(id));
        m.put("title", title);
        m.put("price", String.valueOf(price));
        m.put("stock", String.valueOf(stock));
        m.put("rating", String.valueOf(rating));
        m.put("reviewCount", String.valueOf(reviewCount));
        m.put("cover", images.isEmpty() ? "" : images.get(0));
        return m;
    }
}
