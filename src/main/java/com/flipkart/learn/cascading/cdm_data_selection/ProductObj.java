package com.flipkart.learn.cascading.cdm_data_selection;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by thejus on 13/9/17.
 */
class ProductObj implements Serializable {

    private final String productId;

    private final long timestamp;

    private final String date;

    private final float click;

    private final float buy;

    private String findingmethod;

    private static SimpleDateFormat format = new SimpleDateFormat("dd/MM/YY HH:mm:ss.SSSZ");

    public String getDate() {
        return date;
    }

    public String getFindingmethod() {
        return findingmethod;
    }

    public String getProductId() {
        return productId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public float getClick() {
        return click;
    }

    public float getBuy() {
        return buy;
    }

    public ProductObj(String productId, long timestamp, float click, float buy, String findingmethod) {
        this.productId = productId;
        this.timestamp = timestamp;
        this.date = format.format(new Date(timestamp));
        this.click = click;
        this.buy = buy;
        this.findingmethod = findingmethod;
    }


    @Override
    public String toString() {
        return "ProductObj{" +
                "productId='" + productId + '\'' +
                ", timestamp=" + timestamp +
                ", date=" + date +
                ", click=" + click +
                ", buy=" + buy +
                ", findingmethod='" + findingmethod + '\'' +
                '}';
    }
}
