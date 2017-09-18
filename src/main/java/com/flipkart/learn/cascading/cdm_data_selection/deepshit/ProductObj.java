package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by thejus on 13/9/17.
 */
class ProductObj implements Serializable {

    @JsonProperty(value = "productId")
    private final String productId;

    @JsonProperty(value = "timestamp")
    private final long timestamp;

    @JsonProperty(value = "date")
    private final String date;

    @JsonProperty(value = "position")
    private int position;

    @JsonProperty(value = "click")
    private final float click;

    @JsonProperty(value = "buy")
    private final float buy;

    @JsonProperty(value = "findingmethod")
    private String findingmethod;

    public ProductObj(@JsonProperty(value = "productId") String productId,
                      @JsonProperty(value = "timestamp") long timestamp,
                      @JsonProperty(value = "position") int position,
                      @JsonProperty(value = "click") float click,
                      @JsonProperty(value = "buy") float buy,
                      @JsonProperty(value = "findingmethod") String findingmethod) {
        this.productId = productId;
        this.timestamp = timestamp;
        this.date = format.format(new Date(timestamp));
        this.position = position;
        this.click = click;
        this.buy = buy;
        this.findingmethod = findingmethod;
    }

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

    @JsonIgnore
    public boolean isClick() {
        return click > 0;
    }

    @JsonIgnore
    public boolean isBought() {
        return buy > 0;
    }

    public float getBuy() {
        return buy;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "ProductObj{" +
                "productId='" + productId + '\'' +
                ", timestamp=" + timestamp +
                ", date='" + date + '\'' +
                ", position=" + position +
                ", click=" + click +
                ", buy=" + buy +
                ", findingmethod='" + findingmethod + '\'' +
                '}';
    }
}