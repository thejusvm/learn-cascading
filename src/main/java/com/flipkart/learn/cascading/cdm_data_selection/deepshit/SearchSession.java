package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by thejus on 13/9/17.
 */
public class SearchSession implements Serializable {

    private String sqid;
    private long timestamp;
    private String date;
    private List<ProductObj> products;

    private static SimpleDateFormat format = new SimpleDateFormat("dd/MM/YY HH:mm:ss.SSSZ");

    public SearchSession(String sqid, long timestamp) {
        this.sqid = sqid;
        this.timestamp = timestamp;
        this.date = format.format(new Date(timestamp));
        products = new ArrayList<>();
    }


    public void add(ProductObj product) {
        products.add(product);
    }

    public String getSqid() {
        return sqid;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getDate() {
        return date;
    }

    public List<ProductObj> getProducts() {
        return products;
    }

    public int numClicks() {
        int clicks = 0;
        for (ProductObj product : products) {
            clicks += product.getClick();
        }
        return clicks;
    }

    public int numBuys() {
        int buys = 0;
        for (ProductObj product : products) {
            buys += product.getBuy();
        }
        return buys;
    }

    public int numImpressions() {
        return products.size();
    }
}
