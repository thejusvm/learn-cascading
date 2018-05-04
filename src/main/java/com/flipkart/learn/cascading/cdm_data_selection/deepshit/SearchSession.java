package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by thejus on 13/9/17.
 */
public class SearchSession implements Serializable {

    @JsonProperty(value = "sqid")
    private String sqid;
    @JsonProperty(value = "requestContext")
    private RequestContext requestContext;
    @JsonProperty(value = "timestamp")
    private long timestamp;
    @JsonProperty(value = "date")
    private String date;
    @JsonProperty(value = "products")
    private List<ProductObj> products;

    public SearchSession clone() {
        return new SearchSession(sqid, requestContext, timestamp, date, ImmutableList.copyOf(products));
    }

    @JsonCreator
    public SearchSession(@JsonProperty(value = "sqid") String sqid,
                         @JsonProperty(value = "requestContext") RequestContext requestContext,
                         @JsonProperty(value = "timestamp") long timestamp,
                         @JsonProperty(value = "date") String date,
                         @JsonProperty(value = "products") List<ProductObj> products) {
        this.sqid = sqid;
        this.requestContext = requestContext;
        this.timestamp = timestamp;
        this.date = date;
        this.products = products;
    }

    private static SimpleDateFormat format = new SimpleDateFormat("dd/MM/YY HH:mm:ss.SSSZ");

    public SearchSession(String sqid, RequestContext requestContext, long timestamp) {
        this.sqid = sqid;
        this.requestContext = requestContext;
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

    public RequestContext getRequestContext() {
        return requestContext;
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

    @JsonIgnore
    public List<ProductObj> getClickedProduct() {
        return products
                .stream()
                .filter(ProductObj::isClick)
                .collect(Collectors.toList());
    }

    @JsonIgnore
    public void filterProducts(Predicate<ProductObj> predicate) {
        products = products.stream().filter(predicate).collect(Collectors.toList());
    }

    @JsonIgnore
    public List<ProductObj> getBoughtProducts() {
        return products
                .stream()
                .filter(ProductObj::isBought)
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "SearchSession{" +
                "sqid='" + sqid + '\'' +
                ", timestamp=" + timestamp +
                ", date='" + date + '\'' +
                ", products=" + products +
                '}';
    }
}
