package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;

/**
 * Created by thejus on 13/9/17.
 */
class UserContext implements Serializable {

    @JsonProperty(value = "accountId")
    private String accountId;

    @JsonProperty(value = "deviceId")
    private String deviceId;

    @JsonProperty(value = "platform")
    private String platform;

    @JsonProperty(value = "products")
    private SearchSessions products;

    @JsonCreator
    public UserContext(@JsonProperty(value = "accountId") String accountId,
                       @JsonProperty(value = "deviceId") String deviceId,
                       @JsonProperty(value = "platform") String platform,
                       @JsonProperty(value = "products") SearchSessions products) {
        this.accountId = accountId;
        this.deviceId = deviceId;
        this.platform = platform;
        this.products = products;
    }

    public UserContext() {
        products = new SearchSessions();
    }

    public void setDeviceId(String deviceId) {
        if (this.deviceId == null)
            this.deviceId = deviceId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setPlatform(String platform) {
        if (this.platform == null)
            this.platform = platform;
    }

    public void addToSession(String sqid, RequestContext requestContext, ProductObj product) {
        products.add(sqid, requestContext, product);

    }

    public String getPlatform() {
        return platform;
    }

    public SearchSessions getProducts() {
        return products;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        if (this.accountId == null)
            this.accountId = accountId;
    }

    public void setProducts(SearchSessions products) {
        this.products = products;
    }

    @Override
    public String toString() {
        return "UserContext{" +
                "accountId='" + accountId + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", platform='" + platform + '\'' +
                ", products=" + products +
                ", accountId='" + accountId + '\'' +
                '}';
    }
}
