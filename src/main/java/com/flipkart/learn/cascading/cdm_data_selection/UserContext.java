package com.flipkart.learn.cascading.cdm_data_selection;

import java.io.Serializable;

/**
 * Created by thejus on 13/9/17.
 */
class UserContext implements Serializable {

    private String accountId;
    private String deviceId;
    private String platform;
    private SearchSessions products;

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

    public void addProduct(String sqid, ProductObj product) {
        products.add(sqid, product);

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
