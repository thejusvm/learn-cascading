package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class RequestContext {

    @JsonProperty(value = "searchQuery")
    private final String searchQuery;

    @JsonProperty(value = "storePath")
    private final String storePath;

    @JsonCreator
    public RequestContext(@JsonProperty(value = "searchQuery") String searchQuery,
                          @JsonProperty(value = "storePath") String storePath) {
        this.searchQuery = searchQuery;
        this.storePath = storePath;
    }

    public String getSearchQuery() {
        return searchQuery;
    }

    public String getStorePath() {
        return storePath;
    }
}
