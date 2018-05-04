package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class RequestContext {

    @JsonProperty(value = "searchQuery")
    private final String searchQuery;

    @JsonProperty(value = "storePath")
    private final String storePath;

    @JsonProperty(value = "filtersApplied")
    private final String filtersApplied;

    @JsonProperty(value = "sortBy")
    private final String sortBy;

    @JsonProperty(value = "pincode")
    private final int pincode;

    @JsonCreator
    public RequestContext(@JsonProperty(value = "searchQuery") String searchQuery,
                          @JsonProperty(value = "storePath") String storePath,
                          @JsonProperty(value = "filtersApplied") String filtersApplied,
                          @JsonProperty(value = "sortBy") String sortBy,
                          @JsonProperty(value = "pincode") int pincode) {
        this.searchQuery = searchQuery;
        this.storePath = storePath;
        this.filtersApplied = filtersApplied;
        this.sortBy = sortBy;
        this.pincode = pincode;
    }

    public String getSearchQuery() {
        return searchQuery;
    }

    public String getStorePath() {
        return storePath;
    }

    public String getFiltersApplied() {
        return filtersApplied;
    }

    public String getSortBy() {
        return sortBy;
    }

    public int getPincode() {
        return pincode;
    }
}
