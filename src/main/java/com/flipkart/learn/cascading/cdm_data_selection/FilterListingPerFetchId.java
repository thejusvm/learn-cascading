package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Created by shubhranshu.shekhar on 11/07/17.
 */

public class FilterListingPerFetchId extends BaseOperation<FilterListingPerFetchId.Context> implements cascading.operation.Aggregator<FilterListingPerFetchId.Context>, Serializable {

    public static class Context {
        public NavigableMap<Integer, TupleEntry> retainedTuplesInGroup = new TreeMap<>();

    }
    // Output will be groupFields and aggregatedFields :)
    public FilterListingPerFetchId(Fields aggregatedFields) {
        super(aggregatedFields);
    }

    public void start(FlowProcess flowProcess, AggregatorCall<FilterListingPerFetchId.Context> aggregatorCall) {
        aggregatorCall.setContext(new Context());
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall<FilterListingPerFetchId.Context> aggregatorCall) {
        aggregatorCall.getContext().retainedTuplesInGroup.put(
                aggregatorCall.getArguments().getInteger(DataFields._PRODUCTPAGELISTINGINDEX),
                aggregatorCall.getArguments());
    }

    public void complete(FlowProcess flowProcess, AggregatorCall<FilterListingPerFetchId.Context> aggregatorCall) {
        TupleEntry clickedListing = null;
        for (TupleEntry tuple : aggregatorCall.getContext().retainedTuplesInGroup.values()) {
            if(tuple.getDouble(DataFields._ADDTOCARTCLICKS) > 0 || tuple.getDouble(DataFields._BUYNOWCLICKS) > 0){
                clickedListing = tuple;
                break;
            }
        }

        Tuple result = new Tuple();
        if(clickedListing == null) {
            aggregatorCall.getOutputCollector().add(
                    aggregatorCall.getContext().retainedTuplesInGroup.firstEntry().getValue().getTuple());
        } else {
            result.add(clickedListing.getTuple());
        aggregatorCall.getOutputCollector().add(clickedListing.getTuple());
        }
    }
}




