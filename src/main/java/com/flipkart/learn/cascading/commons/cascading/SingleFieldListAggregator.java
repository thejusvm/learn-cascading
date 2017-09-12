package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Pair;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Operates on a single Cascading field,
 * Returns a List with all the values of the given field
 */
public class SingleFieldListAggregator<T> extends BaseOperation<Pair<List<T>, Tuple>> implements Aggregator<Pair<List<T>, Tuple>> {

    public SingleFieldListAggregator(Fields fields) {
        super(fields);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Pair<List<T>, Tuple>> operationCall) {
        operationCall.setContext(new Pair<List<T>, Tuple>(Lists.<T>newArrayList(), Tuple.size(1)));
    }

    public void start(FlowProcess flowProcess, AggregatorCall<Pair<List<T>, Tuple>> pairAggregatorCall) {
        pairAggregatorCall.getContext().setLhs(null);
        pairAggregatorCall.getContext().getRhs().set(0, null);
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall<Pair<List<T>, Tuple>> pairAggregatorCall) {
        TupleEntry arguments = pairAggregatorCall.getArguments();

        if (arguments.getObject(0) == null)
            return;

        List<T> lhs = pairAggregatorCall.getContext().getLhs();

        List<T> value = lhs == null ? Lists.<T>newArrayList() : lhs;
        if (arguments.getString(0) != null) {
            value.add((T) arguments.getString(0));
            pairAggregatorCall.getContext().setLhs(value);
        }
    }

    public void complete(FlowProcess flowProcess, AggregatorCall<Pair<List<T>, Tuple>> pairAggregatorCall) {
        pairAggregatorCall.getContext().getRhs().set(0, pairAggregatorCall.getContext().getLhs());
        Tuple result = pairAggregatorCall.getContext().getRhs();
        pairAggregatorCall.getOutputCollector().add(result);
    }
}
