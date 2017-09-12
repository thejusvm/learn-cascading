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

import java.util.*;

/**
 * Operates on a multiple Cascading fields,
 * Returns a Map with key as Cascading field and fieldValue as value
 */
public class MapAggregator<T> extends BaseOperation<Pair<List<Map<Comparable, T>>,Tuple>> implements Aggregator<Pair<List<Map<Comparable, T>>,Tuple>> {

    private Comparator<Map<Comparable, ?>> comparator;

    public MapAggregator(Fields fields) {
        super(fields);
    }

    public MapAggregator(Fields fields, Comparator<Map<Comparable, ?>> comparator) {
        this(fields);
        this.comparator = comparator;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Pair<List<Map<Comparable, T>>, Tuple>> operationCall) {
        operationCall.setContext(new Pair<List<Map<Comparable, T>>, Tuple>(Lists.<Map<Comparable, T>>newArrayList(), Tuple.size( 1 ) ) );
    }

    public void start(FlowProcess flowProcess, AggregatorCall<Pair<List<Map<Comparable, T>>, Tuple>> pairAggregatorCall) {
        pairAggregatorCall.getContext().setLhs(null);
        pairAggregatorCall.getContext().getRhs().set( 0, null );
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall<Pair<List<Map<Comparable, T>>, Tuple>> pairAggregatorCall) {
        TupleEntry arguments = pairAggregatorCall.getArguments();

        if( arguments.getObject( 0 ) == null )
            return;

        List<Map<Comparable, T>> lhs = pairAggregatorCall.getContext().getLhs();

        List<Map<Comparable, T>> value = lhs == null ? Lists.<Map<Comparable, T>>newArrayList() : lhs;

        Map<Comparable, T> subValue = new HashMap<Comparable, T>();
        Fields fields = arguments.getFields();
        for (Comparable field : fields) {
            subValue.put(field, (T) arguments.getObject(field));
        }
        value.add(subValue);


        pairAggregatorCall.getContext().setLhs(value);
    }

    public void complete(FlowProcess flowProcess, AggregatorCall<Pair<List<Map<Comparable, T>>, Tuple>> pairAggregatorCall) {
        List<Map<Comparable, T>> lhs = pairAggregatorCall.getContext().getLhs();
        lhs = lhs == null ? Lists.<Map<Comparable, T>>newArrayList() : lhs;
        if(comparator != null) {
            Collections.sort(lhs, comparator);
        }
        pairAggregatorCall.getContext().getRhs().set( 0, lhs);

        Tuple result = pairAggregatorCall.getContext().getRhs();
        pairAggregatorCall.getOutputCollector().add(result);
    }
}


