package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * Operates on a single Cascading field containing a List,
 * For a list of size N, generates N^2 uniq Pairs
 * Generates pairs only in one direction i.e does not generate (a.b) and (b,a).
 * i.e. generates only the lower diagonal elements in the N X N matrix.
 * Uses the comparator passed to sort the list before generating the pairs.
*/
public class GenerateUniqPairs<T> extends BaseOperation<Collection<T>> implements Function<Collection<T>> {

    private final Comparator<T> comparator;
    private final boolean bothWays;

    public GenerateUniqPairs(Fields pairFieldNames, Comparator<T> comparator) {
        this(pairFieldNames, comparator, false);
    }

    public GenerateUniqPairs(Fields pairFieldNames, Comparator<T> comparator, boolean bothWays) {
        super(1, pairFieldNames);
        this.comparator = comparator;
        this.bothWays = bothWays;
    }

    public GenerateUniqPairs(Fields pairFieldNames) {
        this(pairFieldNames, null, false);
    }

    public void operate(FlowProcess flowProcess, FunctionCall<Collection<T>> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object object = arguments.getObject(arguments.getFields());
        Collection<T> collection = (Collection<T>) object;
        List<T> list = new ArrayList<T>(collection);
        if(comparator != null) {
            Collections.sort(list, comparator);
        }
        list = Lists.reverse(list);
        for (int i = 0; i < list.size(); i++) {
            T ith = list.get(i);
            for (int j = 0; j < list.size(); j++) {
                T jth = list.get(j);
                if(bothWays || i > j) {
                    Tuple result = new Tuple(ith, jth);
                    functionCall.getOutputCollector().add(result);
                }

            }
        }
    }
}
