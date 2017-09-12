package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Map;

/**
 * Operates on a single Cascading field containing a Map,
 * And adds keys from the map as new cascading fields
*/
public class GenerateFieldsFromMap extends BaseOperation<Map<Comparable, ?>> implements Function<Map<Comparable, ?>> {

    private final Fields mapExplodeKeys;

    /**
      * @param outputKeys The names of the output cascading fields
     * @param mapExplodeKeys The keys in the map to be exploded as fields
     */
    public GenerateFieldsFromMap(Fields outputKeys, Fields mapExplodeKeys) {
        super(1, outputKeys);
        this.mapExplodeKeys = mapExplodeKeys;
    }

    public GenerateFieldsFromMap(Fields keys) {
        this(keys, keys);
    }

    public void operate(FlowProcess flowProcess, FunctionCall<Map<Comparable, ?>> mapFunctionCall) {
        TupleEntry arguments = mapFunctionCall.getArguments();
        Map<Comparable, ?> map = (Map<Comparable, ?>) arguments.getObject(arguments.getFields());

        Tuple result = new Tuple();
        for (Comparable mapExplodeKey : mapExplodeKeys) {
            result.add(map.get(mapExplodeKey));
        }
        mapFunctionCall.getOutputCollector().add(result);
    }
}
