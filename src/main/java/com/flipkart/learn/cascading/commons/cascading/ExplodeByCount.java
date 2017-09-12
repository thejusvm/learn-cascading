package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Explodes the explode fields count field number of times
 */
public class ExplodeByCount extends BaseOperation implements Function {

    private final Fields explodeFields;
    private final Fields countField;

    public ExplodeByCount(Fields outputFields, Fields explodeFields, Fields countField) {
        super(outputFields);
        this.explodeFields = explodeFields;
        this.countField = countField;
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Fields fields = arguments.getFields();
        int num = ((Number) arguments.getObject(countField)).intValue();

        Tuple tuple = new Tuple();
        for (Comparable field : explodeFields) {
            tuple.add(arguments.getObject(field));
        }
        for(int i = 0 ; i < num + 1 ; i++) {
            functionCall.getOutputCollector().add(tuple);
        }
    }
}
