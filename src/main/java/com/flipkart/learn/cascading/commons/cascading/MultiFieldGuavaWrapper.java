package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Created by thejus on 18/11/15.
 */
public class MultiFieldGuavaWrapper extends BaseOperation implements Function {

    private final MultiInMultiOutFunction transformFn;

    public MultiFieldGuavaWrapper(Fields outputFields, MultiInMultiOutFunction transformFn) {
        super(outputFields);
        this.transformFn = transformFn;
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        int numArguments = arguments.size();
        Object[] guavaInput = new Object[numArguments];
        Fields fields = arguments.getFields();
        int i = 0;
        for (Comparable field : fields) {
            guavaInput[i] = arguments.getObject(field);
            i++;
        }
        functionCall.getOutputCollector().add(new Tuple(transformFn.apply(guavaInput)));
    }
}