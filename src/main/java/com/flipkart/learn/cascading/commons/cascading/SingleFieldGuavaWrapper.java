package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Guava Function wrapper over Cascading Function
 * Works on one cascading field, to generate another cascading field
*/
public class SingleFieldGuavaWrapper extends BaseOperation implements Function {

    private final SerializableFunction transformFn;

    public SingleFieldGuavaWrapper(Fields outputField, SerializableFunction transformFn) {
        super(1, outputField);
        this.transformFn = transformFn;
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Object argument = arguments.getObject(arguments.getFields());
        functionCall.getOutputCollector().add(new Tuple(transformFn.apply(argument)));
    }
}
