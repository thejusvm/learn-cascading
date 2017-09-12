package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Takes a single field containing a of Iterable<Object> and generates a field with multiple tuples containing Object in each tuple
 */
public class ExplodeIterable extends BaseOperation implements Function {
    public ExplodeIterable(Fields fields) {
        super(1, fields);
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Fields fields = arguments.getFields();
        Iterable fieldDataList = (Iterable) arguments.getObject(fields);
        for (Object fieldData : fieldDataList) {
            functionCall.getOutputCollector().add(new Tuple(fieldData));
        }
    }

}
