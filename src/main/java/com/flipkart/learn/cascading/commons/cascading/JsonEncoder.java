package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by thejus on 17/11/15.
 */
public class JsonEncoder extends BaseOperation implements Function {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public JsonEncoder(Fields outputKeys) {
        super(outputKeys);
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();

        Tuple result = new Tuple();
        Fields fields = arguments.getFields();
        for (Comparable field : fields) {
            Object fieldData = arguments.getObject(field);
            try {
                result.add(objectMapper.writeValueAsString(fieldData));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        functionCall.getOutputCollector().add(result);
    }
}