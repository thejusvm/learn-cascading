package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by thejus on 17/11/15.
 */
public class JsonDecoder extends BaseOperation implements Function {

    private final Class clazz;
    private final SerializableTypeReference typeReference;
    private static ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public JsonDecoder(Fields outputKeys, Class clazz) {
        this(outputKeys, clazz, null);
    }

    public JsonDecoder(Fields outputKeys, SerializableTypeReference typeReference) {
        this(outputKeys, null, typeReference);
    }

    private JsonDecoder(Fields outputKeys, Class clazz, SerializableTypeReference typeReference) {
        super(outputKeys);
        this.clazz = clazz;
        this.typeReference = typeReference;
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();

        Tuple result = new Tuple();
        Fields fields = arguments.getFields();
        for (Comparable field : fields) {
            String fieldData = (String) arguments.getObject(field);
            try {
                Object value = null;
                if(fieldData != null) {
                    if(clazz != null) {
                        value = objectMapper.readValue(fieldData, clazz);
                    } else if(typeReference != null) {
                        value = objectMapper.readValue(fieldData, typeReference);
                    } else {
                        throw new RuntimeException("No class or typerefrence to be used for decoding");
                    }
                }
                result.add(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        functionCall.getOutputCollector().add(result);
    }
}