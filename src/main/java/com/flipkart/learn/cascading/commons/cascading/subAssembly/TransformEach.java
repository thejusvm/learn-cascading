package com.flipkart.learn.cascading.commons.cascading.subAssembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.cascading.MultiFieldGuavaWrapper;
import com.flipkart.learn.cascading.commons.cascading.MultiInMultiOutFunction;
import com.flipkart.learn.cascading.commons.cascading.SerializableFunction;
import com.flipkart.learn.cascading.commons.cascading.SingleFieldGuavaWrapper;

/**
 * Created by thejus on 20/11/15.
 */
public class TransformEach extends Each {

    public TransformEach(Pipe basePipe, Fields argumentSelector, SerializableFunction function, Fields outputSelector) {
        this(basePipe, argumentSelector, argumentSelector, function, outputSelector);
    }

    public TransformEach(Pipe basePipe, Fields argumentSelector, Fields outputField, SerializableFunction function, Fields outputSelector) {
        super(basePipe, argumentSelector, getWrapperFunction(outputField, function), outputSelector);
    }

    private static cascading.operation.Function getWrapperFunction(Fields outputField, SerializableFunction function) {
        if (outputField.size() != 1 || function instanceof MultiInMultiOutFunction) {
            return new MultiFieldGuavaWrapper(outputField, (MultiInMultiOutFunction) function);
        } else {
            return new SingleFieldGuavaWrapper(outputField, function);
        }
    }
}
