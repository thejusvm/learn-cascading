package com.flipkart.learn.cascading.commons.cascading;

import cascading.pipe.Pipe;

import java.io.Serializable;

/**
 * Created by thejus on 16/11/15.
 */
public interface SimpleFlow extends Serializable {

    public Pipe getPipe();

    public @interface IrSimpleFlow {

        String name();

    }

}
