package com.flipkart.learn.cascading.commons.cascading;

import org.codehaus.jackson.type.TypeReference;

import java.io.Serializable;

/**
 * Created by thejus on 23/11/15.
 */
public abstract class SerializableTypeReference<T> extends TypeReference<T> implements Serializable {

    protected SerializableTypeReference() {
        super();
    }
}
