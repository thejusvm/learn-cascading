package com.flipkart.images;

/**
 * Created by thejus on 19/7/16.
 */
public interface Container<I> {

    public boolean collect(I line);

    default void close() {

    }
}
