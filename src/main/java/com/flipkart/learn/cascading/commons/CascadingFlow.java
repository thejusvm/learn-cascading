package com.flipkart.learn.cascading.commons;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by thejus on 18/9/17.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CascadingFlow {

    String name();

}
