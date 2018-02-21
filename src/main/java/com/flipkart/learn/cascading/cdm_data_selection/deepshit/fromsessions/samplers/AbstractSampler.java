package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.samplers;

import com.flipkart.learn.cascading.cdm_data_selection.deepshit.IntegerizedProductAttributesRepo;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.IntegerizedProductAttributesWrapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

public abstract class AbstractSampler implements Sampler, Serializable {

    private boolean ifInit;

    protected IntegerizedProductAttributesWrapper wrapper;
    protected Random random;
    protected int size;

    protected final String attributesPath;

    protected AbstractSampler(String attributesPath) {
        this.attributesPath = attributesPath;
    }

    @Override
    public final void init() throws IOException {
        if(!ifInit) {
            synchronized(AbstractSampler.class) {
                if(!ifInit) {
                    wrapper = IntegerizedProductAttributesRepo.getWrapper(attributesPath);
                    random = new Random();
                    size = wrapper.getNumProducts();
                    ifInit = true;
                    subInit();
                }
            }
        }
    }

    protected abstract void subInit();

}
