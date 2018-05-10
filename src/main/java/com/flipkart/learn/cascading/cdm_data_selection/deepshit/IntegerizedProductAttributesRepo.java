package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.apache.commons.math3.util.Pair;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class IntegerizedProductAttributesRepo {

    private static Map<Pair<String, String>, IntegerizedProductAttributesWrapper> repo = new LinkedHashMap<>();

    public static IntegerizedProductAttributesWrapper getWrapper(String path) throws IOException {
        return getWrapper(path, null);
    }

    public static IntegerizedProductAttributesWrapper getWrapper(String path, String idAttribute) throws IOException {
        Pair<String, String> key = new Pair<>(path, idAttribute);
        if(repo.containsKey(key)) {
            return repo.get(key);
        } else {
            synchronized(IntegerizedProductAttributesRepo.class) {
                IntegerizedProductAttributesWrapper wrapper = new IntegerizedProductAttributesWrapper(path, idAttribute);
                repo.put(key, wrapper);
                return wrapper;
            }
        }
    }

}
