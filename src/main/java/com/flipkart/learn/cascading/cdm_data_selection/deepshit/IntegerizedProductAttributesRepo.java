package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class IntegerizedProductAttributesRepo {

    private static Map<String, IntegerizedProductAttributesWrapper> repo = new LinkedHashMap<>();

    public static IntegerizedProductAttributesWrapper getWrapper(String path) throws IOException {
        if(repo.containsKey(path)) {
            return repo.get(path);
        } else {
            synchronized(IntegerizedProductAttributesRepo.class) {
                IntegerizedProductAttributesWrapper wrapper = new IntegerizedProductAttributesWrapper(path);
                repo.put(path, wrapper);
                return wrapper;
            }
        }
    }

}
