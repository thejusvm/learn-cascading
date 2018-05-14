package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.apache.commons.math3.util.Pair;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class IntegerizedProductAttributesRepo {

    private static Map<CacheKey, IntegerizedProductAttributesWrapper> repo = new LinkedHashMap<>();

    public static IntegerizedProductAttributesWrapper getWrapper(String path) throws IOException {
        return getWrapper(path, null, 0, Integer.MAX_VALUE);
    }

    public static IntegerizedProductAttributesWrapper getWrapper(String path, String idAttribute, int start, int end) throws IOException {
        CacheKey key = new CacheKey(path, idAttribute, start, end);
        if(repo.containsKey(key)) {
            return repo.get(key);
        } else {
            synchronized(IntegerizedProductAttributesRepo.class) {
                IntegerizedProductAttributesWrapper wrapper = new IntegerizedProductAttributesWrapper(path, idAttribute, start, end);
                repo.put(key, wrapper);
                return wrapper;
            }
        }
    }

    public static class CacheKey {

        private final String path;
        private final String idAttribute;
        private final int start;
        private final int end;

        public CacheKey(String path, String idAttribute, int start, int end) {
            this.path = path;
            this.idAttribute = idAttribute;
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (start != cacheKey.start) return false;
            if (end != cacheKey.end) return false;
            if (path != null ? !path.equals(cacheKey.path) : cacheKey.path != null) return false;
            return idAttribute != null ? idAttribute.equals(cacheKey.idAttribute) : cacheKey.idAttribute == null;
        }

        @Override
        public int hashCode() {
            int result = path != null ? path.hashCode() : 0;
            result = 31 * result + (idAttribute != null ? idAttribute.hashCode() : 0);
            result = 31 * result + start;
            result = 31 * result + end;
            return result;
        }
    }

}
