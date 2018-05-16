package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.Container;
import com.flipkart.images.FetchImageUrls;
import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.sun.tools.corba.se.idl.StringGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class IntegerizedProductAttributesWrapper {

    private final String idAttribute;
    private List<String> fieldNames;
    private List<List<Integer>> allFieldValues;
    private List<Integer> counts;
    private Map<Integer, Integer> idIndex;
    private DictIntegerizer idDict;

    private static final Logger LOG = LoggerFactory.getLogger(IntegerizedProductAttributesWrapper.class);

    public IntegerizedProductAttributesWrapper(String dir) throws IOException {
        this(dir, null);
    }

    public IntegerizedProductAttributesWrapper(String dir, String idAttribute) throws IOException {
        this(dir, idAttribute, 0, Integer.MAX_VALUE);
    }

    public IntegerizedProductAttributesWrapper(String baseDir, String idAttribute, int fromIndex, int toIndex) throws IOException {
        this.idAttribute = idAttribute;
        fieldNames = new ArrayList<>();
        allFieldValues = new ArrayList<>();
        counts = new ArrayList<>();


        String attributesPath = IntegerizeProductAttributes.getIntegerizedAttributesPath(baseDir);
        populateFieldValues(attributesPath, fromIndex, toIndex);
        if(idAttribute != null) {
            idIndex = createIdIndex(allFieldValues, idAttribute);
            String dictPath = IntegerizeProductAttributes.getAttributeDictsPath(baseDir);
            String idDictPath = DictIntegerizerUtils.getAttributeDictPath(dictPath, idAttribute);
            idDict = initDict(idDictPath, idIndex.keySet());
        }
    }

    private DictIntegerizer initDict(String idDictPath, Set<Integer> integers) {
        DictIntegerizer dictIntegerizer = DictIntegerizerUtils.getDictIntegerizer(idDictPath, integers);
        System.out.println("done reading attributes dict from path : " + idDictPath + ", " + idDict);
        return dictIntegerizer;
    }

    private void populateFieldValues(String attributesPath, int fromIndex, int toIndex) throws IOException {
        final int[] lineCounter = {-1};
        LOG.info("starting to read IntegerizedProductAttributes from path : " + attributesPath);
        List<String> paths = HdfsUtils.listFiles(attributesPath, 1);
        Collections.sort(paths);
        for (String path : paths) {
            FileProcessor.hdfsEachLine(path, new Container<String>() {
                boolean first = true;
                int countsIndex = -1;

                @Override
                public boolean collect(String line) {
                    if (first) {
                        first = false;
                        fieldNames = Arrays.asList(line.split("\t"));
                        countsIndex = fieldNames.indexOf("count");
                    } else {
                        lineCounter[0]++;
                        if(lineCounter[0] >= fromIndex) {
                            if(lineCounter[0] < toIndex) {
                                List<Integer> values = Arrays.stream(line.split("\t"))
                                        .map(Integer::parseInt)
                                        .collect(Collectors.toList());
                                allFieldValues.add(values);
                                counts.add(values.get(countsIndex));
                            } else {
                                return false;
                            }
                        }
                    }
                    return true;
                }
            });
            LOG.info("done to read IntegerizedProductAttributes from path : " + path);
        }
    }

    private Map<Integer, Integer> createIdIndex(List<List<Integer>> allFieldValues, String idAttribute) {
        int idCol = fieldNames.indexOf(idAttribute);
        Map<Integer, Integer> index = new HashMap<>();
        for (int i = 0; i < allFieldValues.size(); i++) {
            List<Integer> allFieldValue = allFieldValues.get(i);
            int idVal = allFieldValue.get(idCol);
            index.put(idVal, i);
        }
        return index;
    }

    public List<Integer> getCounts() {
        return counts;
    }

    public int getNumProducts() {
        return allFieldValues.size();
    }

    public Map<String, Integer> getIdAttributes(String id) {
        if(idAttribute != null) {
            int idOrd = idDict.only_get(id, -1);
            if(idIndex.containsKey(idOrd)) {
                return getAttributes(idIndex.get(idOrd));
            } else {
                return null;
            }
        }
        throw new RuntimeException("no id attribute given");
    }

    public Map<String, Integer> getAttributes(int index) {
        List<Integer> values = allFieldValues.get(index);
        Map<String, Integer> attributeValuesMaps = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            if("count".equals(fieldName)) {
                continue;
            }
            attributeValuesMaps.put(fieldName, values.get(i));
        }
        return attributeValuesMaps;
    }


    public static void main(String[] args) throws IOException {
        IntegerizedProductAttributesWrapper wrapper = new IntegerizedProductAttributesWrapper(
                "data/session-20180210.10000.explode.products-int", null, 85, 105);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String next = scanner.next();
            if("exit".equals(next)) {
                break;
            }
            int val = Integer.parseInt(next);
            System.out.println(wrapper.getAttributes(val));
        }
    }


}
