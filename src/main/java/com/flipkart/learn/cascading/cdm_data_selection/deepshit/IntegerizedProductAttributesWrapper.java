package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.Container;
import com.flipkart.images.FetchImageUrls;
import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.commons.HdfsUtils;
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

    private static final Logger LOG = LoggerFactory.getLogger(IntegerizedProductAttributesWrapper.class);

    public IntegerizedProductAttributesWrapper(String dir) throws IOException {
        this(dir, null);
    }

    public IntegerizedProductAttributesWrapper(String dir, String idAttribute) throws IOException {
        this(dir, idAttribute, 0, Integer.MAX_VALUE);
    }

    public IntegerizedProductAttributesWrapper(String dir, String idAttribute, int fromIndex, int toIndex) throws IOException {
        this.idAttribute = idAttribute;
        fieldNames = new ArrayList<>();
        allFieldValues = new ArrayList<>();
        counts = new ArrayList<>();


        final int[] lineCounter = {-1};
        LOG.info("starting to read IntegerizedProductAttributes from path : " + dir);
        List<String> paths = HdfsUtils.listFiles(dir, 1);
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

        if(idAttribute != null) {
            int idColumn = fieldNames.indexOf(idAttribute);
            idIndex = createIdIndex(allFieldValues, idColumn);
        }

    }

    private Map<Integer, Integer> createIdIndex(List<List<Integer>> allFieldValues, int idCol) {
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

    public Map<String, Integer> getIdAttributes(int id) {
        if(idAttribute != null) {
            if(idIndex.containsKey(id)) {
                return getAttributes(idIndex.get(id));
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
                "data/session-20180210.10000.explode.products-int/integerized_attributes/part-0", null, 85, 105);
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
