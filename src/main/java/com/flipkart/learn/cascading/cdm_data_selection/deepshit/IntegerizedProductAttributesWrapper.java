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

    private List<String> fieldNames;
    private List<List<Integer>> allFieldValues;

    private static final Logger LOG = LoggerFactory.getLogger(IntegerizedProductAttributesWrapper.class);

    public IntegerizedProductAttributesWrapper(String dir) throws IOException {

        fieldNames = new ArrayList<>();
        allFieldValues = new ArrayList<>();

        LOG.info("starting to read IntegerizedProductAttributes from path : " + dir);
        List<String> paths = HdfsUtils.listFiles(dir, 1);
        for (String path : paths) {
            FileProcessor.hdfsEachLine(path, new Container<String>() {
                boolean first = true;
                @Override
                public void collect(String line) {
                    if (first) {
                        first = false;
                        fieldNames = Arrays.asList(line.split("\t"));
                    } else {
                        List<Integer> values = Arrays.stream(line.split("\t"))
                                .map(Integer::parseInt)
                                .collect(Collectors.toList());
                        allFieldValues.add(values);
                    }
                }
            });
            LOG.info("done to read IntegerizedProductAttributes from path : " + path);
        }
    }

    public int getNumProducts() {
        return allFieldValues.size();
    }

    public Map<String, Integer> getAttributes(int index) {
        List<Integer> values = allFieldValues.get(index);
        Map<String, Integer> attributeValuesMaps = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            attributeValuesMaps.put(fieldNames.get(i), values.get(i));
        }
        return attributeValuesMaps;
    }


    public static void main(String[] args) throws IOException {
        IntegerizedProductAttributesWrapper wrapper = new IntegerizedProductAttributesWrapper("/Users/thejus/workspace/learn-cascading/data/product-attributes.MOB.int/integerized_attributes");
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
