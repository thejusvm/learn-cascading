package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.google.common.base.Joiner;
import org.apache.commons.io.FileDeleteStrategy;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class IntegerizeProductAttributes {

    private Map<String, DictIntegerizer> attributeToDict;

    public IntegerizeProductAttributes() {
       attributeToDict = new HashMap<>();
    }

    private void initDicts(List<String> fields) {
        for (String field : fields) {
            if(!attributeToDict.containsKey(field)) {
                attributeToDict.put(field, new DictIntegerizer(field, DictIntegerizerUtils.DEFAULT_DICT_KEYS));
            }
        }
    }

    private DictIntegerizer getDict(String field) {
        return attributeToDict.get(field);
    }

    private void processFile(String inputFile, String outputFile) throws IOException {
        FileProcessor.HDFSSyncWriter writer = new FileProcessor.HDFSSyncWriter(outputFile, true);
        BufferedReader br = HdfsUtils.getReader(inputFile);
        try {
            String firstLine = br.readLine();

            List<String> fields = new ArrayList<>();
            for (String split : firstLine.split("\t")) {
                if(!"count".equals(split)) {
                    fields.add(split);
                }
            }

            writer.write(Joiner.on("\t").join(fields) + "\n");

            int numFields = fields.size();
            initDicts(fields);
            while(true) {
                String line = br.readLine();
                if (line == null) break;
                String[] values = line.split("\t");
                Integer[] intValues = new Integer[numFields];

                for (int i = 0; i < numFields; i++) {
                    String field = fields.get(i);
                    String value = values[i];
                    DictIntegerizer fieldDict = getDict(field);
                    intValues[i] = fieldDict.get(value);
                }
                writer.write(Joiner.on("\t").join(intValues) + "\n");
            }
        } catch (Exception e) {
            br.close();
            writer.close();
            throw e;
        } finally {
            br.close();
            writer.flush();
            writer.close();
        }

    }

    private void processPath(String inputPath, String outputPath) throws IOException {

        HdfsUtils.cleanDir(outputPath);

        String attributeTuplesPath = getIntegerizedAttributesPath(outputPath);
        String attributeDictsPath = getAttributeDictsPath(outputPath);

        List<String> files = HdfsUtils.listFiles(inputPath, 1);
        for (int i = 0; i < files.size(); i++) {
            String inputFile = files.get(i);
            String outputFile = attributeTuplesPath + "/part-" + i;
            processFile(inputFile, outputFile);
            System.out.println("processed file : " + inputFile);
        }

        DictIntegerizerUtils.writeAttributeDicts(attributeToDict.values(), attributeDictsPath);
        for (Map.Entry<String, DictIntegerizer> attributeToDict : attributeToDict.entrySet()) {
            System.out.println("attribute : " + attributeToDict.getKey() + ", size : " + attributeToDict.getValue().getCurrentCount());
        }

    }

    public static String getAttributeDictsPath(String outputPath) {
        return outputPath + "/attribute_dicts";
    }

    public static String getIntegerizedAttributesPath(String outputPath) {
        return outputPath + "/integerized_attributes";
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "/Users/thejus/workspace/learn-cascading/data/product-attributes.MOB",
                    "/Users/thejus/workspace/learn-cascading/data/product-attributes.MOB.int",
            };
        }

        String inputPath = args[0];
        String outputPath = args[1];

        IntegerizeProductAttributes integerizeProductAttributes = new IntegerizeProductAttributes();
        try {
            integerizeProductAttributes.processPath(inputPath, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
