package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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
                attributeToDict.put(field, new DictIntegerizer(field, Helpers.DEFAULT_DICT_KEYS));
            }
        }
    }

    private DictIntegerizer getDict(String field) {
        return attributeToDict.get(field);
    }

    private void processFile(String inputFile, String outputFile, boolean first) throws IOException {
        FileProcessor.SyncWriter writer = new FileProcessor.SyncWriter(outputFile, true);
        BufferedReader br = HdfsUtils.getReader(inputFile);
        try {
            String firstLine = br.readLine();

            List<String> fields = new ArrayList<>();
            for (String split : firstLine.split("\t")) {
                if(!"count".equals(split)) {
                    fields.add(split);
                }
            }

            if(first) {
                writer.write(Joiner.on("\t").join(fields) + "\n");
            }

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
        }

    }

    private void processPath(String inputPath, String outputPath) throws IOException {

        File outputDir = new File(outputPath);

        if(outputDir.exists()) {
            for (File file : outputDir.listFiles()) {
                file.deleteOnExit();
            }
        } else {
            outputDir.mkdir();
        }

        String attributeTuplesPath = outputPath + "/integerized_attributes";
        String attributeDictsPath = outputPath + "/attribute_dicts.json";

        List<String> files = HdfsUtils.listFiles(inputPath, 1);
        boolean first = true;
        for (String file : files) {
            processFile(file, attributeTuplesPath, first);
            System.out.println("processed file : " + file);
            first = false;
        }

        Helpers.writeAttributeDicts(attributeToDict, attributeDictsPath);
        for (Map.Entry<String, DictIntegerizer> attributeToDict : attributeToDict.entrySet()) {
            System.out.println("attribute : " + attributeToDict.getKey() + ", size : " + attributeToDict.getValue().getCurrentCount());
        }
//        for (String attribute : attributeToDict.keySet()) {
//            DictIntegerizer attributeDict = attributeToDict.get(attribute);
//            String[] terms = attributeDict.getTerms();
//            String termDictPath = outputPath + "/termDict." + attribute;
//            FileProcessor.SyncWriter termDictWriter = new FileProcessor.SyncWriter(termDictPath, false);
//            for (String term : terms) {
//                termDictWriter.write(term + "\n");
//            }
//            termDictWriter.close();
//
//        }

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
