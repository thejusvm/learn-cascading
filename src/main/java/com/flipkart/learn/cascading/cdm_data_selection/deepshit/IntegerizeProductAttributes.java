package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.Container;
import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.commons.CountTracker;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.google.common.base.Joiner;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.DictIntegerizerUtils.MISSING_DATA_INDEX;

public class IntegerizeProductAttributes {

    private Map<String, DictIntegerizer> attributeToDict;
    Map<String, CountTracker> fieldToCountTracker = new LinkedHashMap<>();

    public IntegerizeProductAttributes() {
       attributeToDict = new HashMap<>();
    }

    private void initDicts() {
        for (String field : fieldToCountTracker.keySet()) {
            if(!attributeToDict.containsKey(field)) {
                attributeToDict.put(field, new DictIntegerizer(field, DictIntegerizerUtils.DEFAULT_DICT_KEYS));
            }
            CountTracker countTracker = fieldToCountTracker.get(field);
            DictIntegerizer dict = attributeToDict.get(field);
            countTracker.getValsSortedByCount().forEach(dict::get);
        }
    }

    private DictIntegerizer getDict(String field) {
        return attributeToDict.get(field);
    }

    private void collectStatsFromFile(String inputFile) throws IOException {

        FileProcessor.hdfsEachLine(inputFile, new Container<String>() {

            boolean first = true;
            List<String> fieldNames = null;
            int countIndex = 0;

            private void initFieldData(Iterable<String> fieldNames) {
                for (String fieldName : fieldNames) {
                    if(!fieldToCountTracker.containsKey(fieldName)) {
                        fieldToCountTracker.put(fieldName, new CountTracker());
                    }
                }
            }

            private void inc(String fieldName, String val, int count) {
                fieldToCountTracker.get(fieldName).add(val.toLowerCase(), count);
            }

            @Override
            public void collect(String line) {
                if (first) {
                    first = false;
                    fieldNames = Arrays.asList(line.split("\t"));
                    countIndex = fieldNames.indexOf("count");
                    Set<String> fieldNamesSet = new HashSet<>(fieldNames);
                    fieldNamesSet.remove("count");
                    initFieldData(fieldNamesSet);
                } else {
                    String[] lineSplit = line.split("\t");
                    int count = Integer.parseInt(lineSplit[countIndex]);
                    for (int i = 0; i < fieldNames.size(); i++) {
                        if(i == countIndex) {
                            continue;
                        }
                        String fieldName = fieldNames.get(i);
                        String fieldVal = lineSplit[i];
                        inc(fieldName, fieldVal, count);
                    }
                }
            }
        });

    }


    private void collectStatsFromPath(String inputPath) throws IOException {
        List<String> files = HdfsUtils.listFiles(inputPath, 1);
        for (int i = 0; i < files.size(); i++) {
            String inputFile = files.get(i);
            collectStatsFromFile(inputFile);
            System.out.println("stats collected from file : " + inputFile);
        }
        initDicts();
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
            while(true) {
                String line = br.readLine();
                if (line == null) break;
                String[] values = line.split("\t");
                Integer[] intValues = new Integer[numFields];

                for (int i = 0; i < numFields; i++) {
                    String field = fields.get(i);
                    String value = values[i];
                    DictIntegerizer fieldDict = getDict(field);
                    intValues[i] = fieldDict.only_get(value, MISSING_DATA_INDEX);
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

    private static ObjectMapper mapper = new ObjectMapper();

    private void processPath(String inputPath, String outputPath) throws IOException {

        HdfsUtils.cleanDir(outputPath);

        String attributeTuplesPath = getIntegerizedAttributesPath(outputPath);
        String attributeDictsPath = getAttributeDictsPath(outputPath);
        String attributeSummaryPath = getAttributeSummaryPath(outputPath);

        List<String> files = HdfsUtils.listFiles(inputPath, 1);
        for (int i = 0; i < files.size(); i++) {
            String inputFile = files.get(i);
            String outputFile = attributeTuplesPath + "/part-" + i;
            processFile(inputFile, outputFile);
            System.out.println("processed file : " + inputFile);
        }

        DictIntegerizerUtils.writeAttributeDicts(attributeToDict.values(), attributeDictsPath);
        Map<String, Integer> attributesSummary = new HashMap<>();
        for (Map.Entry<String, DictIntegerizer> attributeToDict : attributeToDict.entrySet()) {
            attributesSummary.put(attributeToDict.getKey(), attributeToDict.getValue().getCurrentCount());
            System.out.println("attribute : " + attributeToDict.getKey() + ", size : " + attributeToDict.getValue().getCurrentCount());
        }
        writeSummary(attributeSummaryPath, attributesSummary);


    }

    private void writeSummary(String attributeSummaryPath, Map<String, Integer> attributesSummary) throws IOException {
        String summaryString = mapper.writeValueAsString(attributesSummary);
        FileProcessor.HDFSSyncWriter writer = new FileProcessor.HDFSSyncWriter(attributeSummaryPath, false);
        try {
            writer.write(summaryString);
        } finally {
            writer.flush();
            writer.close();
        }
    }

    public static String getAttributeSummaryPath(String outputPath) {
        return outputPath + "/attribute_summary";
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
                    "data/sessions-2017100.products",
                    "data/sessions-2017100.products-int.1",
            };
        }

        String inputPath = args[0];
        String outputPath = args[1];

        IntegerizeProductAttributes integerizeProductAttributes = new IntegerizeProductAttributes();
        try {
            integerizeProductAttributes.collectStatsFromPath(inputPath);
            System.out.println("---------------------------------------------");
            System.out.println("Done collecting stats");
            System.out.println("---------------------------------------------");
            integerizeProductAttributes.processPath(inputPath, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
