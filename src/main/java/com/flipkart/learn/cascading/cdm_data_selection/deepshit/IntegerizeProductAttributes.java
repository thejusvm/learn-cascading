package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.Container;
import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.commons.CountTracker;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.DictIntegerizerUtils.MISSING_DATA_INDEX;

public class IntegerizeProductAttributes {

    private Map<String, DictIntegerizer> attributeToDict;
    Map<String, CountTracker> fieldToCountTracker = new LinkedHashMap<>();
    private String firstLine;
    List<Pair<List<Integer>, Integer>> integerizedProductsAndCounts = new ArrayList<>();

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

    private void processFile(String inputFile) throws IOException {
        BufferedReader br = HdfsUtils.getReader(inputFile);
        try {
            firstLine = br.readLine();

            List<String> fields = new ArrayList<>();
            for (String split : firstLine.split("\t")) {
                if(!"count".equals(split)) {
                    fields.add(split);
                }
            }

            int numFields = fields.size();
            while(true) {
                String line = br.readLine();
                if (line == null) break;
                String[] values = line.split("\t");
                List<Integer> intValues = new ArrayList<>();

                for (int i = 0; i < numFields; i++) {
                    String field = fields.get(i);
                    String value = values[i];
                    DictIntegerizer fieldDict = getDict(field);
                    intValues.add(fieldDict.only_get(value, MISSING_DATA_INDEX));
                }
                int count = Integer.parseInt(values[numFields]);
                intValues.add(count);
                integerizedProductsAndCounts.add(new ImmutablePair<>(intValues, count));
            }
        } catch (Exception e) {
            br.close();
            throw e;
        } finally {
            br.close();
        }

    }

    private static ObjectMapper mapper = new ObjectMapper();

    private void processPath(String inputPath, String outputPath) throws IOException {

        HdfsUtils.cleanDir(outputPath);

        String attributeTuplesPath = getIntegerizedAttributesPath(outputPath);
        String attributeDictsPath = getAttributeDictsPath(outputPath);
        String countsTrackerPath = getCountsTrackerPath(outputPath);
        String attributeSummaryPath = getAttributeSummaryPath(outputPath);

        List<String> files = HdfsUtils.listFiles(inputPath, 1);
        for (int i = 0; i < files.size(); i++) {
            String inputFile = files.get(i);
            processFile(inputFile);
            System.out.println("processed file : " + inputFile);
        }

        integerizedProductsAndCounts.sort(Comparator.comparing(x -> -1 * x.getValue()));
        List<List<Integer>> integerizedProducts = integerizedProductsAndCounts.stream().map(Pair::getKey).collect(Collectors.toList());
        writeIntegerizedAttributes(firstLine, integerizedProducts, attributeTuplesPath + "/part-0");
        DictIntegerizerUtils.writeAttributeDicts(attributeToDict.values(), attributeDictsPath);
        writeCounts(fieldToCountTracker, countsTrackerPath);
        Map<String, Integer> attributesSummary = new HashMap<>();
        for (Map.Entry<String, DictIntegerizer> attributeToDict : attributeToDict.entrySet()) {
            attributesSummary.put(attributeToDict.getKey(), attributeToDict.getValue().getCurrentCount());
            System.out.println("attribute : " + attributeToDict.getKey() + ", size : " + attributeToDict.getValue().getCurrentCount());
        }
        writeSummary(attributeSummaryPath, attributesSummary);

    }

    private static void writeCounts(Map<String, CountTracker> dicts, String outputPath) throws IOException {
        for (Map.Entry<String, CountTracker> dict : dicts.entrySet()) {
            String attributeDictPath = getCountsTrackerAttributePath(outputPath, dict.getKey());
            writeCounts(dict.getValue(), attributeDictPath);
        }

    }

    private static String getCountsTrackerAttributePath(String outputPath, String name) {
        return outputPath + "/" + name + ".counts";
    }

    private static void writeCounts(CountTracker countTracker, String attributeDictPath) throws IOException {
        List<org.apache.commons.math3.util.Pair<String, Integer>> terms = countTracker.getSortedByCount();
        FileProcessor.HDFSSyncWriter termDictWriter = new FileProcessor.HDFSSyncWriter(attributeDictPath, false);
        for (org.apache.commons.math3.util.Pair<String, Integer> term : terms) {
            termDictWriter.write(term.getFirst() + "\t" + term.getSecond() + "\n");
        }
        termDictWriter.flush();
        termDictWriter.close();
    }


    private static void writeIntegerizedAttributes(String firstLine, List<List<Integer>> integerizedProducts, String outputPath) throws IOException {
        FileProcessor.HDFSSyncWriter writer = new FileProcessor.HDFSSyncWriter(outputPath, true);
        writer.write(firstLine + "\n");
        try {
            for (List<Integer> integerizedProduct : integerizedProducts) {
                writer.write(Joiner.on("\t").join(integerizedProduct) + "\n");
            }
        } finally {
            writer.flush();
            writer.close();
        }

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

    public static String getCountsTrackerPath(String outputPath) {
        return outputPath + "/count_tracker";
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
