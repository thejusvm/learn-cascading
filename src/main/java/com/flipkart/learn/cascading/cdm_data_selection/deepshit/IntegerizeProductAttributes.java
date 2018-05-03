package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.Container;
import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureSchema;
import com.flipkart.learn.cascading.commons.CountTracker;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
    Map<String, PivotedStatsTracker> numericfieldToStatsTracker = new LinkedHashMap<>();
    private String firstLine;
    List<Pair<List<Object>, Integer>> integerizedProductsAndCounts = new ArrayList<>();
    private int mincount = -1;

    public void setMincount(int mincount) {
        this.mincount = mincount;
    }

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
            countTracker.getValsSortedByCount(mincount).forEach(dict::get);
        }
    }

    private DictIntegerizer getDict(String field) {
        return attributeToDict.get(field);
    }

    private void collectStatsFromFile(String inputFile, FeatureSchema schema) throws IOException {

        FileProcessor.hdfsEachLine(inputFile, new Container<String>() {

            boolean first = true;
            List<String> fieldNames = null;
            Set<Integer> ignoreIndexes = null;
            Set<Integer> numericIndexes = null;
            int countIndex = 0;

            private void initEnumFieldData(Iterable<String> fieldNames) {
                for (String fieldName : fieldNames) {
                    if(!fieldToCountTracker.containsKey(fieldName)) {
                        fieldToCountTracker.put(fieldName, new CountTracker());
                    }
                }
            }

            private void initNumericFieldData(List<String> numericFeatures) {
                for (String numericFeature : numericFeatures) {
                    if(!numericfieldToStatsTracker.containsKey(numericFeature)) {
                        numericfieldToStatsTracker.put(numericFeature, new PivotedStatsTracker(numericFeature));
                    }
                }

            }

            private void inc(String fieldName, String val, int count) {
                fieldToCountTracker.get(fieldName).add(val.toLowerCase(), count);
            }

            private void trackStats(String fieldName, float val, int count) {
                numericfieldToStatsTracker.get(fieldName).track(val);
            }

            @Override
            public void collect(String line) {
                if (first) {
                    first = false;
                    fieldNames = Arrays.asList(line.split("\t"));

                    Set<String> fieldNamesSet = new HashSet<>(fieldNames);
                    ignoreIndexes = new HashSet<>();
                    numericIndexes = new HashSet<>();

                    List<String> numericFeatures = schema.getFeaturesNamesForType(Feature.FeatureType.NUMERIC);
                    for (String numericFeature : numericFeatures) {
                        ignoreIndexes.add(fieldNames.indexOf(numericFeature));
                        numericIndexes.add(fieldNames.indexOf(numericFeature));
                    }
                    fieldNamesSet.removeAll(numericFeatures);

                    countIndex = fieldNames.indexOf("count");
                    ignoreIndexes.add(countIndex);
                    fieldNamesSet.remove("count");

                    initEnumFieldData(fieldNamesSet);
                    initNumericFieldData(numericFeatures);
                } else {
                    String[] lineSplit = line.split("\t");
                    int count = Integer.parseInt(lineSplit[countIndex]);
                    for (int i = 0; i < fieldNames.size(); i++) {
                        if(numericIndexes.contains(i)) {
                            String fieldName = fieldNames.get(i);
                            String fieldVal = lineSplit[i];
                            trackStats(fieldName, Float.parseFloat(fieldVal), count);
                        }
                        if(!ignoreIndexes.contains(i)) { //same as isEnumField?
                            String fieldName = fieldNames.get(i);
                            String fieldVal = lineSplit[i];
                            inc(fieldName, fieldVal, count);
                        }
                    }
                }
            }
        });

    }


    private void collectStatsFromPath(String inputPath, FeatureSchema schema) throws IOException {
        List<String> files = HdfsUtils.listFiles(inputPath, 1);
        for (int i = 0; i < files.size(); i++) {
            String inputFile = files.get(i);
            collectStatsFromFile(inputFile, schema);
            System.out.println("stats collected from file : " + inputFile);
        }
        initDicts();
    }

    private void processFile(String inputFile, FeatureSchema schema) throws IOException {
        BufferedReader br = HdfsUtils.getReader(inputFile);
        try {
            firstLine = br.readLine();

            List<String> fields = ImmutableList.copyOf(firstLine.split("\t"));
            Set<String> enumFeatures = ImmutableSet.copyOf(schema.getFeaturesNamesForType(Feature.FeatureType.ENUMERATION));
            int numFields = fields.size();
            while(true) {
                String line = br.readLine();
                if (line == null) break;
                String[] values = line.split("\t");
                List<Object> processedValues = new ArrayList<>();
                for (int i = 0; i < numFields; i++) {
                    String field = fields.get(i);
                    if(enumFeatures.contains(field)) {
                        String value = values[i];
                        DictIntegerizer fieldDict = getDict(field);
                        processedValues.add(fieldDict.only_get(value, MISSING_DATA_INDEX));
                    } else {
                        processedValues.add(values[i]);
                    }
                }
                int count = Integer.parseInt(values[fields.indexOf("count")]);
                integerizedProductsAndCounts.add(new ImmutablePair<>(processedValues, count));
            }
        } catch (Exception e) {
            br.close();
            throw e;
        } finally {
            br.close();
        }

    }

    private static ObjectMapper mapper = new ObjectMapper();

    private void processPath(String inputPath, String outputPath, FeatureSchema schema) throws IOException {

        HdfsUtils.cleanDir(outputPath);

        String attributeTuplesPath = getIntegerizedAttributesPath(outputPath);
        String attributeDictsPath = getAttributeDictsPath(outputPath);
        String countsTrackerPath = getCountsTrackerPath(outputPath);
        String attributeSummaryPath = getAttributeSummaryPath(outputPath);
        String attributeSummaryV1Path = getAttributeSummaryV1Path(outputPath);

        List<String> files = HdfsUtils.listFiles(inputPath, 1);
        for (int i = 0; i < files.size(); i++) {
            String inputFile = files.get(i);
            processFile(inputFile, schema);
            System.out.println("processed file : " + inputFile);
        }

        integerizedProductsAndCounts.sort(Comparator.comparing(x -> -1 * x.getValue()));
        List<List<Object>> integerizedProducts = integerizedProductsAndCounts.stream().map(Pair::getKey).collect(Collectors.toList());
        writeIntegerizedAttributes(firstLine, integerizedProducts, attributeTuplesPath + "/part-0");
        DictIntegerizerUtils.writeAttributeDicts(attributeToDict.values(), attributeDictsPath);
        writeCounts(fieldToCountTracker, countsTrackerPath);
        writeSummary(attributeSummaryPath);
        writeV1Summary(attributeSummaryV1Path, schema);

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

    private static List<Integer> getCountsList(CountTracker countTracker, DictIntegerizer dictIntegerizer) throws IOException {
        List<org.apache.commons.math3.util.Pair<String, Integer>> terms = countTracker.getSortedByCount();
        int numterms = dictIntegerizer.getCurrentCount();
        Integer[] counts = new Integer[numterms];
        Arrays.fill(counts, 0);
        for (org.apache.commons.math3.util.Pair<String, Integer> termCount : terms) {
            String term = termCount.getKey();
            Integer count = termCount.getValue();
            int position = dictIntegerizer.only_get(term, -1);
            counts[position] = count;
        }
        return ImmutableList.copyOf(counts);
    }


    private static void writeIntegerizedAttributes(String firstLine, List<List<Object>> integerizedProducts, String outputPath) throws IOException {
        FileProcessor.HDFSSyncWriter writer = new FileProcessor.HDFSSyncWriter(outputPath, true);
        writer.write(firstLine + "\n");
        try {
            for (List<Object> integerizedProduct : integerizedProducts) {
                writer.write(Joiner.on("\t").join(integerizedProduct) + "\n");
            }
        } finally {
            writer.flush();
            writer.close();
        }

    }

    private void writeSummary(String attributeSummaryPath) throws IOException {

        Map<String, Integer> attributesSummary = new HashMap<>();
        for (Map.Entry<String, DictIntegerizer> attributeToDict : attributeToDict.entrySet()) {
            attributesSummary.put(attributeToDict.getKey(), attributeToDict.getValue().getCurrentCount());
            System.out.println("attribute : " + attributeToDict.getKey() + ", size : " + attributeToDict.getValue().getCurrentCount());
        }

        String summaryString = mapper.writeValueAsString(attributesSummary);

        FileProcessor.HDFSSyncWriter writer = new FileProcessor.HDFSSyncWriter(attributeSummaryPath, false);
        try {
            writer.write(summaryString);
        } finally {
            writer.flush();
            writer.close();
        }

    }

    private void writeV1Summary(String attributeSummaryPath, FeatureSchema schema) throws IOException {

        Map<String, Map<String, Object>> attributesSummary = new HashMap<>();

        for (Feature feature : schema.getFeatures()) {

            String featureName = feature.getFeatureName();
            Feature.FeatureType featuretype = feature.getFeatureType();
            Map<String, Object> attributeSummary = new HashMap<>();
            attributesSummary.put(featureName, attributeSummary);
            attributeSummary.put("featureType", featuretype);

            if(feature.getFeatureType() == Feature.FeatureType.ENUMERATION) {
                DictIntegerizer di = attributeToDict.get(featureName);
                int count = di.getCurrentCount();
                attributeSummary.put("num_terms", count);

                CountTracker countTracker = fieldToCountTracker.get(featureName);
                List<Integer> termCounts = getCountsList(countTracker, di);
                attributeSummary.put("term_counts", termCounts);

            } else if(feature.getFeatureType() == Feature.FeatureType.NUMERIC) {
                PivotedStatsTracker statsTracker = numericfieldToStatsTracker.get(featureName);
                attributeSummary.put("stats", statsTracker.getDefaultPivotedStats());
            }
//            System.out.println("attribute : " + attributeSummary);
        }

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

    public static String getAttributeSummaryV1Path(String outputPath) {
        return outputPath + "/attribute_summary.v1";
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

    public static void flow(String inputPath, String outputPath) {
        flow(inputPath, outputPath, -1);
    }

    public static void flow(String inputPath, String outputPath, int minCount) {
        FeatureSchema schema = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY);
        IntegerizeProductAttributes integerizeProductAttributes = new IntegerizeProductAttributes();

        integerizeProductAttributes.setMincount(minCount);
        try {
            integerizeProductAttributes.collectStatsFromPath(inputPath, schema);
            System.out.println("---------------------------------------------");
            System.out.println("Done collecting stats");
            System.out.println("---------------------------------------------");
            integerizeProductAttributes.processPath(inputPath, outputPath, schema);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/session-20180210.10000.explode.products",
                    "data/session-20180210.10000.explode.products-int",
//                    "10"
            };
        }

        String inputPath = args[0];
        String outputPath = args[1];

        if(args.length == 2) {
            flow(inputPath, outputPath);
        } else {
            flow(inputPath, outputPath, Integer.parseInt(args[2]));
        }
    }

}
