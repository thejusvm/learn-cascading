package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.DictIntegerizerUtils;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.ImmutableMap;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

public class AttributeMapToColumns extends SubAssembly {

    private final List<String> fieldNames;
    Map<String, String> fieldToPrefix = ImmutableMap.of(
            POSITIVE_PRODUCTS, "positive",
            NEGATIVE_PRODUCTS, "negative",
            PAST_CLICKED_PRODUCTS, "clicked",
            PAST_BOUGHT_PRODUCTS, "bought"
            );


    public AttributeMapToColumns(List<String> fieldNames) {
        this(new Pipe("train_data_gen"), fieldNames);
    }

    public AttributeMapToColumns(Pipe pipe, List<String> fieldNames) {
        this(pipe, fieldNames, true);
    }

    public AttributeMapToColumns(Pipe pipe, List<String> fieldNames, boolean jsonify) {
        this.fieldNames = fieldNames;

        if(jsonify) {
            Fields filedsToProcess = new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS);
            pipe = new JsonDecodeEach(pipe, filedsToProcess, List.class);
        }

        for (Map.Entry<String, String> fieldToPrefix : fieldToPrefix.entrySet()) {
            pipe = new Each(pipe, new Fields(fieldToPrefix.getKey()), new GenerateAttributeColumns(fieldToPrefix.getValue(), fieldNames), Fields.SWAP);
        }

        if(jsonify) {
            pipe = new JsonEncodeEach(pipe, getAllOutputColumns());
        }

        setTails(pipe);
    }

    public Fields getAllOutputColumns() {
        Fields allFields = new Fields();
        for (String prefix : fieldToPrefix.values()) {
            allFields = Fields.merge(allFields, new Fields(generateColumnNames(prefix, fieldNames)));
        }
        return allFields;
    }

    private static String[] generateColumnNames(String prefix, List<String> fieldNames) {
        String[] columnNames = new String[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            columnNames[i] = generateColumnName(prefix, fieldName);
        }
        return columnNames;
    }

    private static String generateColumnName(String prefix, String fieldName) {
        return prefix + "_" + fieldName;
    }

    private static class GenerateAttributeColumns extends BaseOperation implements Function {

        private final List<String> fieldNames;

        public GenerateAttributeColumns(String prefix, List<String> fieldNames) {
            super(new Fields(generateColumnNames(prefix, fieldNames)));
            this.fieldNames = fieldNames;
        }


        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            List<Map<String, Integer>> attributeMaps = (List<Map<String, Integer>>) functionCall.getArguments().getObject(0);
            List<List<Integer>> attributeValues = new ArrayList<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                attributeValues.add(new ArrayList<>());
            }
            for (Map<String, Integer> attributeMap : attributeMaps) {
                for (int i = 0; i < fieldNames.size(); i++) {
                    String fieldName = fieldNames.get(i);
                    Integer value = attributeMap.getOrDefault(fieldName, DictIntegerizerUtils.MISSING_DATA_INDEX);
                    attributeValues.get(i).add(value);
                }
            }
            Tuple result = new Tuple();
            for (List<Integer> attributeValue : attributeValues) {
                result.add(attributeValue);
            }
            functionCall.getOutputCollector().add(result);
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000.neg",
                    "data/sessionexplode-2017-0801.1000.attributecols"
            };
        }

        List<String> fields = new LinkedList<>();
        fields.add("productId");
        fields.add("brand");
        fields.add("ideal_for");
        fields.add("type");
        fields.add("color");
        fields.add("pattern");
        fields.add("occasion");
        fields.add("fit");
        fields.add("fabric");
        fields.add("vertical");

        AttributeMapToColumns prepPipe = null;
        prepPipe = new AttributeMapToColumns(fields);

        PipeRunner runner = new PipeRunner("train-data-gen");
        runner.setNumReducers(600);
        runner.executeHfs(prepPipe, args[0], args[1], true);

    }
}
