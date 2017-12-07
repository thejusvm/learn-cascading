package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.ProductObj;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSession;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Pair;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator.lifeStylePrefixes;

public class ExtractAttributePairs implements SimpleFlow {

    public final static String ATTRIBUTE_CHAIN = "attributeChain";
    private String attribute;

    public ExtractAttributePairs(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("query-chain-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        Fields queryChain = new Fields(ATTRIBUTE_CHAIN);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new Each(pipe, userContext, new AttributeChainExtract(queryChain, attribute), Fields.ALL);
        pipe = new Retain(pipe, queryChain);
        String attribute_1 = "attribute_1";
        String attribute_2 = "attribute_2";
        Fields attributes = new Fields(attribute_1, attribute_2);
        pipe = new Each(pipe, queryChain, new GeneratePairs(new Fields(attribute_1, attribute_2)), Fields.ALL);
        pipe = new Retain(pipe, attributes);
        pipe = new GroupBy(pipe, attributes);
        pipe = new Every(pipe, attributes, new Count());
        return pipe;
    }

    private static class GeneratePairs extends BaseOperation implements Function {

        public GeneratePairs(Fields fieldDeclaration) {
            super(fieldDeclaration);
        }


        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            List<String> attributes = (List<String>) functionCall.getArguments().getObject(0);
            List<List<String>> subSets = Lists.partition(attributes, 2);
            for (List<String> subSet : subSets) {
                if(subSet.size() == 2) {
                    functionCall.getOutputCollector().add(new Tuple(subSet.get(0), subSet.get(1)));
                }
            }
        }
    }

    private static class AttributeChainExtract extends BaseOperation implements Function {

        private final String attribute;

        public AttributeChainExtract(Fields field, String attribute) {
            super(field);
            this.attribute = attribute;
        }

        private boolean isLifestyleProduct(ProductObj productObj) {
            return Iterables.any(ImmutableSet.<String>copyOf(lifeStylePrefixes), x -> productObj.getProductId().startsWith(x));

        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            SearchSessions searchSessions = (SearchSessions) functionCall.getArguments().getObject(0);
            List<String> attributeList = new ArrayList<>();

            Collection<SearchSession> sessions = searchSessions.getSessions().values();
            for (SearchSession session : sessions) {
                session.getClickedProduct().stream()
                        .filter(this::isLifestyleProduct)
                        .map(productObj -> new Pair<>(productObj.getProductId(), productObj.getAttributes().get(attribute)))
                        .distinct()
                        .map(Pair::getSecond)
                        .filter(Objects::nonNull)
                        .forEach(attributeList::add);
            }
            if(!attributeList.isEmpty()) {
                functionCall.getOutputCollector().add(new Tuple(attributeList));
            }
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/session-2017-0801.1000", "data/brandChains-2017-0801.1000", "brand"};
        }

        PipeRunner runner = new PipeRunner("query-chain");
        runner.setNumReducers(20);
        ExtractAttributePairs queryChains = new ExtractAttributePairs(args[2]);

        runner.executeHfs(queryChains.getPipe(), args[0], args[1], true);

    }


}
