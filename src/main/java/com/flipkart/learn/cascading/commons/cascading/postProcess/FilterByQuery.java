//package com.flipkart.learn.cascading.commons.cascading.postProcess;
//
//import cascading.flow.FlowProcess;
//import cascading.operation.BaseOperation;
//import cascading.operation.Filter;
//import cascading.operation.FilterCall;
//import cascading.pipe.Each;
//import cascading.pipe.Pipe;
//import cascading.tuple.Fields;
//import com.flipkart.intentmeta.intent.cascading.SimpleFlow;
//import com.flipkart.intentmeta.intent.cascading.SimpleFlowRunner;
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Sets;
//import org.codehaus.jackson.map.ObjectMapper;
//
//import java.io.IOException;
//import java.io.Serializable;
//import java.util.Collection;
//import java.util.Map;
//import java.util.Set;
//
///**
// * Decodes the first column as a json Map and looks for the "query" field.
// * Retains only those lines where the query exist in the query set.
// */
//public class FilterByQuery implements SimpleFlow {
//
//    private Set<String> queries;
//    private String queryField = "query";
//
//    public FilterByQuery(Set<String> queries) {
//        this.queries = queries;
//    }
//
//    @Override
//    public Pipe getPipe() {
//        Pipe pipe = new Pipe("filter");
//        pipe = new Each(pipe, Fields.ALL, new QueryIn(queryField, queries));
//        return pipe;
//    }
//
//    public static void main(String[] args) {
//        ImmutableSet<String> queries = ImmutableSet.copyOf(args[2].split(","));
//        SimpleFlowRunner.execute(new FilterByQuery(queries), args[0], false, args[1]);
//    }
//
//
//    private static class QueryIn extends BaseOperation implements Filter, Serializable {
//
//        private static ObjectMapper objectMapper = new ObjectMapper();
//        private String queryField;
//        private Set<String> queries;
//
//        public QueryIn(String queryField, Set<String> queries) {
//            this.queryField = queryField;
//            this.queries = queries;
//        }
//
//        @Override
//        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
//            String col0 = (String) filterCall.getArguments().getObject(0);
//            try {
//                Map dataMap = objectMapper.readValue(col0, Map.class);
//                Object query = dataMap.get(queryField);
//                if(query instanceof String) {
//                    return !queries.contains(query);
//                } else if(query instanceof Map) {
//                    return Sets.intersection(queries, ((Map) query).keySet()).isEmpty();
//                } else if(query instanceof Collection) {
//                    return Sets.intersection(queries, ImmutableSet.copyOf((Collection) query)).isEmpty();
//                }
//                return true;
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }
//}
//;