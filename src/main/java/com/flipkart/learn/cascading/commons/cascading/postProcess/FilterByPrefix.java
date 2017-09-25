package com.flipkart.learn.cascading.commons.cascading.postProcess;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.Serializable;
import java.util.Set;

/**
 * Decodes the first column as a json Map and looks for the "query" field.
 * Retains only those lines where the query exist in the query set.
 */
public class FilterByPrefix implements SimpleFlow {

    private String prefixes;
    private int queryField = 0;

    public FilterByPrefix(int queryField, String prefixes) {
        this.queryField = queryField;
        this.prefixes = prefixes;
    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("filter");
        pipe = new Each(pipe, Fields.ALL, new QueryIn(queryField, prefixes));
        return pipe;
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/catalog-data", "data/catalog-data.KBHE25", "0", "KBHE25"};
        }

        SimpleFlowRunner.execute(new FilterByPrefix(Integer.parseInt(args[2]), args[3]), args[0], false, args[1]);
    }

    private static class QueryIn extends BaseOperation implements Filter, Serializable {

        private static ObjectMapper objectMapper = new ObjectMapper();
        private int queryField;
        private String prefixes;

        public QueryIn(int queryField, String prefixes) {
            this.queryField = queryField;
            this.prefixes = prefixes;
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
            String fieldData = (String) filterCall.getArguments().getObject(queryField, String.class);
            boolean startsWith = fieldData.startsWith(prefixes);
            if(startsWith) {
                System.out.println("true");
            } else {
                System.out.println("false");
            }
            return !startsWith;
        }
    }
}
;