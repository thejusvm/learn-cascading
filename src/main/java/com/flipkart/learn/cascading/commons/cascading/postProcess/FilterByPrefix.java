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

    private Set<String> prefixes;
    private int queryField = 0;

    public FilterByPrefix(int queryField, Set<String> prefixes) {
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
            args = new String[]{"data/test-session-exploded.1", "data/test-session-exploded.1.search", "accountId", "ACC988CE5AADDB408CB9A7E8715BC7987DQ"};
        }

        ImmutableSet<String> prefixes = ImmutableSet.copyOf(args[3].split(","));
        SimpleFlowRunner.execute(new FilterByPrefix(Integer.parseInt(args[2]), prefixes), args[0], args[1]);
    }

    private static class QueryIn extends BaseOperation implements Filter, Serializable {

        private static ObjectMapper objectMapper = new ObjectMapper();
        private int queryField;
        private Set<String> prefixes;

        public QueryIn(int queryField, Set<String> prefixes) {
            this.queryField = queryField;
            this.prefixes = prefixes;
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
            String fieldData = (String) filterCall.getArguments().getObject(queryField, String.class);
            Iterables.all(prefixes, prefix -> {
                assert prefix != null;
                return fieldData.startsWith(prefix);
            });
            return !prefixes.contains(fieldData);
        }
    }
}
;