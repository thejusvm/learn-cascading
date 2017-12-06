package com.flipkart.learn.cascading.commons.cascading.postProcess;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Decodes the first column as a json Map and looks for the "query" field.
 * Retains only those lines where the query exist in the query set.
 */
public class FilterByQuery extends SubAssembly {

    private Set<String> queries;
    private String queryField = "query";

    public FilterByQuery(String queryField, Set<String> queries) {
        this(new Pipe("filter_by_query"), queryField, queries);
    }

    public FilterByQuery(Pipe inputPipe, String queryField, Set<String> queries) {
        this.queryField = queryField;
        this.queries = queries;
        Pipe pipe = inputPipe;
        pipe = new Each(pipe, Fields.ALL, new QueryIn(queryField, queries));
        setTails(pipe);
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/test-session-exploded.1", "data/test-session-exploded.1.search", "accountId", "ACC988CE5AADDB408CB9A7E8715BC7987DQ"};
        }

        ImmutableSet<String> queries = ImmutableSet.copyOf(args[3].split(","));
        PipeRunner runner = new PipeRunner("filter_by_query");
        runner.executeHfs(new FilterByQuery(args[2], queries), args[0], args[1], true);
    }

    private static class QueryIn extends BaseOperation implements Filter, Serializable {

        private static ObjectMapper objectMapper = new ObjectMapper();
        private String queryField;
        private Set<String> queries;

        public QueryIn(String queryField, Set<String> queries) {
            this.queryField = queryField;
            this.queries = queries;
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
            String fieldData = filterCall.getArguments().getString(queryField);
            return !queries.contains(fieldData);
        }
    }
}
;