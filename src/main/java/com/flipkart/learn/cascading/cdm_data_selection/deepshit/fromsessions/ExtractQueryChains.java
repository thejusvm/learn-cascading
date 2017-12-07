package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSession;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;

import java.util.*;

public class ExtractQueryChains implements SimpleFlow {

    public final static String _QUERYCHAIN = "queryChain";

    public ExtractQueryChains() {

    }

    @Override
    public Pipe getPipe() {
        Pipe pipe = new Pipe("query-chain-pipe");
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        Fields queryChain = new Fields(_QUERYCHAIN);
        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new Each(pipe, userContext, new QueryChainExtract(queryChain), Fields.ALL);
        pipe = new Retain(pipe, queryChain);

        pipe = new JsonEncodeEach(pipe, queryChain);
        return pipe;
    }

    private static class QueryChainExtract extends BaseOperation implements Function {

        public QueryChainExtract(Fields field) {
            super(field);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            SearchSessions searchSessions = (SearchSessions) functionCall.getArguments().getObject(0);
            List<String> queriesList = new ArrayList<>();

            String previousQuery = "";
            Collection<SearchSession> sessions = searchSessions.getSessions().values();
            for (SearchSession session : sessions) {
                String query = session.getSearchQuery();

                if(query != null && !"".equals(query) && !query.equals(previousQuery)) {
                    queriesList.add(query);
                    previousQuery = query;
                }
            }
            if(!queriesList.isEmpty()) {
                functionCall.getOutputCollector().add(new Tuple(queriesList));
            }
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/session-2017-0801.1000", "data/queryChain-2017-0801.1000"};
        }

        PipeRunner runner = new PipeRunner("query-chain");
        runner.setNumReducers(600);
        ExtractQueryChains queryChains = new ExtractQueryChains();

        runner.executeHfs(queryChains.getPipe(), args[0], args[1], true);

    }

}
