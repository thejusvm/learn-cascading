package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.*;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.images.FetchImageUrls;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SingleFieldListAggregator;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.flipkart.learn.cascading.cdm_data_selection.DataFields._ACCOUNTID;

public class SessionDataMerger extends SubAssembly{

    private static final Logger LOG = LoggerFactory.getLogger(FetchImageUrls.class);

    public SessionDataMerger(List<Pipe> sessionPipes) {
        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);

        Pipe thePipe = new Merge(sessionPipes.toArray(new Pipe[0]));
        thePipe = new GroupBy(thePipe, new Fields(_ACCOUNTID));
        thePipe = new Every(thePipe, userContext, new SingleFieldListAggregator(userContext));
        thePipe = new Each(thePipe, userContext, new MergeSessions(userContext), Fields.SWAP);
        thePipe = new JsonEncodeEach(thePipe, userContext);
        setTails(thePipe);
    }

    private static class MergeSessions extends BaseOperation implements Function {

        private static ObjectMapper mapper = new ObjectMapper();

        public MergeSessions(Fields fields) {
            super(fields);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            List<String> sessionsString = (List<String>) functionCall.getArguments().getObject(0);
            List<SearchSessions> sessions = sessionsString.stream()
                    .filter(Objects::nonNull)
                    .map(x -> {
                        try {
                            return mapper.readValue(x, SearchSessions.class);
                        } catch (IOException e) {
                            LOG.error("", e);
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList());
            functionCall.getOutputCollector().add(new Tuple(SearchSessions.mergeSessions(sessions)));
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/session-2017-0801.1000/part-*", "data/session-2017-0801.1000.copy/part-*", "data/session-2017-0801.1000.merged"};
        }

        List<String> argsList = ImmutableList.copyOf(args);

        List<String> inputPaths = new ArrayList<String>();
        for (int i = 0; i < argsList.size() - 1; i++) {
            String arg = argsList.get(i);
            inputPaths.add(arg);
        }

        if(inputPaths.size() < 1) {
            System.out.println("Num input paths " + inputPaths.size() + " less than expected size 2");
            return;
        }

        String outputPath = argsList.get(argsList.size() - 1);

        PipeRunner pipeRunner = new PipeRunner("bulk-multi-sessions-merger");

        List<Pipe> inputPipes = new ArrayList<>(inputPaths.size());
        for (int i = 0; i < inputPaths.size(); i++) {
            String inputPath = inputPaths.get(i);
            Pipe inputPipe = new Pipe("single-day-" + i);
            inputPipes.add(inputPipe);
            pipeRunner.addHFSSource(inputPipe, inputPath);
        }

        SessionDataMerger merger = new SessionDataMerger(inputPipes);
        pipeRunner.addHFSTailSink(merger, outputPath, true);
        pipeRunner.setNumReducers(1000);
        pipeRunner.execute();
    }

}
