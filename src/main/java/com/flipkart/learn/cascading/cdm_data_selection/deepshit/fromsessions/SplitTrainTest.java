package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;


public class SplitTrainTest extends SubAssembly {

    private static final Logger LOG = LoggerFactory.getLogger(SplitTrainTest.class);


    public SplitTrainTest(Pipe pipe, long timestamp) {

        Pipe trainPipe = new Each(pipe, new Fields(DataFields._TIMESTAMP), new Greater(DataFields._TIMESTAMP, timestamp, false));

        Pipe testPipe = new Pipe("test-pipe", pipe);
        testPipe = new Each(testPipe, new Fields(DataFields._TIMESTAMP), new Greater(DataFields._TIMESTAMP, timestamp, true));

        setTails(trainPipe, testPipe);

    }


    private class Greater extends BaseOperation implements Filter {

        private String fieldname;
        private final long timestamp;
        private final boolean reverse;

        public Greater(String fieldname, long timestamp, boolean reverse) {
            this.fieldname = fieldname;
            this.timestamp = timestamp;
            this.reverse = reverse;
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
            long longVal = filterCall.getArguments().getLong(fieldname);
            boolean boolVal = longVal < timestamp;
            return reverse == boolVal;
        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000.final/part-*",
                    "2017-08-01",
                    "data/sessionexplode-2017-0801.1000.tt"
            };
        }



        String inputPath = args[0];
        String trainTestSplitDate = args[1];
        String outputPath = args[2];
        String trainPath = outputPath + "/train";
        String testPath = outputPath + "/test";


        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        long timestamp;
        try {
            Date date = format.parse(trainTestSplitDate);
            timestamp = date.getTime();
        } catch (ParseException e) {
            LOG.error("unable to parse date format", e);
            throw new RuntimeException(e);
        }

        Pipe pipe = new Pipe("sessionexplode-translation");

        SplitTrainTest toPrep = new SplitTrainTest(pipe, timestamp);
        Pipe[] ouputPipes = toPrep.getTails();

        PipeRunner runner = new PipeRunner("prep_data");
        runner.setNumReducers(600);

        runner.addHFSSource(pipe, inputPath);
        runner.addHFSTailSink(ouputPipes[0], trainPath, true);
        runner.addHFSTailSink(ouputPipes[1], testPath, true);
        runner.execute();

    }

}
