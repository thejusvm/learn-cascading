package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;


public class SplitTrainTest extends SubAssembly {

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
            boolean boolVal = longVal > timestamp;
            return reverse == boolVal;
        }
    }
}
