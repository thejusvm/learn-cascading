package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.IntegerizeProductAttributes;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

public class SessionExplodeToPrep extends SubAssembly {

    private static final Logger LOG = LoggerFactory.getLogger(SessionExplodeToPrep.class);

    public SessionExplodeToPrep(Pipe pipe, List<String> fields, String attributeDataDir, long timestamp) {
        String integerizedAttributesPath = IntegerizeProductAttributes.getIntegerizedAttributesPath(attributeDataDir);
        int numpastClicks = 32;

        pipe = new JsonDecodeEach(pipe, new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS), List.class);
        pipe = new NegativeSamplesGenerator(pipe, integerizedAttributesPath, false);
        pipe = new HandlePastClicks(pipe, numpastClicks, false);
        pipe = new AttributeMapToColumns(pipe, fields, false);
        pipe = new JsonEncodeEach(pipe, ((AttributeMapToColumns)pipe).getAllOutputColumns());
        Pipe[] pipes = new SplitTrainTest(pipe, timestamp).getTails();
        setTails(pipes);
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000.int/part-*",
                    "data/product-attributes.MOB.int",
                    "2017-08-01",
                    "data/sessionexplode-2017-0801.1000.final"
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

        String inputPath = args[0];
        String attributeDataDir = args[1];
        String trainTestSplitDate = args[2];
        String outputPath = args[3];
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

        SessionExplodeToPrep toPrep = new SessionExplodeToPrep(pipe, fields, attributeDataDir, timestamp);
        Pipe[] ouputPipes = toPrep.getTails();

        PipeRunner runner = new PipeRunner("prep_data");
        runner.setNumReducers(600);

        runner.addHFSSource(pipe, inputPath);
        runner.addHFSTailSink(ouputPipes[0], trainPath, true);
        runner.addHFSTailSink(ouputPipes[1], testPath, true);
        runner.execute();

    }

}
