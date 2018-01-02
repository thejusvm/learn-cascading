package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.IntegerizeProductAttributes;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SerializableFunction;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.ExtractCmsAttributes.FETCH_CONFIG;
import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.SessionExploder.*;

public class SessionExplodeToPrep extends SubAssembly {

    public SessionExplodeToPrep(List<String> fields, String attributeDataDir) {
        String integerizedAttributesPath = IntegerizeProductAttributes.getIntegerizedAttributesPath(attributeDataDir);
        int numpastClicks = 32;

        Pipe pipe = new Pipe("sessionexplode-translation");

        pipe = new JsonDecodeEach(pipe, new Fields(POSITIVE_PRODUCTS, NEGATIVE_PRODUCTS, PAST_CLICKED_PRODUCTS, PAST_BOUGHT_PRODUCTS), List.class);
        pipe = new NegativeSamplesGenerator(pipe, integerizedAttributesPath, false);
        pipe = new HandlePastClicks(pipe, numpastClicks, false);
        pipe = new AttributeMapToColumns(pipe, fields, false);

        Fields outputColumns = ((AttributeMapToColumns) pipe).getAllOutputColumns();
        for (Comparable outputColumn : outputColumns) {
            //Hacking the zero empty value since tensorflow 1.3 code doesnt handle string split with empty values
            //tensorflow 1.4 has the issue fixed. we cant move to it as of now
            //assumption : negatives is already processed and the set of negatives is non empty
            //this is for clicked and bought signals. and 0 is the index of PAD_TEXT for clicked products
            SerializableFunction serializableFunction = x -> ((List) x).isEmpty() ? "0" : Joiner.on(",").join((List) x);
            pipe = new TransformEach(pipe, new Fields(outputColumn), serializableFunction, Fields.SWAP);
        }
        setTails(pipe);
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000.int/part-*",
                    "data/product-attributes.MOB.int",
                    "data/sessionexplode-2017-0801.1000.final"
            };
        }

        List<String> fields = ImmutableList.copyOf(FETCH_CONFIG.keySet());

        SessionExplodeToPrep toPrep = new SessionExplodeToPrep(fields, args[1]);
        PipeRunner runner = new PipeRunner("prep_data");
        runner.setNumReducers(600);
        runner.executeHfs(toPrep, args[0], args[2], true);

    }

}
