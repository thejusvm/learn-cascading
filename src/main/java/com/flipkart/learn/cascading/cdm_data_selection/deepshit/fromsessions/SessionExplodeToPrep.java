package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.IntegerizeProductAttributes;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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

        pipe = new JsonEncodeEach(pipe, ((AttributeMapToColumns)pipe).getAllOutputColumns());

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



        SessionExplodeToPrep toPrep = new SessionExplodeToPrep(fields, args[1]);
        PipeRunner runner = new PipeRunner("prep_data");
        runner.setNumReducers(600);
        runner.executeHfs(toPrep, args[0], args[2], true);

    }

}
