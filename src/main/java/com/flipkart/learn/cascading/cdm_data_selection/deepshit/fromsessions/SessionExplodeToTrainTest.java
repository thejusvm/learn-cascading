package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

public class SessionExplodeToTrainTest {

    private static void flow(String input, String productDataPath, String trainTestSplitDate, String output) {

        String expodedIntegerized = input + ".int";
        String expodedToPrep = input + ".toprep";

        IntegerizeExplodedSession.flow(input + "/part-*", productDataPath +"/attribute_dicts" , expodedIntegerized);
        SessionExplodeToPrep.flow(expodedIntegerized + "/part-*", productDataPath, expodedToPrep);
        SplitTrainTest.flow(expodedToPrep + "/part-*", trainTestSplitDate, output);

    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/sessionexplode-2017-0801.1000",
                    "data/product-attributes.MOB.int",
                    "2017-07-31",
                    "data/sessionexplode-2017-0801.1000.tt.longshort"
            };
        }

        flow(args[0], args[1], args[2], args[3]);


    }

}
