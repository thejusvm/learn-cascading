package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

public class SessionExplodeToTrainTest {

    private static void flow(String input, String productDataPath, String trainTestSplitDate, String output) {

        String expodedIntegerized = input + ".int";
        String expodedToPrep = input + ".toprep";

        IntegerizeExplodedSession.flow(input + "/part-*", productDataPath, expodedIntegerized);
        SessionExplodeToPrep.flow(expodedIntegerized + "/part-*", productDataPath, expodedToPrep);
        SplitTrainTest.flow(expodedToPrep + "/part-*", trainTestSplitDate, output);

    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/session-20180210.10000.explode",
                    "data/session-20180210.10000.explode.products-int",
                    "2017-07-31",
                    "data/session-20180210.10000.explode.split"
            };
        }

        flow(args[0], args[1], args[2], args[3]);


    }

}
