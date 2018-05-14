package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import java.io.IOException;

public class SessionExplodeToTrainTest {

    private static void flow(String input, String productDataPath, String trainTestSplitDate, String output, int numParts) throws IOException {

        String expodedIntegerized = input + ".int";
        String expodedToPrep = input + ".toprep";

        String expodedIntegerizedOut = IntegerizeExplodedSession.flow(input + "/part-*", productDataPath, expodedIntegerized, numParts);
        SessionExplodeToPrep.flow(expodedIntegerizedOut + "/part-*", productDataPath, expodedToPrep);
        SplitTrainTest.flow(expodedToPrep + "/part-*", trainTestSplitDate, output);

    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{
                    "data/session-20180210.10000.explode",
                    "data/session-20180210.10000.explode.products-int",
                    "2017-07-31",
                    "data/session-20180210.10000.explode.split",
                    "2"
            };
        }

        try {
            flow(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

}
