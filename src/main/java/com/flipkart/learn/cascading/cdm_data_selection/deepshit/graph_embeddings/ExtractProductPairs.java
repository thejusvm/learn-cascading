package com.flipkart.learn.cascading.cdm_data_selection.deepshit.graph_embeddings;

import com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions.ExtractAttributePairs;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;

public class ExtractProductPairs {

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/session-20180210.10000", "data/session-20180210.10000.productId", "productId"};
        }

        PipeRunner runner = new PipeRunner("query-chain");
        runner.setNumReducers(20);
        ExtractAttributePairs queryChains = new ExtractAttributePairs(args[2]);

        runner.executeHfs(queryChains.getPipe(), args[0], args[1], true);

    }


}
