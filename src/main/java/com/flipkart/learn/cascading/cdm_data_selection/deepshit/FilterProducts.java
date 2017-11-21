package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import com.flipkart.learn.cascading.commons.cascading.postProcess.FilterByQuery;
import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.map.ObjectMapper;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.Set;

public class FilterProducts {


    private final Pipe productDataPipe;
    private final Pipe liveProductsPipe;
    private String productField;

    public FilterProducts(String productField) {
        this.productField = productField;
        productDataPipe = new Pipe("product_data");
        liveProductsPipe = new Pipe("live_products");
    }

    public Pipe getPipe() {
        Pipe outputPipe = new CoGroup(productDataPipe, new Fields(productField),
                liveProductsPipe, new Fields(DataFields._FSN),
                new RightJoin());
        outputPipe = new Discard(outputPipe, new Fields(DataFields._FSN));
        return outputPipe;
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/product-attributes.MOB", "data/live.MOB", "data/product-attributes.MOB.live"};
        }

        String productDataPath = args[0];
        String filterProductPath = args[1];
        String outputPath = args[2];

        FilterProducts filterProducts = new FilterProducts(DataFields._PRODUCTID);

        PipeRunner runner = new PipeRunner("filter-products");
        runner.addHFSSource(filterProducts.productDataPipe, productDataPath);
        runner.addHFSSource(filterProducts.liveProductsPipe, filterProductPath, new Fields(DataFields._FSN));
        runner.addHFSTailSink(filterProducts.getPipe(), outputPath, true, true);
        runner.execute();

    }


}

