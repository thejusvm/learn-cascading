package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.avro.AvroScheme;
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import com.flipkart.learn.cascading.commons.cascading.postProcess.FilterByQuery;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class ExtractPidsFromCDM {

    private Pipe productDataPipe;
    private Pipe cdmPipe;
    private Pipe outputPipe;

    public ExtractPidsFromCDM(String attribute, Set<String> attributeValues) {
        this.productDataPipe = new Pipe("productDataPipe");
        this.cdmPipe = SessionDataGenerator.getCDMPipe();
        Fields productId = new Fields(DataFields._PRODUCTID);
        Fields cdmPidField = new Fields("cdm_pid");
        cdmPipe = new Rename(cdmPipe, productId, cdmPidField);
        cdmPipe = new Retain(cdmPipe, cdmPidField);
        cdmPipe = new Unique(cdmPipe, cdmPidField);

        productDataPipe = new FilterByQuery(productDataPipe, attribute, attributeValues);

        outputPipe = new CoGroup(productDataPipe, productId,
                cdmPipe, cdmPidField,
                new InnerJoin());
        outputPipe = new Discard(outputPipe, cdmPidField);


    }


    public Pipe getProductDataPipe() {
        return productDataPipe;
    }

    public Pipe getCdmPipe() {
        return cdmPipe;
    }

    public Pipe getOutputPipe() {
        return outputPipe;
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/cdm-2017-0801.1000.avro", "data/product-attributes.MOB", "data/product-data.cdm", "brand", "Mi"};
        }

        Tap inputTap = new GlobHfs((Scheme)new AvroScheme(), args[0]);


        ExtractPidsFromCDM cdmPids = new ExtractPidsFromCDM(args[3], ImmutableSet.copyOf(args[4].split(",")));

        PipeRunner runner = new PipeRunner("get_products");
        runner.setNumReducers(600);
        runner.addSource(cdmPids.getCdmPipe(), inputTap);
        runner.addHFSSource(cdmPids.getProductDataPipe(), args[1]);
        runner.addHFSTailSink(cdmPids.getOutputPipe(), args[2], true);
        runner.execute();

    }

}
