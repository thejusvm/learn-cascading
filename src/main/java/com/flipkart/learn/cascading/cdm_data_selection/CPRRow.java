package com.flipkart.learn.cascading.cdm_data_selection;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.IOException;

/**
 * Created by shubhranshu.shekhar on 06/06/17.
 */
public class CPRRow extends BaseOperation implements Function {

    private static AvroSchemaReader avroSchemaReader;

    static {
        try {
            avroSchemaReader = new AvroSchemaReader("cpr_data_schema/impressionppvSchema.avsc")
                    .buildSchema();
        }
        catch (IOException e){
            e.printStackTrace();//switch to logger for hadoop runs
        }
    }

    //protected String[] compoundFields;

    public CPRRow(Fields outputFields) {
        super(outputFields);
        //this.compoundFields = compoundFields;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry entry = functionCall.getArguments();

        System.out.println(entry.getFields().toString());
        Tuple productPageListingAttributes = (Tuple) entry.getObject("productPageListingAttributes");
        if (productPageListingAttributes != null) {
            String listingId = productPageListingAttributes.getString(avroSchemaReader.getIndex("productPageListingAttributes",
                    "listingId").get().getIdx());

            System.out.println(listingId);
        }
        Tuple result = new Tuple();
        if(entry != null) {
            //result.add(entry.getString(1));
            functionCall.getOutputCollector().add(result);
        }
    }
}
