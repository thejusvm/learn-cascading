package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.cdm_data_selection.VerticalFromCMSJson;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import com.flipkart.learn.cascading.commons.cascading.postProcess.FilterByPrefix;

/**
 * Created by thejus on 11/10/17.
 */
public class ExtractCmsAttributes implements SimpleFlow {

    private final String[] attributeNames;

    public ExtractCmsAttributes(String[] attributeNames) {
        this.attributeNames = attributeNames;
    }

    @Override
    public Pipe getPipe() {
        Pipe cmsPipe = new Pipe("cmsPipe");
        cmsPipe = new Each(cmsPipe, new Fields(DataFields._CMS),
                new VerticalFromCMSJson(attributeNames), Fields.SWAP);
        return cmsPipe;
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"data/catalog-data.MOB", "data/product-attributes.MOB", "brand,sim_type,vertical"};
        }
        Fields inputFields = new Fields(DataFields._PRODUCTID, DataFields._CMS);
        SimpleFlowRunner.execute(new ExtractCmsAttributes(args[2].split(",")), args[0], inputFields, args[1]);
    }
}
