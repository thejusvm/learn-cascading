package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.cdm_data_selection.VerticalFromCMSJson;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import com.flipkart.learn.cascading.commons.cascading.postProcess.FilterByPrefix;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.*;

/**
 * Created by thejus on 11/10/17.
 */
public class ExtractCmsAttributes implements SimpleFlow {

    private List<Feature> cmsFeatures;

    public ExtractCmsAttributes(List<Feature> cmsFeatures) {
        this.cmsFeatures = cmsFeatures;
    }

    @Override
    public Pipe getPipe() {
        Pipe cmsPipe = new Pipe("cmsPipe");
        cmsPipe = new Each(cmsPipe, new Fields(DataFields._CMS), new VerticalFromCMSJson(cmsFeatures), Fields.SWAP);
        return cmsPipe;
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"data/catalog-data.MOB", "data/product-attributes.MOB"};
        }

        Fields inputFields = new Fields(DataFields._PRODUCTID, DataFields._CMS);
        List<Feature> cmsFeatures = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY).getFeaturesForSource(Feature.Source.CMS);

        SimpleFlowRunner.execute(new ExtractCmsAttributes(cmsFeatures), args[0], inputFields, args[1]);
    }
}
