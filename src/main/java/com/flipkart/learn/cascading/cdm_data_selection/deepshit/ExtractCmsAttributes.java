package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.cdm_data_selection.VerticalFromCMSJson;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import com.flipkart.learn.cascading.commons.cascading.postProcess.FilterByPrefix;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by thejus on 11/10/17.
 */
public class ExtractCmsAttributes implements SimpleFlow {

    private final Map<String, Set<String>> fetchConfig;
    private Map<String, String> renameConfig;

    public ExtractCmsAttributes(Map<String, Set<String>> attributeNames, Map<String, String> renameConfig) {
        this.fetchConfig = attributeNames;
        this.renameConfig = renameConfig;
    }

    @Override
    public Pipe getPipe() {
        Pipe cmsPipe = new Pipe("cmsPipe");
        cmsPipe = new Each(cmsPipe, new Fields(DataFields._CMS),
                new VerticalFromCMSJson(fetchConfig, renameConfig), Fields.SWAP);
        return cmsPipe;
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"data/catalog-data.MOB", "data/product-attributes.MOB", "brand,sim_type,vertical"};
        }
        Map<String, String> renameConfig = ImmutableMap.of("Women's","Women", "Men's", "Mens", "Girl's","Girls", "Boy's", "Boys");
        Map<String, Set<String>> fetchConfig = new LinkedHashMap<>();
        fetchConfig.put("brand", null);
        fetchConfig.put("ideal_for", ImmutableSet.of("Women","Men","Women's","Men's","Girls","Boys","Baby","Girl's","Boy's","Kids","Adults","Couple","Junior","Senior","Unisex","Infants"));
        fetchConfig.put("vertical", null);
        Fields inputFields = new Fields(DataFields._PRODUCTID, DataFields._CMS);
        SimpleFlowRunner.execute(new ExtractCmsAttributes(fetchConfig, renameConfig), args[0], inputFields, args[1]);
    }
}
