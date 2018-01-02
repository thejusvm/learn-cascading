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

    public static final Map<String, Set<String>> FETCH_CONFIG = new LinkedHashMap<>();
    static {
        FETCH_CONFIG.put("productId", null);
        FETCH_CONFIG.put("brand", null);
        FETCH_CONFIG.put("ideal_for", ImmutableSet.of("Women","Men","Women's","Men's","Girls","Boys","Baby","Girl's","Boy's","Kids","Adults","Couple","Junior","Senior","Unisex","Infants"));
        FETCH_CONFIG.put("type", null);
        FETCH_CONFIG.put("color", null);
        FETCH_CONFIG.put("pattern", null);
        FETCH_CONFIG.put("occasion", null);
        FETCH_CONFIG.put("fit", null);
        FETCH_CONFIG.put("fabric", null);
        FETCH_CONFIG.put("vertical", null);
    }


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

        Fields inputFields = new Fields(DataFields._PRODUCTID, DataFields._CMS);
        Map<String, Set<String>> fetConfig = new LinkedHashMap<>(FETCH_CONFIG);
        fetConfig.remove("productId");
        SimpleFlowRunner.execute(new ExtractCmsAttributes(fetConfig, renameConfig), args[0], inputFields, args[1]);
    }
}
