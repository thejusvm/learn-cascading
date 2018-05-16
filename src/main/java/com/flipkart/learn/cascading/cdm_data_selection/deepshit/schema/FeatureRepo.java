package com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema;

import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature.*;

public class FeatureRepo implements Serializable {

    private static FeatureSchema lifestyleFeatureSchema;

    public static final String LIFESTYLE_KEY = "lifestyle";

    private static Map<String, FeatureSchema> featureSchemaMap;

    static {

        EnumFeature productId = new EnumFeature("productId", Source.CDM, Volatility.SLOW);
        lifestyleFeatureSchema = new FeatureSchema(productId);

        lifestyleFeatureSchema.registerFeature(new EnumFeature("brand", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("ideal_for", Source.CMS,
                Volatility.SLOW, ImmutableSet.of("Women","Men","Women's","Men's","Girls","Boys","Baby","Girl's","Boy's","Kids","Adults","Couple","Junior","Senior","Unisex","Infants"),
                ImmutableMap.of("Women's","Women", "Men's", "Mens", "Girl's","Girls", "Boy's", "Boys")));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("size", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("type", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("color", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("pattern", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("occasion", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("fit", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("material", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("fabric", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("theme", Source.CMS, Volatility.SLOW));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("vertical", Source.CMS, Volatility.SLOW));

//        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._MRP, Source.CDM));
//        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._FSP, Source.CDM));
        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._FINALPRICE, Source.CDM, Volatility.FAST));
        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._DISCOUNTPERCENT, Source.CDM, Volatility.FAST));
        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._DISCOUNTPRICE, Source.CDM, Volatility.FAST));
//        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._POSITION, Source.CDM));


        featureSchemaMap = ImmutableMap.of(LIFESTYLE_KEY, lifestyleFeatureSchema);

    }


    public static FeatureSchema getFeatureSchema(String store) {

        if(featureSchemaMap.containsKey(store)) {
            return featureSchemaMap.get(store);
        } else {
            throw new RuntimeException("Schema not defined for store : " + store);
        }

    }

    public static void main(String[] args) {

        FeatureSchema schema = FeatureRepo.getFeatureSchema(LIFESTYLE_KEY);
        try {
            System.out.println(new ObjectMapper().writeValueAsString(schema));
        } catch (IOException ignored) {}

    }

}
