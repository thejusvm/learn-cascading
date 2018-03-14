package com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema;

import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Map;

public class FeatureRepo implements Serializable {

    private static FeatureSchema lifestyleFeatureSchema;

    public static final String LIFESTYLE_KEY = "lifestyle";

    private static Map<String, FeatureSchema> featureSchemaMap;

    static {

        lifestyleFeatureSchema = new FeatureSchema();

        lifestyleFeatureSchema.registerFeature(new EnumFeature("productId", Feature.Source.CDM));

        lifestyleFeatureSchema.registerFeature(new EnumFeature("brand", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("ideal_for", Feature.Source.CMS,
                ImmutableSet.of("Women","Men","Women's","Men's","Girls","Boys","Baby","Girl's","Boy's","Kids","Adults","Couple","Junior","Senior","Unisex","Infants"),
                ImmutableMap.of("Women's","Women", "Men's", "Mens", "Girl's","Girls", "Boy's", "Boys")));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("size", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("type", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("color", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("pattern", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("occasion", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("fit", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("material", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("fabric", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("theme", Feature.Source.CMS));
        lifestyleFeatureSchema.registerFeature(new EnumFeature("vertical", Feature.Source.CMS));

//        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._MRP, Feature.Source.CDM));
//        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._FSP, Feature.Source.CDM));
        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._FINALPRICE, Feature.Source.CDM));
        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._DISCOUNTPERCENT, Feature.Source.CDM));
        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._DISCOUNTPRICE, Feature.Source.CDM));
//        lifestyleFeatureSchema.registerFeature(new NumericFeature(DataFields._POSITION, Feature.Source.CDM));


        featureSchemaMap = ImmutableMap.of(LIFESTYLE_KEY, lifestyleFeatureSchema);

    }


    public static FeatureSchema getFeatureSchema(String store) {

        if(featureSchemaMap.containsKey(store)) {
            return featureSchemaMap.get(store);
        } else {
            throw new RuntimeException("Schema not defined for store : " + store);
        }

    }

}
