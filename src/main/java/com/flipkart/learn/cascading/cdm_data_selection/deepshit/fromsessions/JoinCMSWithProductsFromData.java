package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.DictIntegerizerUtils;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureSchema;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.SerializableFunction;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;

import javax.xml.crypto.Data;
import java.util.List;

public class JoinCMSWithProductsFromData extends SubAssembly {

    private static final String ID = "id";

    public JoinCMSWithProductsFromData(Pipe cmsPipe, Pipe dataPipe, List<String> cmsEnumFields) {
        Fields productIdField = new Fields(DataFields._PRODUCTID);
        Fields IDField = new Fields(ID);
        cmsPipe = new Rename(cmsPipe, productIdField, IDField);

        Pipe pipe = new CoGroup(dataPipe, productIdField, cmsPipe, IDField,
                new LeftJoin());

        pipe = new Discard(pipe, IDField);
        for (String cmsEnumField : cmsEnumFields) {
            if(DataFields._PRODUCTID.equals(cmsEnumField)) continue;
            pipe = new TransformEach(pipe, new Fields(cmsEnumField), new SerializableFunction() {
                @Override
                public Object apply(Object x) {
                    return x == null || "".equals(x) ? DictIntegerizerUtils.MISSING_DATA : x;
                }
            }, Fields.SWAP);
        }
        setTails(pipe);
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/product-attributes.MOB/part-00000", "data/session-20180210.10000.explode.products", "data/session-20180210.10000.explode.products-enrich"};
        }

        FeatureSchema schema = FeatureRepo.getFeatureSchema(FeatureRepo.LIFESTYLE_KEY);
        List<String> cmsEnumFields = ProductsFromSessionExplode.getCMSEnumFields(schema);


        Pipe cmsPipe = new Pipe("cms");
        Pipe dataPipe = new Pipe("data");

        Pipe joinedPipe = new JoinCMSWithProductsFromData(cmsPipe, dataPipe, cmsEnumFields);

        PipeRunner runner = new PipeRunner("attribute-data-joiner");
        runner.addHFSSource(cmsPipe, args[0]);
        runner.addHFSSource(dataPipe, args[1]);
        runner.addHFSTailSink(joinedPipe, args[2], true);
        runner.execute();
    }

}
