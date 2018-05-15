package com.flipkart.learn.cascading.cdm_data_selection.deepshit.graph_embeddings;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.flipkart.learn.cascading.cdm_data_selection.DataFields;
import com.flipkart.learn.cascading.cdm_data_selection.VerticalFromCMSJson;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.ExtractCmsAttributes;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.Feature;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.schema.FeatureRepo;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlow;
import com.flipkart.learn.cascading.commons.cascading.SimpleFlowRunner;
import org.codehaus.jackson.map.ObjectMapper;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator.lifeStylePrefixes;

public class ExtractProductAttributes implements SimpleFlow {

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public Pipe getPipe() {
        Pipe cmsPipe = new Pipe("cmsPipe");
        cmsPipe = new Each(cmsPipe, new Fields(DataFields._PRODUCTID), new SessionDataGenerator.PrefixFilter(new String[]{"TSH"}));
        cmsPipe = new Each(cmsPipe, new Fields(DataFields._CMS), new ExtractAttributes(), Fields.SWAP);
        return cmsPipe;
    }

    private class ExtractAttributes extends BaseOperation implements Function {
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            String cmsJson = functionCall.getArguments().getString(DataFields._CMS);
            try {
                Map<String, Object> dataMap = mapper.readValue(cmsJson, Map.class);
                for (String key : dataMap.keySet()) {
                    List val = (List) dataMap.get(key);
                    for (Object o : val) {
                        functionCall.getOutputCollector().add(new Tuple(key + ":" + o.toString()));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        }
    }

    public static void main(String[] args) {

        if(args.length == 0) {
            args = new String[]{"data/catalog-data.MOB", "data/product-attributes.pairs"};
        }

        Fields inputFields = new Fields(DataFields._PRODUCTID, DataFields._CMS);

        SimpleFlowRunner.execute(new ExtractProductAttributes(), args[0], inputFields, args[1]);


    }

}
