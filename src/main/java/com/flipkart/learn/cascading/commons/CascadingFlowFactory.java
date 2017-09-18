package com.flipkart.learn.cascading.commons;

import com.flipkart.learn.cascading.assemblyjoins.AssembledJoinsFlow;
import com.flipkart.learn.cascading.cdm_data_selection.CPRDataFlow;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.data_selection.DataSelectionFlow;
import com.flipkart.learn.cascading.group_aggregation.GroupAggregatorFlow;
import com.flipkart.learn.cascading.pass_through.PassThroughFlow;
import com.flipkart.learn.cascading.plain_copier.PlainCopierFlow;
import com.flipkart.learn.cascading.projection_selection.ProjectionSelectionFlow;
import com.flipkart.learn.cascading.various_joins.VariousJoinsFlow;
import org.reflections.Reflections;

import java.util.Set;

/**
 * Created by arun.agarwal on 19/05/17.
 */
public class CascadingFlowFactory {

    public static CascadingFlows getCascadingFlow(String flowName) {

        CascadingFlows cascadingFlows = null;
        try {
            cascadingFlows = getCascadingFlowFromAnnotation(flowName);
        } catch (Exception e) {
            throw new RuntimeException("error instantiating from annotation", e);
        }

        if(cascadingFlows != null) {
            return cascadingFlows;
        }

        switch (flowName) {
            case "sampleFileCopy":
                return new PlainCopierFlow();
            case "projection-selection":
                return new ProjectionSelectionFlow();
            case "group-aggregator":
                return new GroupAggregatorFlow();
            case "various-joins":
                return new VariousJoinsFlow();
            case "assembled-joins":
                return new AssembledJoinsFlow();
            case "pass-through":
                return new PassThroughFlow();
            case "data-selection":
                return new DataSelectionFlow();
            case "cpr-data":
                return new CPRDataFlow();
            default:
                throw new IllegalArgumentException("Appropriate factory is not available for this runtime configuration in Bucket:");
        }
    }

    private static CascadingFlows getCascadingFlowFromAnnotation(String flowName) throws IllegalAccessException, InstantiationException {
        Reflections reflections = new Reflections("com.flipkart.learn.cascading");
        Set<Class<?>> flowClasses = reflections.getTypesAnnotatedWith(CascadingFlow.class);
        for (Class<?> flowClass : flowClasses) {
            CascadingFlow annotation = flowClass.getAnnotation(CascadingFlow.class);
            String annotationName = annotation.name();
            if(flowName.equals(annotationName)) {
                return (CascadingFlows) flowClass.newInstance();
            }
        }
        return null;
    }
}
