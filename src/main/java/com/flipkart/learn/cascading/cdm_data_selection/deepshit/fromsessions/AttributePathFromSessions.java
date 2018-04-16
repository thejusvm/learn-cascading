package com.flipkart.learn.cascading.cdm_data_selection.deepshit.fromsessions;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSession;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SearchSessions;
import com.flipkart.learn.cascading.cdm_data_selection.deepshit.SessionDataGenerator;
import com.flipkart.learn.cascading.commons.cascading.PipeRunner;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonDecodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.JsonEncodeEach;
import com.flipkart.learn.cascading.commons.cascading.subAssembly.TransformEach;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AttributePathFromSessions extends SubAssembly {

    public static final String ATTRIBUTE_PATH = "userContext";
    private final String attribute;

    public AttributePathFromSessions(Pipe pipe, String attribute) {
        this.attribute = attribute;

        Fields userContext = new Fields(SessionDataGenerator.USER_CONTEXT);
        Fields attributePath = new Fields(ATTRIBUTE_PATH);

        pipe = new JsonDecodeEach(pipe, userContext, SearchSessions.class);
        pipe = new TransformEach(pipe, userContext, attributePath, ss -> getProducts((SearchSessions) ss), Fields.RESULTS);
        pipe = new TransformEach(pipe, attributePath, ss -> Joiner.on(" ").join((List)ss), Fields.RESULTS);

        setTails(pipe);

    }

    public List<Object> getProducts(SearchSessions searchSessions) {
        Collection<SearchSession> sessions = searchSessions.getSessions().values();
        List<Object> attributes = new ArrayList<>();
        for (SearchSession session : sessions) {
            List<Object> clickedAttributes = session.getClickedProduct().stream().map(productObj -> productObj.getAttributes().get(attribute)).collect(Collectors.toList());
            attributes.addAll(clickedAttributes);
        }
        return attributes;
    }

    public static void main(String[] args) {

        if(args.length ==0) {
            args = new String[] {"data/session-20180210.10000", "data/session-20180210.10000.path", "productId"};
        }

        String input = args[0];
        String output = args[1];
        String attribute = args[2];

        PipeRunner runner = new PipeRunner("attribute-path");
        runner.executeHfs(new AttributePathFromSessions(new Pipe("path"), attribute), input, output, true);

    }

}
