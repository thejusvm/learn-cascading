package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DictIntegerizerUtilsTest {

    @Test
    public void testEncodeDecode() throws IOException {
        DictIntegerizer a1 = new DictIntegerizer("ab", new String[]{"a", "b", "c"});
        DictIntegerizer a2 = new DictIntegerizer("cd", new String[]{"a", "b", "c"});

        for (int i = 0; i < 10; i++) {
            a1.get(String.valueOf(i));
        }
        for (int i = 20; i < 30; i++) {
            a2.get(String.valueOf(i));
        }

        ImmutableList<DictIntegerizer> dictsMap = ImmutableList.of(a1, a2);

        String outputPath = "data/ditest";
        DictIntegerizerUtils.writeAttributeDicts(dictsMap, outputPath);
        List<DictIntegerizer> reconstructedDictMap = DictIntegerizerUtils.readAttributeDicts(outputPath);

//        System.out.println(dictsMap);
//        System.out.println(reconstructedDictMap);

        assertEquals(dictsMap, reconstructedDictMap);

    }

}