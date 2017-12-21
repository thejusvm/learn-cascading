package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.FileProcessor;
import com.sun.corba.se.impl.orbutil.concurrent.Sync;
import org.codehaus.jackson.type.TypeReference;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class Helpers {

    public static String PAD = "<pad>";
    public static String MISSING_DATA = "<missing-val>";
    public static String DEFAULT_CLICK = "<defaultclick>";
    public static String[] DEFAULT_DICT_KEYS = new String[] {PAD, MISSING_DATA, DEFAULT_CLICK};


    private static ObjectMapper mapper = new ObjectMapper();

    public static Map<String, DictIntegerizer> readAttributeDicts(String attributeDictFile) throws IOException {
        ArrayList<String> lines = new ArrayList<>();
        FileProcessor.eachLine(attributeDictFile, lines::add);
        String attDictJson = lines.get(0);
        return mapper.readValue(attDictJson, new TypeReference<Map<String, DictIntegerizer>>() {});
    }

    public static void writeAttributeDicts(Map<String, DictIntegerizer> di, String outputFile) throws IOException {
        String diString = mapper.writeValueAsString(di);
        FileProcessor.SyncWriter writer = new FileProcessor.SyncWriter(outputFile, false);
        writer.write(diString);
        writer.flush();
        writer.close();
    }




}
