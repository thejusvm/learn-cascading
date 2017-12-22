package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.Container;
import com.flipkart.images.FileProcessor;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DictIntegerizerUtils {

    public static String PAD = "<pad>";
    public static String MISSING_DATA = "<missing-val>";
    public static String DEFAULT_CLICK = "<defaultclick>";
    public static String[] DEFAULT_DICT_KEYS = new String[] {PAD, MISSING_DATA, DEFAULT_CLICK};


    private static ObjectMapper mapper = new ObjectMapper();

    public static List<DictIntegerizer> readAttributeDicts(String attributeDictPath) throws IOException {
        List<DictIntegerizer> dicts = new ArrayList<>();
        for (File file : new File(attributeDictPath).listFiles()) {
            DictIntegerizerCollector dictCollector = new DictIntegerizerCollector();
            FileProcessor.eachLine(file, dictCollector);
            DictIntegerizer dict = dictCollector.getDict();
            dicts.add(dict);
        }
        return dicts;
    }

    public static void writeAttributeDicts(Collection<DictIntegerizer> dicts, String outputPath) throws IOException {

        File outputDir = new File(outputPath);

        if(outputDir.exists()) {
            for (File file : outputDir.listFiles()) {
                file.deleteOnExit();
            }
        } else {
            outputDir.mkdir();
        }

        for (DictIntegerizer dict : dicts) {
            String attributeDictPath = getAttributeDictPath(outputPath, dict.getName());
            writeAttributeDict(dict, attributeDictPath);
        }

    }

    private static void writeAttributeDict(DictIntegerizer attributeDict, String attributeDictPath) throws IOException {
        String[] terms = attributeDict.getTerms();
        FileProcessor.SyncWriter termDictWriter = new FileProcessor.SyncWriter(attributeDictPath, false);
        termDictWriter.write(attributeDict.getName() + "\n");
        for (String term : terms) {
            termDictWriter.write(term + "\n");
        }
        termDictWriter.flush();
        termDictWriter.close();
    }

    private static String getAttributeDictPath(String outputPath, String name) {
        return outputPath + "/" + name + ".dict";
    }

    public static Map<String, DictIntegerizer> indexByName(Collection<DictIntegerizer> attribueDictList) {
        Map<String, DictIntegerizer> dictsMap = new HashMap<>();
        for (DictIntegerizer dictIntegerizer : attribueDictList) {
            dictsMap.put(dictIntegerizer.getName(), dictIntegerizer);
        }
        return dictsMap;
    }


    private static class DictIntegerizerCollector implements Container<String> {

        private DictIntegerizer dict;
        boolean first;

        public DictIntegerizerCollector() {
            first = true;
        }

        @Override
        public void collect(String line) {
            if(first) {
                String name = line;
                dict = new DictIntegerizer(name);
                first = false;
            } else {
                dict.get(line);
            }
        }

        public DictIntegerizer getDict() {
            return dict;
        }
    }
}
