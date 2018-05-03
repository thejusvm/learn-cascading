package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import com.flipkart.images.Container;
import com.flipkart.images.FileProcessor;
import com.flipkart.learn.cascading.commons.HdfsUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DictIntegerizerUtils {

    public static String PAD = "<pad>";
    public static String MISSING_DATA = "<missing-val>";
    public static String DEFAULT_CLICK = "<default>";
    public static String[] DEFAULT_DICT_KEYS = new String[] {PAD, MISSING_DATA, DEFAULT_CLICK};

    public static int MISSING_DATA_INDEX = 1;

    static {
        for (int i = 0; i < DEFAULT_DICT_KEYS.length; i++) {
            String defaultDictKey = DEFAULT_DICT_KEYS[i];
            if(MISSING_DATA.equals(defaultDictKey)) {
                MISSING_DATA_INDEX = i;
                break;
            }
        }
    }


    private static ObjectMapper mapper = new ObjectMapper();

    public static List<DictIntegerizer> readAttributeDicts(String attributeDictPath) throws IOException {
        List<DictIntegerizer> dicts = new ArrayList<>();
        for (String file : HdfsUtils.listFiles(attributeDictPath, 1)) {
            System.out.println("reading dict from file : " + file);
            DictIntegerizerCollector dictCollector = new DictIntegerizerCollector();
            FileProcessor.hdfsEachLine(file, dictCollector);
            DictIntegerizer dict = dictCollector.getDict();
            System.out.println("done reading dict : " + dict);
            dicts.add(dict);
        }
        return dicts;
    }

    public static void writeAttributeDicts(Collection<DictIntegerizer> dicts, String outputPath) throws IOException {

//        HdfsUtils.cleanDir(outputPath);
        for (DictIntegerizer dict : dicts) {
            String attributeDictPath = getAttributeDictPath(outputPath, dict.getName());
            writeAttributeDict(dict, attributeDictPath);
        }

    }

    private static void writeAttributeDict(DictIntegerizer attributeDict, String attributeDictPath) throws IOException {
        String[] terms = attributeDict.getTerms();
        FileProcessor.HDFSSyncWriter termDictWriter = new FileProcessor.HDFSSyncWriter(attributeDictPath, false);
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
