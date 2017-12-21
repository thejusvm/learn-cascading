package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DictIntegerizer implements Serializable{

    @JsonProperty
    private final String name;

    @JsonProperty
    private final String[] defaultKeys;

    @JsonProperty
    private final Map<String, Integer> termDict;

    @JsonProperty
    private int currentCount;

    @JsonCreator
    public DictIntegerizer(@JsonProperty(value = "name") String name,
                           @JsonProperty(value = "defaultKeys") String[] defaultKeys,
                           @JsonProperty(value = "termDict") Map<String, Integer> termDict,
                           @JsonProperty(value = "currentCount") int currentCount) {
        this.name = name;
        this.defaultKeys = defaultKeys;
        this.termDict = termDict;
        this.currentCount = currentCount;

    }

    public DictIntegerizer(String name, String[] defaultKeys) {
        this.name = name;
        this.defaultKeys = defaultKeys;
        this.termDict = new HashMap<>();
        this.currentCount = 0;
        for (String defaultKey : defaultKeys) {
            this.get(defaultKey);
        }
    }

    public String getName() {
        return name;
    }

    public String[] getDefaultKeys() {
        return defaultKeys;
    }

    public Map<String, Integer> getTermDict() {
        return termDict;
    }

    public int getCurrentCount() {
        return currentCount;
    }

    public void setCurrentCount(int currentCount) {
        this.currentCount = currentCount;
    }

    private void add(String term) {
        termDict.put(term, currentCount);
        currentCount++;
    }

    public int get(String term) {
        term = term.toLowerCase();
        if(!termDict.containsKey(term)) {
            add(term);
        }
        return termDict.get(term);
    }

    public int only_get(String term, int missingVal) {
        if(term != null) {
            term = term.toLowerCase();
            return termDict.getOrDefault(term, missingVal);
        } else {
            return missingVal;
        }
    }

    @JsonIgnore
    public String[] getTerms() {
        String[] terms = new String[currentCount];
        for (Map.Entry<String, Integer> termToInt : termDict.entrySet()) {
            terms[termToInt.getValue()] = termToInt.getKey();
        }
        return terms;
    }

    @Override
    public String toString() {
        return "DictIntegerizer{" +
                "name='" + name + '\'' +
                ", defaultKeys=" + Arrays.toString(defaultKeys) +
                ", termDict=" + termDict +
                ", currentCount=" + currentCount +
                '}';
    }


    public static void main(String[] args) throws IOException {

        DictIntegerizer integerizer = new DictIntegerizer("sup", new String[]{"a", "b", "c"});
        for (int i = 0; i < 10; i++) {
            integerizer.get(String.valueOf(i));
        }

        ObjectMapper mapper = new ObjectMapper();
        String dictIString = mapper.writeValueAsString(integerizer);

        System.out.println(dictIString);

        DictIntegerizer integerizerReconstruct = mapper.readValue(dictIString, DictIntegerizer.class);
        System.out.println(integerizerReconstruct);


    }


}
