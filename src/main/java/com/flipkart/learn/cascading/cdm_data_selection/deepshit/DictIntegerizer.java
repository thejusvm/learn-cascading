package com.flipkart.learn.cascading.cdm_data_selection.deepshit;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class DictIntegerizer implements Serializable{

    private final String name;

    private final Map<String, Integer> termDict;

    private final Set<Integer> collectOnly;

    private int currentCount;

    public DictIntegerizer(String name) {
        this(name, new String[0]);
    }

    public DictIntegerizer(String name, Set<Integer> collectOnly) {
        this(name, new String[0], collectOnly);
    }

    public DictIntegerizer(String name, String[] defaultKeys) {
        this(name, defaultKeys, null);
    }

    public DictIntegerizer(String name, String[] defaultKeys, Set<Integer> collectOnly) {
        this.name = name;
        this.termDict = new HashMap<>();
        this.currentCount = 0;
        for (String defaultKey : defaultKeys) {
            this.get(defaultKey);
        }
        this.collectOnly = collectOnly;
    }

    public String getName() {
        return name;
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
        if(collectOnly == null || collectOnly.contains(currentCount)) {
            termDict.put(term, currentCount);
        }
        currentCount++;
    }

    public int get(String term) {
        term = term.toLowerCase();
        if(!termDict.containsKey(term)) {
            add(term);
        }
        return termDict.getOrDefault(term, -1);
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
                ", currentCount=" + currentCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DictIntegerizer that = (DictIntegerizer) o;

        if (currentCount != that.currentCount) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return termDict != null ? termDict.equals(that.termDict) : that.termDict == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (termDict != null ? termDict.hashCode() : 0);
        result = 31 * result + currentCount;
        return result;
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
