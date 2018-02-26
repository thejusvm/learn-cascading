package com.flipkart.learn.cascading.commons.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.HfsProps;
import cascading.tuple.Fields;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by thejus on 9/11/15.
 */
public class PipeRunner {

    private final List<SerializationType> serializationTypes;

    private final FlowDef flowDef;

    private int numReducers;

    private String mapChildJavaOpts;

    private long combinedSize = 512l * 1024l * 1024l;

    public enum SerializationType {

        HADOOP("org.apache.hadoop.io.serializer.WritableSerialization"),
        JAVA("org.apache.hadoop.io.serializer.JavaSerialization"),
        KRYO("cascading.kryo.KryoSerialization"),
        AVRO("main.java.com.flipkart.cascading.avro.AvroSpecificRecordSerialization");

        private String className;

        SerializationType(String className) {
            this.className = className;
        }

        public String getClassName() {
            return className;
        }
    }

    public PipeRunner(FlowDef flowDef) {
        this.flowDef = flowDef;
        this.serializationTypes = new ArrayList<>();
    }

    public PipeRunner(String processName) {
        flowDef = FlowDef.flowDef();
        flowDef.setName(processName);
        this.serializationTypes = new ArrayList<>();
    }

    public PipeRunner setNumReducers(int numReducers) {
        this.numReducers = numReducers;
        return this;
    }

    public PipeRunner addSerializationType(SerializationType serializationType) {
        // add Java serializaiton only if u need it badly, as it will slow down the whole thing.
        serializationTypes.add(serializationType);
        return this;
    }

    public static boolean isLocal() {
        return System.getProperties().getProperty("os.name").toLowerCase().contains("mac");
    }

    public void setCombinedSize(long combinedSize) {
        this.combinedSize = combinedSize;
    }

    public void executeFlowDef(FlowDef flowDef) {
        Joiner joiner = Joiner.on(",").skipNulls();

        // add Java serializaiton only if u need it badly, as it will slow down the whole thing.
        String serialization = joiner.join(ImmutableList
                .<SerializationType>builder()
                .addAll(serializationTypes)
                .add(SerializationType.HADOOP)
                .build()
                .stream()
                .map(input -> input.getClassName())
                .collect(Collectors.toList()));

        Configuration conf = new Configuration();
        Iterator<Map.Entry<String, String>> confIter = conf.iterator();
        Map<Object, Object> confMap = new HashMap<>();
        while (confIter.hasNext()) {
            Map.Entry<String, String> nextElement = confIter.next();
            confMap.put(nextElement.getKey(), nextElement.getValue());
        }

        Properties properties = AppProps.appProps().buildProperties(confMap);
        properties.setProperty("mapred.task.timeout", "600000");
        String split = "671088640";
        properties.setProperty("mapred.max.split.size", split);
        properties.setProperty("mapred.min.split.size", split);
        properties.setProperty("mapreduce.job.reduces", "200");
        properties.setProperty("mapred.reduce.tasks", "200");
        properties.setProperty("io.serializations", serialization);
        properties.setProperty("mapred.job.queue.name", "search_dev");
        properties.setProperty("mapred.map.child.java.opts", "-Xmx1024m");
        properties.setProperty("mapred.child.java.opts", "-Xmx1024m");
        properties.setProperty("mapreduce.task.timeout", "6000000");
//        properties.setProperty("mapreduce.job.reduce.slowstart.completedmaps", "0.7");
//        properties.setProperty("mapreduce.input.fileinputformat.split.maxsize", "2294967296");
//        properties.setProperty("mapreduce.input.fileinputformat.split.minsize", "2294967296");
//        properties.setProperty("mapreduce.task.io.sort.mb", "1024");
//        properties.setProperty("mapreduce.task.io.sort.factor", "50");

//        properties.setProperty("mapreduce.map.speculative", "true");
//        properties.setProperty("mapreduce.reduce.speculative", "true");
//        properties.setProperty("mapreduce.job.speculative.slowtaskthreshold", "1.0");

        HfsProps.setCombinedInputMaxSize(properties, combinedSize);
        HfsProps.setUseCombinedInput(properties, true);

        if(isLocal()) {
            numReducers = 1;
        }

        if (numReducers > 0) {
            String numReducersString = String.valueOf(numReducers);
            properties.setProperty("mapred.reduce.tasks", numReducersString);
            properties.setProperty("mapreduce.job.reduces", numReducersString);

        }
        if (mapChildJavaOpts != null && !mapChildJavaOpts.isEmpty()) {
            properties.setProperty("mapred.map.child.java.opts", mapChildJavaOpts);
        }
        AppProps.setApplicationJarClass(properties, this.getClass());
        Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
        Flow wcFlow = flowConnector.connect(flowDef);
        // uncomment to visualize the flow
        // wcFlow.writeDOT("dot/wc.dot");
        wcFlow.complete();
    }

    public void execute() {
        executeFlowDef(flowDef);
    }

    public void executeTap(Pipe pipe, Tap inputTap, Tap outputTap) {
        flowDef.addSource(pipe, inputTap)
               .addTailSink(pipe, outputTap);
        execute();
    }


    public void executeTap(Pipe pipe1, Tap inputTap1, Pipe pipe2, Tap inputTap2, Pipe pipe3, Tap outputTap) {
        flowDef.addSource(pipe1, inputTap1)
               .addSource(pipe2, inputTap2)
               .addTailSink(pipe3, outputTap);
        execute();
    }

    public void executeHfs(Pipe pipe, String input, Fields inputFields, String output, boolean replaceOutput) {
        Tap inputTap;
        if(inputFields.equals(Fields.ALL)) {
            inputTap = new GlobHfs(new TextDelimited(inputFields, true, "\t"), input);
        } else {
            inputTap = new GlobHfs(new TextDelimited(inputFields, "\t"), input);
        }
        Tap outputTap = new Hfs(new TextDelimited(Fields.ALL, true, "\t"), output,
                replaceOutput ? SinkMode.REPLACE : SinkMode.KEEP);
        executeTap(pipe, inputTap, outputTap);
    }

    public void executeHfs(Pipe pipe, String input, String output, boolean replaceOutput) {
        executeHfs(pipe, input, Fields.ALL, output, replaceOutput);
    }

    public void executeHfs(Pipe pipe, String input, String output) {
        executeHfs(pipe, input, output, false);
    }


    public PipeRunner addSource(Pipe pipe, Tap source) {
        flowDef.addSource(pipe, source);
        return this;
    }

    public PipeRunner addHFSSource(Pipe pipe, String fileName, Fields inputFields) {
        Tap inputTap = new GlobHfs(new TextDelimited(inputFields, "\t"), fileName);
        return addSource(pipe, inputTap);
    }

    public PipeRunner addHFSSource(Pipe pipe, String fileName) {
        Tap inputTap = new GlobHfs(new TextDelimited(Fields.ALL, true, "\t"), fileName);
        return addSource(pipe, inputTap);
    }


    public PipeRunner addSources(Map<String, Tap> sources) {
        flowDef.addSources(sources);
        return this;
    }

    public PipeRunner addSink(Pipe tail, Tap sink) {
        flowDef.addSink(tail, sink);
        return this;
    }

    public PipeRunner addTailSink(Pipe tail, Tap sink) {
        flowDef.addTailSink(tail, sink);
        return this;
    }

    public PipeRunner addHFSTailSink(Pipe tail, String fileName, boolean replaceOutput) {
        Tap outputTap = new Hfs(new TextDelimited(Fields.ALL, true, "\t"), fileName,
                replaceOutput ? SinkMode.REPLACE : SinkMode.KEEP);
        return addTailSink(tail, outputTap);
    }

    public PipeRunner addHFSTailSink(Pipe tail, String fileName, boolean replaceOutput, boolean header) {
        Tap outputTap = new Hfs(new TextDelimited(Fields.ALL, header, "\t"), fileName,
                replaceOutput ? SinkMode.REPLACE : SinkMode.KEEP);
        return addTailSink(tail, outputTap);
    }

    public PipeRunner withMapChildJavaOpts(String mapChildJavaOpts) {
        this.mapChildJavaOpts = mapChildJavaOpts;
        return this;
    }


    public PipeRunner addTrap(Pipe pipe, Tap trap) {
        flowDef.addTrap(pipe, trap);
        return this;
    }
}
