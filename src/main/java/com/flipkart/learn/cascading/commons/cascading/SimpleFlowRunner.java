package com.flipkart.learn.cascading.commons.cascading;

import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * Created by thejus on 15/12/15.
 */
public class SimpleFlowRunner {
    /**
     * SimpleFlowExecutor
      * @param
     *        // args[0] : className of the SimpleFlow
     *         args[1] : Input file path
     *         args[2] : Output file path
     *         args[3] : Fields in the input path
     *
     */
    public static void main(String[] args) {
        try {
            String className = args[0];
            String inputFilePath = args[1];
            String outputFilePath = args[2];

            Fields inputFields = Fields.ALL;
            boolean hasHeader = true;

            if(args.length > 3) {
                hasHeader = false;
                inputFields = new Fields(args[3].split(","));
            }

            Class simpleFlowClass = Class.forName(className);
            SimpleFlow simpleFlow = (SimpleFlow) simpleFlowClass.newInstance();

            execute(simpleFlow, inputFilePath, inputFields, hasHeader, outputFilePath, true);


        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static void execute(SimpleFlow simpleFlow, String inputFilePath, String outputFilePath) {
        execute(simpleFlow, inputFilePath, Fields.ALL, true, outputFilePath, true);
    }

    public static void execute(SimpleFlow simpleFlow, String inputFilePath, boolean hasHeader, String outputFilePath) {
        execute(simpleFlow, inputFilePath, Fields.ALL, hasHeader, outputFilePath, hasHeader);
    }

    public static void execute(SimpleFlow simpleFlow, String inputFilePath, Fields inputFields, String outputFilePath) {
        execute(simpleFlow, inputFilePath, inputFields, false, outputFilePath, true);
    }

    private static void execute(SimpleFlow simpleFlow, String inputFilePath, Fields inputFields, boolean hasHeader, String outputFilePath, boolean outputHasheader) {
        Tap inputTap = new GlobHfs(new TextDelimited(inputFields, hasHeader, "\t" ), inputFilePath);
        Tap outputTap =  new Hfs( new TextDelimited( Fields.ALL, outputHasheader, "\t" ), outputFilePath, SinkMode.REPLACE);
        Pipe pipe = simpleFlow.getPipe();
        PipeRunner runner = new PipeRunner("SimpleFlow-" + simpleFlow.getClass().getSimpleName());
        runner.addSerializationType(PipeRunner.SerializationType.KRYO);
        runner.executeTap(pipe, inputTap, outputTap);
    }


    public static void avroExecute(SimpleFlow simpleFlow, String inputFilePath, String outputFilePath) {
        Tap inputTap = new Hfs(new TextDelimited( Fields.ALL, true, "\t" ),inputFilePath);
        Tap outputTap = new Hfs(new TextDelimited( Fields.ALL, true, "\t" ), outputFilePath, SinkMode.REPLACE);
        Pipe pipe = simpleFlow.getPipe();
        PipeRunner runner = new PipeRunner("AvroSimpleFlow-" + simpleFlow.getClass().getSimpleName());
        runner.addSerializationType(PipeRunner.SerializationType.AVRO);
        runner.executeTap(pipe, inputTap, outputTap);
    }


}
