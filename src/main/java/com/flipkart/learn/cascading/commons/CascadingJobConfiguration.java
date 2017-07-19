package com.flipkart.learn.cascading.commons;

import cascading.property.AppProps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Map;
import java.util.Properties;

/**
 * Created by arun.agarwal on 19/05/17.
 */
public class CascadingJobConfiguration {

    public static Properties getConfiguration(int numReducers) {
        Properties properties = new Properties();
        properties.setProperty("mapred.job.queue.name","search");
        properties.setProperty("io.compression.codecs","org.apache.hadoop.io.compress.SnappyCodec," +
                "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec," +
                "org.apache.hadoop.io.compress.BZip2Codec");

//         loading action conf prepared by Oozie
        String actionXml = System.getProperty("oozie.action.conf.xml");
        //Additional overides for our oozie cluster.
        properties.setProperty("mapreduce.job.reduces", ""+numReducers);
        properties.setProperty("'mapred.mapper.new-api", "true");

        if (actionXml == null) {
            return properties;
        }
        if (!new File(actionXml).exists()) {
            return properties;
        }

        Configuration actionConf = new Configuration(false);
        actionConf.addResource(new Path("file:///", actionXml));
        for (Map.Entry<String, String> entry : actionConf) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        //sproperties.setProperty("mapred.max.split.size", "1073741824");
        properties.setProperty("mapreduce.input.fileinputformat.split.maxsize", "1073741824");
        properties.setProperty("mapreduce.input.fileinputformat.split.minsize", "1073741824");
        properties.setProperty("mapred.fileinputformat.split.maxsize", "1073741824");


        properties.setProperty("mapreduce.job.reduce.slowstart.completedmaps", "0.95");
        properties.setProperty("mapreduce.task.io.sort.mb", "1024");
        properties.setProperty("mapreduce.task.io.sort.factor", "50");
// properties.setProperty("mapred.max.split.size", "1024");
        properties.setProperty("mapreduce.input.fileinputformat.split.maxsize", "1073741824");
        properties.setProperty("mapreduce.input.fileinputformat.split.minsize", "1073741824");
        properties.setProperty("mapreduce.job.reduces", ""+numReducers);
        properties.setProperty("mapred.mapper.new-api", "true");
        properties.setProperty("avro.mapred.ignore.inputs.without.extension", "false");


        //Overriders if any.
        properties.setProperty("avro.mapred.ignore.inputs.without.extension", "false");

        return properties;
    }


    public static void setJobDetails(Properties properties, Class clazz, String tag, String applicationName) {
        AppProps.addApplicationTag(properties, tag);
        AppProps.setApplicationJarClass(properties, clazz);
        AppProps.setApplicationName(properties, applicationName);
    }
}
