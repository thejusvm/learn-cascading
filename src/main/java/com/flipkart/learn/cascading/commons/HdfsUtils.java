package com.flipkart.learn.cascading.commons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by dhruv.pancholi on 28/11/16.
 */
public class HdfsUtils {

    private static Configuration configuration = new Configuration();

    public static void setConfiguration(Configuration configuration) {
        HdfsUtils.configuration = configuration;
    }

    public static List<String> listSortedFiles(String path, int num) throws IOException {
        FileSystem fs = FileSystem.get(new Path(path).toUri(), configuration);
        FileStatus[] items = fs.listStatus(new Path(path));
        List<String> files = new ArrayList<>();
        for (FileStatus item : items) {
            files.add(item.getPath().toString());
        }
        Collections.sort(files, Comparator.reverseOrder());
        files = files.subList(0, Math.min(files.size(), num));
        return files;
    }

    public static Map<String, Integer> getDateNumMap(List<String> files, int margin) {
        List<String> dates = new ArrayList<>(files.size());
        for (String file : files) {
            String[] filer = file.split("/");
            String date = filer[filer.length - 1];
            dates.add(date);
        }
        int count = 1 + margin;
        Map<String, Integer> map = new TreeMap<>();
        for (String date : dates) map.put(date, count++);
        return map;
    }

    public static Map<String, Integer> getDateNumMap(String path, int num, int margin) throws IOException {
        return getDateNumMap(listSortedFiles(path, num), margin);
    }

    public static List<String> listFiles(String path, long minSize) throws IOException {
        FileSystem fs = FileSystem.get(new Path(path).toUri(), configuration);
        FileStatus[] items = fs.listStatus(new Path(path));
        List<String> files = new ArrayList<>();
        for (FileStatus item : items) {
            long length = item.getLen();
            if(length >= minSize) {
                files.add(item.getPath().toString());
            }
        }
        return files;
    }

    public static String slurp(String file) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream in = null;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            in = fs.open(new Path(file));
            IOUtils.copyBytes(in, outputStream, 4096, false);
            return new String(outputStream.toByteArray());
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(outputStream);
        }
    }

    public static BufferedReader getReader(String pt) throws IOException {
        Path path = new Path(pt);
        FileSystem fs = FileSystem.get(configuration);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        return br;
    }

    public static List<String> nextLines(BufferedReader br, int numLines) throws IOException {
        List<String> lines = new ArrayList<>(numLines);
        String line = null;
        for (int i = 0; i < numLines; i++) {
            line = br.readLine();
            if (line == null) return lines;
            lines.add(line);
        }
        return lines;
    }

    public static List<String> nextLines(String path, int numLines) throws IOException {
        BufferedReader br = getReader(path);
        List<String> lines = nextLines(br, numLines);
        br.close();
        return lines;
    }



    public static void printLines(String path) throws IOException {
        BufferedReader br = getReader(path);
        List<String> lines = null;
        while (true) {
            lines = nextLines(br, 30);
            if (lines.size() == 0) return;
            for (String line : lines) {
                System.out.println(line);
            }
        }
    }

    public static void main(String[] args) throws IOException {
//        args = new String[]{"hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-26", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-25", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-24", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-23", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-22", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-21", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-20", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-18", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-17", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-16", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-10", "hdfs://krios/projects/search/bigfoot/outputdata/ConsumerApps/CaEvent/2016-11-09"};
//        System.out.println(getDateNumMap(Arrays.asList(args)));
//        System.out.println(listFiles("hdfs://localhost:9000/"));
        printLines("/Users/dhruv.pancholi/Desktop/meta-data/query_count.txt");
    }
}
