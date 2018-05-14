package com.flipkart.images;

import com.flipkart.learn.cascading.commons.HdfsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by thejus on 19/7/16.
 */
public class FileProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FileProcessor.class);


    public static void eachLine(String inputFile, Container<String> lineCollector) {
        eachLine(new File(inputFile), lineCollector);
    }

    public static void eachLine(File inputFile, Container<String> lineCollector) {
        BufferedReader br = null;
        try {
            String line;
            br = new BufferedReader(new FileReader(inputFile));

            while ((line = br.readLine()) != null) {
                boolean toContinue = lineCollector.collect(line);
                if(!toContinue) {
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lineCollector.close();
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void hdfsBulkEachLine(String inputFile, Container<String> lineCollector) {
        try {
            String[] fileSplit = HdfsUtils.slurp(inputFile).split("\n");
            for (String line : fileSplit) {
                lineCollector.collect(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lineCollector.close();
        }
    }

    public static void hdfsEachLine(String inputFile, Container<String> lineCollector) {
        BufferedReader br = null;
        try {
            String line;
            br = HdfsUtils.getReader(inputFile);

            int counter = 1;
            while ((line = br.readLine()) != null) {
                boolean toContinue = lineCollector.collect(line);
                if(!toContinue) {
                    break;
                }
                counter ++;
                if(counter % 10000 == 0) {
                    LOG.info("read " + inputFile + " lines : " + counter);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lineCollector.close();
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static class SyncWriter {

        private final FileOutputStream fop;
        File file;

        public SyncWriter(String fileName, boolean append) throws FileNotFoundException {
            file = new File(fileName);
            fop = new FileOutputStream(file, append);
        }

        public synchronized void write(String line) throws IOException {
            fop.write(line.getBytes());
        }

        public synchronized void flush() throws IOException {
            fop.flush();
        }

        public void close() throws IOException {
            fop.close();
        }
    }

    public static class HDFSSyncWriter {

        private final OutputStream fop;

        public HDFSSyncWriter(String fileName, boolean overwrite) throws IOException {
            fop = HdfsUtils.getOuputStream(fileName, overwrite);
        }

        public synchronized void write(String line) throws IOException {
            fop.write(line.getBytes());
        }

        public synchronized void flush() throws IOException {
            fop.flush();
        }

        public void close() throws IOException {
            fop.close();
        }
    }


}

