package com.flipkart.images;

import java.io.*;

/**
 * Created by thejus on 19/7/16.
 */
public class FileProcessor {

    public static void eachLine(String inputFile, Container<String> lineCollector) {
        BufferedReader br = null;
        try {
            String line;
            br = new BufferedReader(new FileReader(inputFile));

            while ((line = br.readLine()) != null) {
                lineCollector.collect(line);
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


}

