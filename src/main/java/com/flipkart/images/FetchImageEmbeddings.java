package com.flipkart.images;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

public class FetchImageEmbeddings {

    private static final Logger LOG = LoggerFactory.getLogger(FetchImageUrls.class);
    private final String host;
    private final String port;
    private String imageEndpoint;
    private final HttpClientWrapper httpCient;
    private static final ObjectMapper mapper = new ObjectMapper();


    public FetchImageEmbeddings(String host, String port, String imageEndpoint) {
        this.host = host;
        this.port = port;
        this.imageEndpoint = imageEndpoint;
        this.httpCient = new HttpClientWrapper(host, port, 5000);
    }

    private void dump(String inputFile, String outputFile) throws IOException, ExecutionException, InterruptedException {

        List<PidObj> pids = new ArrayList<>();
        FileProcessor.eachLine(inputFile, line -> pids.add(PidObj.create(line.split("\t"))));

        FileProcessor.SyncWriter writer = new FileProcessor.SyncWriter(outputFile, true);

        ForkJoinPool forkJoinPool = new ForkJoinPool(10);
        forkJoinPool.submit((Callable) () -> {
                    pids.parallelStream()
                            .forEach(pidObj -> {

                                String id = pidObj.pid;
                                String vertical = pidObj.vertical;
                                String imageUrl = pidObj.getBaseImageUrl();

                                try {
                                    byte[] byteResponse = fetchDeepThoughtResponse(id, vertical, imageUrl);
                                    Map responseMap = mapper.readValue(byteResponse, Map.class);
                                    if (!"error".equals(responseMap.get("status"))) {
                                        Object values = ((Map) responseMap.get("result")).get("fv");
                                        String valuesString = mapper.writeValueAsString(values);
                                        writer.write(id + "\t" + valuesString + "\n");
                                    } else {
                                        writer.write(id + "\t" + "[]" + "\n");
                                    }

                                } catch (IOException e) {
                                    LOG.error("error from API " + pidObj, e);
                                }


                            });
                    return null;
                }).get();
        writer.flush();
        writer.close();
    }

    private byte[] fetchDeepThoughtResponse(String id, String vertical, String imageUrl) throws IOException {
        StopWatch watch = new StopWatch();
        watch.start();
        String url = "http://" + host + ":" + port + "/extract/" + vertical + "/";
        imageUrl = "http://" + imageEndpoint + imageUrl;
        String body = "{\"url\":\"" + imageUrl + "\"}";
        byte[] resp = httpCient.post(url, body, ImmutableMap.of("Content-Type", "application/json"), 0);
        watch.stop();
        LOG.info("fetching data for fsns : " + id + " in " +  watch.getTime() + " ms");
//        System.out.println("fetching data for fsns : " + id + " in " +  watch.getTime() + " ms");
        return resp;
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            args = new String[]{"data/imageUrl.2oq", "data/imageEmb.2oq"};
        }

        FetchImageEmbeddings fetcher = new FetchImageEmbeddings("10.47.0.104", "80", "10.47.1.110");
        try {
            fetcher.dump(args[0], args[1]);
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.error("some error " + e);
        }
    }

}
