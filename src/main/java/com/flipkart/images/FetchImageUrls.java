package com.flipkart.images;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

public class FetchImageUrls {

    private static final Logger LOG = LoggerFactory.getLogger(FetchImageUrls.class);


    private final String host;
    private final String port;

    public FetchImageUrls(String host, String port) {
        this.host = host;
        this.port = port;
    }


    public void dump(String inputFile, String outputFile) throws IOException, ExecutionException, InterruptedException {

        List<String> pids = new ArrayList<>();
        FileProcessor.eachLine(inputFile, line -> pids.add(line.split("\t")[0]));

        List<List<String>> pidsGrouped = Lists.partition(pids, 10);

        FileProcessor.SyncWriter writer = new FileProcessor.SyncWriter(outputFile, true);

        ForkJoinPool forkJoinPool = new ForkJoinPool(10);

        ZuluClient client = new ZuluClient(host, port, 5000);

        forkJoinPool.submit((Callable) () -> {
                    pidsGrouped.parallelStream()
                            .filter(x -> !"productId".equals(x))
                            .forEach(ids -> {
                                List<ZuluClient.Reponse> responses = null;
                                try {
                                    responses = client.getNecessaryResponse(ids);
                                    for (ZuluClient.Reponse respons : responses) {
                                        String line = Joiner.on("\t").join(ImmutableList.of(respons.getProductId(), respons.getVertical(), respons.getImageUrl()));
                                        try {
                                            writer.write(line + "\n");
                                            writer.flush();
                                        } catch (IOException e) {
                                            LOG.error("error writing to file", e);
                                        }
                                    }
                                } catch (IOException e) {
                                    LOG.error("", e);
                                }
                            });
                    return null;
                }).get();

        writer.close();

    }


    public static void main(String[] args)  {

        if(args.length == 0) {
            args = new String[]{"data/product-attributes.MOB/part-00000", "data/imageUrls.MOB.live"};
        }

        FetchImageUrls fetcher = new FetchImageUrls("10.47.1.8", "31200");
        try {
            fetcher.dump(args[0], args[1]);
        } catch (IOException | ExecutionException | InterruptedException e) {
            LOG.error("some error", e);
        }

    }




}
