//package com.flipkart.images;
//
//import com.aerospike.client.AerospikeClient;
//import com.aerospike.client.Key;
//import com.aerospike.client.Record;
//import com.google.common.collect.ImmutableList;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.ByteArrayInputStream;
//import java.io.DataInputStream;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//public class ReadEmbeddingsFromAerospike {
//
//    private static final Logger LOG = LoggerFactory.getLogger(FetchImageUrls.class);
//    private final String host;
//    private final int port;
//
//    public ReadEmbeddingsFromAerospike(String host, int port) {
//        this.host = host;
//        this.port = port;
//    }
//
//    public void dump(String inputFile, String outputFile) {
//
//        List<PidObj> pids = new ArrayList<>();
////        FileProcessor.eachLine(inputFile, line -> pids.add(PidObj.create(line.split("\t"))));
//
//        AerospikeClient client = new AerospikeClient(host, port);
//
//        pids = ImmutableList.of(new PidObj("TSHE6XVRNCS58EHW", "t_shirt", "/image/t-shirt/e/h/w/wrts4411navy-wrangler-m-original-imae7gyqatsxca3c.jpeg"));
//
//        pids.stream()
//                .forEach(pidObj -> {
//                    String baseUrl = pidObj.imageUrl;
//                    Key key = new Key("deepthought", "triplet-alexnet-shallow-0001", baseUrl);
//                    Record record = client.get(null, key);
//                    if(record == null) {
//                        System.out.println("No data in aerospike for " + pidObj.pid);
//                    } else {
//                        List<byte[]> list = (List<byte[]>) record.getList("fv");
//                        System.out.println(list.toArray(new byte[0][]));
//                        for (Object o : list) {
//                            byte[] bytes = (byte[]) o;
//                            ByteArrayInputStream bas = new ByteArrayInputStream(bytes);
//                            DataInputStream ds = new DataInputStream(bas);
//                            float[] fArr = new float[bytes.length / 4];  // 4 bytes per float
//                            for (int i = 0; i < fArr.length; i++)
//                            {
//                                try {
//                                    fArr[i] = ds.readFloat();
//                                } catch (IOException e) {
//                                    LOG.error("error decoding value : " + pidObj+ " " + record, e);
//                                }
//                            }
//                            System.out.println(Arrays.toString(fArr));
//                        }
//                    }
//
//                });
//
//        client.close();
//
//    }
//
//
//    public static void main(String[] args) {
//        if(args.length == 0) {
//            args = new String[]{"data/imageUrl.2oq", "data/imageEmb.2oq"};
//        }
//
//        ReadEmbeddingsFromAerospike fetcher = new ReadEmbeddingsFromAerospike("10.32.85.2" , 3000);
//        fetcher.dump(args[0], args[1]);
//    }
//
//}
