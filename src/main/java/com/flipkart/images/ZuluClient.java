package com.flipkart.images;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.time.StopWatch;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZuluClient {

    private static final Logger LOG = LoggerFactory.getLogger(ZuluClient.class);


    private final String host;
    private final String port;
    private final HttpClientWrapper httpClient;
    private int timeout;
    private static final ObjectMapper mapper = new ObjectMapper();
    private Map<String, String> headers;

    public ZuluClient(String host, String port, int timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        headers = new HashMap<>();
        headers.put("z-clientid", "w3.sherlock");
        headers.put("z-requestid", "request");
        headers.put("z-timestamp", "00:00:00");
        headers.put("Content-Typep", "application/json");
        httpClient = new HttpClientWrapper(host, port, timeout);
    }

    public List<Reponse> getNecessaryResponse(List<String> productIds) throws IOException {

        StopWatch watch = new StopWatch();
        watch.start();
        String baseUrl = "/views?viewNames=discovery_details&entityIds=";
        String entityIds = Joiner.on(",").join(productIds);

        String uri = "http://" + host + ":" + port + baseUrl + entityIds;

        byte[] byteResponse = httpClient.get(uri, headers, 0, false);
        Map responseMap = mapper.readValue(byteResponse, Map.class);

        List<Reponse> reponses = new ArrayList<>();
        List<Map> entities = (List<Map>) responseMap.get("entityViews");
        for (Map entity : entities) {
            Reponse reponse = new Reponse();
            reponse.setProductId((String) entity.get("entityId"));
            Map<String, Object> view = (Map<String, Object>) entity.get("view");
            reponse.setVertical((String)view.get("vertical"));
            reponse.setImageUrl((String) view.get("primary_image_url"));
            reponses.add(reponse);
        }

        watch.stop();
        LOG.info("fetching data for fsns : " + productIds + " in " +  watch.getTime() + " ms");
        return reponses;

    }

    public static class Reponse {

        private String productId = "NA";
        private String vertical = "NA";
        private String imageUrl = "NA";

        public void setProductId(String productId) {
            if (productId != null) {
                this.productId = productId;
            }
        }

        public void setVertical(String vertical) {
            if(vertical != null) {
                this.vertical = vertical;
            }
        }

        public void setImageUrl(String imageUrl) {
            if (imageUrl != null) {
                this.imageUrl = imageUrl;
            }
        }

        public String getProductId() {
            return productId;
        }

        public String getVertical() {
            return vertical;
        }

        public String getImageUrl() {
            return imageUrl;
        }

        @Override
        public String toString() {
            return "Reponse{" +
                    "productId='" + productId + '\'' +
                    ", vertical='" + vertical + '\'' +
                    ", imageUrl='" + imageUrl + '\'' +
                    '}';
        }
    }


    public static void main(String[] args) throws IOException {

        ZuluClient client = new ZuluClient("10.47.1.8", "31200", 1000);
        List<Reponse> resp = client.getNecessaryResponse(ImmutableList.of("TSHEQ54WRZ3R3WRN"));
        System.out.println(resp);

    }

}
