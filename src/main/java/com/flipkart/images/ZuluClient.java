package com.flipkart.images;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.time.StopWatch;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
    private int timeout;
    private static final ObjectMapper mapper = new ObjectMapper();
    private Map<String, String> headers;

    public ZuluClient(String host, String port, int timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        HttpClient client = HttpClients.createDefault();
        headers = new HashMap<>();
        headers.put("z-clientid", "w3.sherlock");
        headers.put("z-requestid", "request");
        headers.put("z-timestamp", "00:00:00");
        headers.put("Content-Typep", "application/json");

    }

    public List<Reponse> getNecessaryResponse(List<String> productIds) throws IOException {

        StopWatch watch = new StopWatch();
        watch.start();
        String baseUrl = "/views?viewNames=discovery_details&entityIds=";
        String entityIds = Joiner.on(",").join(productIds);

        String uri = "http://" + host + ":" + port + baseUrl + entityIds;

        byte[] byteResponse = get(uri, headers, 0, false);
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


    private byte [] get(final String uri, final Map<String, String> header, Integer retry, final boolean handleRedirect) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        byte [] response = null;
        try {
            HttpGet httpGet = new HttpGet(uri);
            if(header != null) {
                for(Map.Entry e : header.entrySet())
                    httpGet.addHeader((String) e.getKey(), (String) e.getValue());
            }
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(timeout)
                    .setConnectionRequestTimeout(timeout)
                    .setSocketTimeout(timeout)
                    .setRedirectsEnabled(true)
                    .build();

            httpGet.setConfig(requestConfig);
            ResponseHandler<byte []> responseHandler = new ResponseHandler<byte[]>() {
                @Override
                public byte[] handleResponse(
                        final HttpResponse response) throws IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 400) {
                        if(status == 302 && handleRedirect) {
                            HttpEntity entity = response.getEntity();
                            return EntityUtils.toByteArray(entity);
                        } else {
                            HttpEntity entity = response.getEntity();
                            return entity != null ? EntityUtils.toByteArray(entity) : null;
                        }
                    } else {
                        throw new ClientProtocolException("GET: UNEXPECTED RESPONSE STATUS: " + status + " for URI : " + uri);
                    }
                }
            };
            response = httpclient.execute(httpGet, responseHandler);
            httpclient.close();
        } catch (SocketTimeoutException e) {
            if(retry>0)
                return get(uri, header, --retry, handleRedirect);
            else {
                throw new SocketTimeoutException("[ERROR] Socket Timeout Exception for uri " + uri);
            }
        } catch (ConnectTimeoutException e) {
            if(retry>0)
                return get(uri, header, --retry, handleRedirect);
            else {
                throw new ConnectTimeoutException("[ERROR] Connect Timeout Exception for uri " + uri);
            }
        } catch (ClientProtocolException e) {
            if(retry>0)
                return get(uri, header, --retry, handleRedirect);
            else
                throw new ClientProtocolException("[ERROR] Client Protocol Exception for uri: "+ uri, e);
        } catch (IOException e) {
            if(retry>0)
                return get(uri, header, --retry, handleRedirect);
            else
                throw new IOException("[ERROR] IOException  for uri: " + uri, e);
        }
        return response;
    }


    public static void main(String[] args) throws IOException {

        ZuluClient client = new ZuluClient("10.47.1.8", "31200", 1000);
        List<Reponse> resp = client.getNecessaryResponse(ImmutableList.of("TSHEQ54WRZ3R3WRN"));

        System.out.println(resp);


    }

}
