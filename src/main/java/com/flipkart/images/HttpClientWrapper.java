package com.flipkart.images;

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
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;

public class HttpClientWrapper {

    private final HttpClient httpclient;
    private int timeout;


    public HttpClientWrapper(String host, String port, int timeout) {
        this.timeout = timeout;
        httpclient = getHttpClient(host, port);
    }

    private HttpClient getHttpClient(String host, String port) {
        final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        HttpHost httpHost = new HttpHost(host, Integer.parseInt(port));
        connectionManager.setMaxPerRoute(new HttpRoute(httpHost), 100);
        connectionManager.setDefaultMaxPerRoute(100);
        connectionManager.setMaxTotal(100);

        return HttpClients.custom().setConnectionManager(connectionManager).build();
    }

    public byte [] get(final String uri, final Map<String, String> header, Integer retry, final boolean handleRedirect) throws IOException {
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
//            httpclient.close();
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


}
