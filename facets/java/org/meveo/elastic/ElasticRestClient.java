package org.meveo.elastic;

import java.io.IOException;
import java.net.URI;
import java.util.function.Function;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

public class ElasticRestClient {
    private CloseableHttpClient client;
    private String baseUri;

    public ElasticRestClient(String host, int port, String userName, String password) {
        CredentialsProvider provider = new BasicCredentialsProvider();
        Credentials credentials =  new UsernamePasswordCredentials(userName, password);
        provider.setCredentials(AuthScope.ANY, credentials);

        this.client = HttpClientBuilder.create()
            .setDefaultCredentialsProvider(provider)
            .build();

        this.baseUri = host + ":" + port;
    }

    public void close() {
        try {
            this.client.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public int head(String relativeTargetFormat, Object... args) {
        var httpHead = new HttpHead(baseUri + String.format(relativeTargetFormat, args));
        return this.execute(httpHead, response -> response.getStatusLine().getStatusCode());
    }

    public void setBody(HttpEntityEnclosingRequestBase request, String body) {
        request.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
    }

    public HttpGetWithBody get(String relativeTargetFormat, Object... args) {
        return new HttpGetWithBody(baseUri + String.format(relativeTargetFormat, args));
    }

    public HttpPost post(String relativeTargetFormat, Object... args) {
        return new HttpPost(baseUri + String.format(relativeTargetFormat, args));
    }

    public HttpPut put(String relativeTargetFormat, Object... args) {
        return new HttpPut(baseUri + String.format(relativeTargetFormat, args));
    }

    public <T> T execute(HttpRequestBase request, Function<CloseableHttpResponse, T> handler) {
        try {
            try (var response = this.client.execute(request)) {
                if (handler == null) {
                    return null;
                }
                return handler.apply(response);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }
    
    private static class HttpGetWithBody extends HttpEntityEnclosingRequestBase {
        public HttpGetWithBody (String uri) {
            super();
            setURI(URI.create(uri));
        }

        @Override
        public String getMethod() {
            return "GET";
        }
    } 
}
