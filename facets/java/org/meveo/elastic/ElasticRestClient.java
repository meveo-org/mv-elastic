package org.meveo.elastic;

import java.io.IOException;
import java.net.URI;
import java.util.function.Function;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
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
import org.meveo.admin.exception.BusinessException;

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

    public int delete(String relativeTargetFormat, Object... args) {
        var httpHead = new HttpDelete(baseUri + String.format(relativeTargetFormat, args));
        return this.execute(httpHead, response -> response.getStatusLine().getStatusCode());
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
    
    public <T> T execute(HttpRequestBase request, ResultHandler<T> handler) {
        try {
            try (var response = this.client.execute(request)) {
                if (handler == null) {
                    return null;
                }
                return handler.apply(response);
            }
        } catch (Exception e) {
        	// TODO Log error ?
        	return null;
        }
    }
    
    public <T> T execute(HttpRequestBase request, ResultHandler<T> handler, ErrorConsumer errorHandler) {
        try {
            try (var response = this.client.execute(request)) {
                if (handler == null) {
                    return null;
                }
                return handler.apply(response);
            }
        } catch (Exception e) {
        	errorHandler.handle(e);
        	return null;
        }
    }

    public <T> T execute(HttpRequestBase request, ResultHandler<T> handler, ErrorHandler errorHandler) throws BusinessException {
        try {
            try (var response = this.client.execute(request)) {
                if (handler == null) {
                    return null;
                }
                return handler.apply(response);
            }
        } catch (Exception e) {
        	throw errorHandler.handle(e);
        }
    }
    
    public <T> T execute(HttpRequestBase request, ResultHandler<T> handler, String errorMessage) throws BusinessException {
    	return execute(request, handler, handleError(errorMessage));
    }
    
    @FunctionalInterface
    public static interface ResultHandler<T> {
    	T apply(CloseableHttpResponse e) throws Exception;
    }
    
    @FunctionalInterface
    public static interface ErrorHandler {
    	BusinessException handle(Exception e);
    }
    
    @FunctionalInterface
    public static interface ErrorConsumer {
    	void handle(Exception e);
    }
    
    public static ErrorHandler handleError(String message) {
    	return new ErrorHandler() {
			@Override
			public BusinessException handle(Exception e) {
				return new BusinessException(message, e);
			}
    	};
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
