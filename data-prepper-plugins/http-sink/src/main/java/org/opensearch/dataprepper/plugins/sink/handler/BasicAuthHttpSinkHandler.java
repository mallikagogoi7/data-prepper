package org.opensearch.dataprepper.plugins.sink.handler;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.dataprepper.plugins.sink.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.util.HttpSinkUtil;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

public class BasicAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    private final HttpSinkConfiguration sinkConfiguration;

    private final HttpClientConnectionManager httpClientConnectionManager;

    public BasicAuthHttpSinkHandler(final HttpSinkConfiguration sinkConfiguration,
                                    final HttpClientConnectionManager httpClientConnectionManager){
        this.sinkConfiguration = sinkConfiguration;
        this.httpClientConnectionManager = httpClientConnectionManager;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder  httpAuthOptionsBuilder) {
        // TODO: validate username/password exist
        String username = sinkConfiguration.getAuthentication().getPluginSettings().get("username").toString();
        String password = sinkConfiguration.getAuthentication().getPluginSettings().get("password").toString();
        final HttpHost targetHost = HttpSinkUtil.getHttpHostByURL(HttpSinkUtil.getURLByUrlString(httpAuthOptionsBuilder.getUrl()));
        final BasicCredentialsProvider provider = new BasicCredentialsProvider();
        AuthScope authScope = new AuthScope(targetHost);
        provider.setCredentials(authScope, new UsernamePasswordCredentials(username, password.toCharArray()));
        httpAuthOptionsBuilder.setHttpClientBuilder(httpAuthOptionsBuilder.build().getHttpClientBuilder()
                .setConnectionManager(httpClientConnectionManager)
                .addResponseInterceptorLast(new FailedHttpResponseInterceptor(httpAuthOptionsBuilder.getUrl()))
                .setDefaultCredentialsProvider(provider));
        return httpAuthOptionsBuilder.build();
    }
}
