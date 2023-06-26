package org.opensearch.dataprepper.plugins.sink.handler;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

public class BasicAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    private final HttpSinkConfiguration sinkConfiguration;

    public BasicAuthHttpSinkHandler(final HttpSinkConfiguration sinkConfiguration){
        this.sinkConfiguration = sinkConfiguration;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions  authOptions) {
        // TODO: validate username/password exist
        String username = sinkConfiguration.getAuthentication().getPluginSettings().get("username").toString();
        String password = sinkConfiguration.getAuthentication().getPluginSettings().get("password").toString();
        CloseableHttpClient httpclient = null;
        final HttpHost targetHost;
        URL url = null;
        try {
            url = new URL(authOptions.getUrl());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        try {
            targetHost = new HttpHost(url.toURI().getScheme(), url.getHost(), url.getPort());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        final BasicCredentialsProvider provider = new BasicCredentialsProvider();
        AuthScope authScope = new AuthScope(targetHost);
        provider.setCredentials(authScope, new UsernamePasswordCredentials(username, password.toCharArray()));
        if(sinkConfiguration.isInsecure()){
            httpclient = HttpClients.custom()
                    .setDefaultCredentialsProvider(provider)
                    .build();

        }else{
            httpclient = HttpClients.custom()
                    .setConnectionManager(authOptions.getHttpClientConnectionManager())
                    .setDefaultCredentialsProvider(provider)
                    .build();
        }
        authOptions.setCloseableHttpClient(httpclient);
        return authOptions;
    }
}
