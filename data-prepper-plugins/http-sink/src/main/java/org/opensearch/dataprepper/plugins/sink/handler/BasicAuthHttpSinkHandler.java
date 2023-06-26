package org.opensearch.dataprepper.plugins.sink.handler;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Optional;

public class BasicAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {
    @Override
    public HttpAuthOptions authenticate(final HttpSinkConfiguration sinkConfiguration, final UrlConfigurationOption urlConfigurationOption, final HttpAuthOptions httpAuthOptions) throws Exception {
        // TODO: validate username/password exist
        String username = sinkConfiguration.getAuthentication().getPluginSettings().get("username").toString();
        String password = sinkConfiguration.getAuthentication().getPluginSettings().get("password").toString();
        CloseableHttpClient httpclient = null;

        URL url = new URL(urlConfigurationOption.getUrl());

        final HttpHost targetHost = new HttpHost(url.toURI().getScheme(), url.getHost(), url.getPort());
        final BasicCredentialsProvider provider = new BasicCredentialsProvider();
        AuthScope authScope = new AuthScope(targetHost);
        provider.setCredentials(authScope, new UsernamePasswordCredentials(username, password.toCharArray()));
        if(sinkConfiguration.isInsecure()){
            httpclient = HttpClients.custom()
                    .setDefaultCredentialsProvider(provider)
                    .build();

        }else{
            httpclient = HttpClients.custom()
                    .setConnectionManager(httpAuthOptions.getHttpClientConnectionManager())
                    .setDefaultCredentialsProvider(provider)
                    .build();
        }
        httpAuthOptions.setCloseableHttpClient(httpclient);
        return httpAuthOptions;
    }
}
