/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.TlsConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.dataprepper.plugins.sink.AuthHandler;
import org.opensearch.dataprepper.plugins.sink.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

public class HttpSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkService.class);

    private final Codec codec;

    private final HttpSinkConfiguration httpSinkConf;

    private final BufferFactory bufferFactory;

    private final AuthHandler authHandler;

    private final static String AUTH_HTTP_BASIC = "http_basic";

    private final static String AUTH_BEARER_TOKEN = "bearer_token";

    private final static String HTTP_METHOD_DEFAULT = "POST";

    private final static String HTTP_METHOD_PUT = "PUT";

    public HttpSinkService(final Codec codec, final HttpSinkConfiguration httpSinkConf, final BufferFactory bufferFactory, final AuthHandler authHandler){
        this.codec= codec;
        this.httpSinkConf = httpSinkConf;
        this.bufferFactory = bufferFactory;
        this.authHandler = authHandler;
    }

    public String getHttpMethodFromConf(UrlConfigurationOption urlConfOption) throws Exception {
        String httpMethod = urlConfOption.getHttpMethod()!=null? urlConfOption.getHttpMethod(): httpSinkConf.getHttpMethod();
        if(httpMethod == null) {
            httpMethod = HTTP_METHOD_DEFAULT;
        }
        if(httpMethod != HTTP_METHOD_DEFAULT || httpMethod != HTTP_METHOD_PUT){
            throw new Exception("Provide POST/PUT Http Method");
        }
        return httpMethod;
    }

    public String getAuthTypeFromConf(UrlConfigurationOption urlConfOption) {
        return urlConfOption.getAuthType()!=null? urlConfOption.getAuthType(): httpSinkConf.getAuthType();
    }

    public String getProxyFromConf(UrlConfigurationOption urlConfOption) {
        return urlConfOption.getProxy()!=null? urlConfOption.getProxy(): httpSinkConf.getProxy();
    }

    public BasicCredentialsProvider getBasicCredProvider(final String username, final String password, final String request) throws MalformedURLException, URISyntaxException {
        URL url = new URL(request);

        final HttpHost targetHost = new HttpHost(url.toURI().getScheme(), url.getHost(), url.getPort());
        final BasicCredentialsProvider provider = new BasicCredentialsProvider();
        AuthScope authScope = new AuthScope(targetHost);
        provider.setCredentials(authScope, new UsernamePasswordCredentials(username, password.toCharArray()));

        return provider;
    }

    public void getBearerTokenProvider(){

    }

    public void sendHttpRequest(String encodedEvent) throws IOException, InterruptedException {
        for(UrlConfigurationOption urlConfOption: httpSinkConf.getUrlConfigurationOptions()) {
            if(getProxyFromConf(urlConfOption) == null) {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    String requestBody = objectMapper
                            .writeValueAsString(encodedEvent);

                    HttpClientContext clientContext = HttpClientContext.create();

                    switch (getHttpMethodFromConf(urlConfOption)) {
                        case "POST":
                            ClassicHttpRequest httpPost = null;
                            CredentialsProvider provider= null;

                            httpPost = ClassicRequestBuilder.post(urlConfOption.getUrl())
                                    .setEntity(requestBody)
                                    .build();

                            if (getAuthTypeFromConf(urlConfOption).equals(AUTH_HTTP_BASIC)) {
                                if (httpSinkConf.getAuthentication().getPluginName().equals(AUTH_HTTP_BASIC)) {
                                    String username = httpSinkConf.getAuthentication().getPluginSettings().get("username").toString();
                                    String password = httpSinkConf.getAuthentication().getPluginSettings().get("password").toString();
                                    provider = getBasicCredProvider(username,password, urlConfOption.getUrl());
                                }
                            }

                            if (getAuthTypeFromConf(urlConfOption).equals(AUTH_BEARER_TOKEN)) {
                                if (httpSinkConf.getAuthentication().getPluginName().equals(AUTH_BEARER_TOKEN)) {
                                    String token = httpSinkConf.getAuthentication().getPluginSettings().get("token").toString();
                                    httpPost.addHeader("Authorization", "Bearer" +token);
                                }
                            }
                            CloseableHttpClient httpclient = HttpClients.custom()
                                    .setConnectionManager(authHandler.sslConnection())
                                    .setDefaultCredentialsProvider(provider)
                                    .build();

                            LOG.info("  [MG]--> Request: " + httpPost);
                            httpclient.execute(httpPost, clientContext, response -> {
                                System.out.println(response.getCode() + " " + response.getReasonPhrase());
                                final HttpEntity entity = response.getEntity();
                                EntityUtils.consume(entity);
                                SSLSession sslSession = clientContext.getSSLSession();
                                if (sslSession != null) {
                                    LOG.info(" [MG]--> SSL Protocol : " + sslSession.getProtocol());
                                    LOG.info(" [MG]--> SSL Cipher Suit : " + sslSession.getCipherSuite());
                                }
                                return null;
                            });
                            break;
                    }
                } catch (Exception e) {

                }
            }
        }
    }
}
