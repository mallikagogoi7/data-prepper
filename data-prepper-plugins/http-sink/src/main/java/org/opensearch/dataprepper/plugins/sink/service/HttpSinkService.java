/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.plugins.sink.AuthHandler;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Base64;

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
        if(httpMethod.equals(HTTP_METHOD_DEFAULT) || httpMethod.equals(HTTP_METHOD_PUT)){
            return httpMethod;
        }
        throw new Exception("Http Method should to POST/PUT");
    }

    public String getAuthTypeFromConf(UrlConfigurationOption urlConfOption) {
        String auth = urlConfOption.getAuthType()!=null? urlConfOption.getAuthType(): httpSinkConf.getAuthType();
        return auth;
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

    private static final String getBasicAuthenticationHeader(String username, String password) {
        String valueToEncode = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(valueToEncode.getBytes());
    }

    public void getBearerTokenProvider(){

    }

    public void sendHttpRequest(String encodedEvent) throws IOException, InterruptedException, URISyntaxException {
        for(UrlConfigurationOption urlConfOption: httpSinkConf.getUrlConfigurationOptions()) {
            if(getProxyFromConf(urlConfOption) == null) {
                executeHttpClient(encodedEvent, urlConfOption);
            }else {
                executeHttpClientWithProxy(encodedEvent, urlConfOption);
            }
        }
    }

    private void executeHttpClient(String encodedEvent, UrlConfigurationOption urlConfOption){
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String requestBody = objectMapper
                    .writeValueAsString(encodedEvent);

            HttpClientContext clientContext = HttpClientContext.create();
            switch (getHttpMethodFromConf(urlConfOption)) {
                case "POST":
                    ClassicHttpRequest httpPost = null;

                    httpPost = ClassicRequestBuilder.post(urlConfOption.getUrl())
                            .setEntity(requestBody)
                            .build();
                    String authType = getAuthTypeFromConf(urlConfOption);

                    if (authType!= null &&  authType.equals(AUTH_HTTP_BASIC)) {
                        if (httpSinkConf.getAuthentication().getPluginName().equals(AUTH_HTTP_BASIC)) {
                            String username = httpSinkConf.getAuthentication().getPluginSettings().get("username").toString();
                            String password = httpSinkConf.getAuthentication().getPluginSettings().get("password").toString();
                            String auth = getBasicAuthenticationHeader(username,password);
                            httpPost.addHeader("Authorization ", auth);
                        }
                    }
                    else if (authType!= null && authType.equals(AUTH_BEARER_TOKEN)) {
                        if (httpSinkConf.getAuthentication().getPluginName().equals(AUTH_BEARER_TOKEN)) {
                            String token = httpSinkConf.getAuthentication().getPluginSettings().get("token").toString();
                            httpPost.addHeader("Authorization", "Bearer " +token);
                        }
                    }
                    httpPost.addHeader("Content-Type", "application/json");
                    CloseableHttpClient httpclient = HttpClients.custom()
                            .setConnectionManager(authHandler.sslConnection())
                            .build();

                    LOG.info("  [MG]--> Request: " + httpPost);
                    httpclient.execute(httpPost, clientContext, response -> {
                        LOG.info("Http Response code : " + response.getCode());
                        final HttpEntity entity = response.getEntity();
                        EntityUtils.consume(entity);
                        SSLSession sslSession = clientContext.getSSLSession();
                        if (sslSession != null) {
                            LOG.info(" [MG]--> SSL Protocol : " + sslSession.getProtocol());
                            LOG.info(" [MG]--> SSL Cipher Suit : " + sslSession.getCipherSuite());
                        }
                        LOG.info("Request Body: " +response.getEntity());
                        return null;
                    });
                    break;
            }
        } catch (Exception e) {
            LOG.error("Error while calling Http Client " + e.getMessage());
        }
    }

    private void executeHttpClientWithProxy(String encodedEvent, UrlConfigurationOption urlConfOption) throws MalformedURLException, URISyntaxException, JsonProcessingException {
        LOG.info("________________ Execute Proxy _______________________");
        URL targetUrl = new URL(urlConfOption.getUrl());
        final HttpHost targetHost = new HttpHost(targetUrl.toURI().getScheme(), targetUrl.getHost(), targetUrl.getPort());

        URL proxyUrl = new URL(urlConfOption.getProxy());
        final HttpHost proxyHost = new HttpHost(proxyUrl.toURI().getScheme(), proxyUrl.getHost(), proxyUrl.getPort());

        ObjectMapper objectMapper = new ObjectMapper();
        String requestBody = objectMapper
                .writeValueAsString(encodedEvent);

        try( final CloseableHttpClient httpClient = HttpClients.custom()
                .setProxy(proxyHost)
                .build()) {
          final ClassicHttpRequest httpPostRequest = ClassicRequestBuilder.post(urlConfOption.getUrl())
                  .setEntity(requestBody)
                  .build();

            httpClient.execute(targetHost, httpPostRequest, response -> {
                LOG.info("inside execute");
                LOG.info("httpPostRequest : " + httpPostRequest);
                LOG.info("Http Response code : " + response.getCode());
                final HttpEntity entity = response.getEntity();
                EntityUtils.consume(entity);
                return null;
            });

        } catch (Exception e) {
            LOG.info("Exception in proxy: "+ e.getMessage());
        }
    }

    private void executeHC(){
    }
}
