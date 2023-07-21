/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.opensearch.dataprepper.plugins.sink.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.OAuthRefreshTokenManager;
import org.opensearch.dataprepper.plugins.sink.configuration.BearerTokenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * * This class handles Bearer Token Authentication
 */
public class BearerTokenAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BearerTokenAuthHttpSinkHandler.class);

    public static final String AUTHORIZATION = "Authorization";

    private final HttpClientConnectionManager httpClientConnectionManager;

    private final BearerTokenOptions bearerTokenOptions;

    private final ObjectMapper objectMapper;

    private OAuthRefreshTokenManager oAuthRefreshTokenManager;

    public BearerTokenAuthHttpSinkHandler(final BearerTokenOptions bearerTokenOptions,
                                          final HttpClientConnectionManager httpClientConnectionManager,
                                          final OAuthRefreshTokenManager oAuthRefreshTokenManager){
        this.bearerTokenOptions = bearerTokenOptions;
        this.httpClientConnectionManager = httpClientConnectionManager;
        this.objectMapper = new ObjectMapper();
        this.oAuthRefreshTokenManager = oAuthRefreshTokenManager;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder httpAuthOptionsBuilder) {
        httpAuthOptionsBuilder.getClassicHttpRequestBuilder()
                .addHeader(AUTHORIZATION, oAuthRefreshTokenManager.getRefreshToken(bearerTokenOptions));
        httpAuthOptionsBuilder.setHttpClientBuilder(httpAuthOptionsBuilder.build().getHttpClientBuilder()
                .setConnectionManager(httpClientConnectionManager)
                .addResponseInterceptorLast(new FailedHttpResponseInterceptor(httpAuthOptionsBuilder.getUrl())));
        return httpAuthOptionsBuilder.build();
    }
}
