/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.handler;

import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.opensearch.dataprepper.plugins.sink.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;

public class BearerTokenAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    private final HttpClientConnectionManager httpClientConnectionManager;

    private final HttpSinkConfiguration sinkConfiguration;

    public BearerTokenAuthHttpSinkHandler(final HttpSinkConfiguration sinkConfiguration, final HttpClientConnectionManager httpClientConnectionManager){
        this.sinkConfiguration = sinkConfiguration;
        this.httpClientConnectionManager = httpClientConnectionManager;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder httpAuthOptionsBuilder) {
        String token = sinkConfiguration.getAuthentication().getPluginSettings().get("token").toString();
        httpAuthOptionsBuilder.getClassicHttpRequestBuilder().addHeader("Authorization", "Bearer " +token);
        httpAuthOptionsBuilder.setHttpClientBuilder(httpAuthOptionsBuilder.build().getHttpClientBuilder()
                .setConnectionManager(httpClientConnectionManager)
                .addResponseInterceptorLast(new FailedHttpResponseInterceptor(httpAuthOptionsBuilder.getUrl())));
        return httpAuthOptionsBuilder.build();
    }
}
