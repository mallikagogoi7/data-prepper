/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.handler;

import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BearerTokenAuthHttpSinkHandlerTest {

    private String urlString = "http://localhost:8080";
    @Test
    public void authenticateTest() {
        HttpAuthOptions.Builder httpAuthOptionsBuilder = new HttpAuthOptions.Builder();
        httpAuthOptionsBuilder.setUrl(urlString);
        httpAuthOptionsBuilder.setHttpClientBuilder(HttpClients.custom());
        httpAuthOptionsBuilder.setHttpClientConnectionManager(PoolingHttpClientConnectionManagerBuilder.create().build());
        httpAuthOptionsBuilder.setClassicHttpRequestBuilder(ClassicRequestBuilder.post());
        Assertions.assertEquals(urlString, new BearerTokenAuthHttpSinkHandler(null, new PoolingHttpClientConnectionManager(),null).authenticate(httpAuthOptionsBuilder).getUrl());
    }
}
