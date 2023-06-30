/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.handler;

import org.apache.hc.client5.http.io.HttpClientConnectionManager;

public class BearerTokenAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    private final HttpClientConnectionManager httpClientConnectionManager;


    public BearerTokenAuthHttpSinkHandler(final HttpClientConnectionManager httpClientConnectionManager){
        this.httpClientConnectionManager = httpClientConnectionManager;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder httpAuthOptionsBuilder) {
        // if ssl enabled then set connection manager
        return null;
    }
}
