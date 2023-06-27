package org.opensearch.dataprepper.plugins.sink.handler;

import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;

import java.util.Optional;

public class BearerTokenAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    private final HttpClientConnectionManager httpClientConnectionManager;

    public BearerTokenAuthHttpSinkHandler(final HttpClientConnectionManager httpClientConnectionManager){
        this.httpClientConnectionManager = httpClientConnectionManager;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions httpAuthOptions) {
        // if ssl enabled then set connection manager
        return null;
    }
}
