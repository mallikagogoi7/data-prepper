package org.opensearch.dataprepper.plugins.sink.handler;

import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;

import java.util.Optional;

public class BearerTokenAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {
    @Override
    public HttpAuthOptions authenticate(final HttpSinkConfiguration sinkConfiguration, final UrlConfigurationOption urlConfigurationOption, final HttpAuthOptions httpAuthOptions) throws Exception {
        // if ssl enabled then set connection manager
        return null;
    }
}
