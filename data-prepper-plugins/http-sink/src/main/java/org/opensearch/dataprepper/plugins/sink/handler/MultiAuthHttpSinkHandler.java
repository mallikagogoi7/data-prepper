package org.opensearch.dataprepper.plugins.sink.handler;

import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;

import java.util.Optional;

public interface MultiAuthHttpSinkHandler {
    HttpAuthOptions authenticate(final HttpAuthOptions  authOptions);

}