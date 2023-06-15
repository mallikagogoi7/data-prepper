package org.opensearch.dataprepper.plugins.sink.configuration;

import org.hibernate.validator.internal.util.Contracts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class UrlConfigurationOptionTest {

    @Test
    void default_worker_test() {
        assertThat(new UrlConfigurationOption().getWorkers(),equalTo(1));
    }

    @Test
    void default_codec_test() {
        assertNull(new UrlConfigurationOption().getCodec());
    }

    @Test
    void default_proxy_test() {
        assertNull(new UrlConfigurationOption().getProxy());
    }

    @Test
    void default_http_method_test() {
        assertThat(new UrlConfigurationOption().getHttpMethod(),equalTo("POST"));
    }

    @Test
    void default_auth_type_test() {
        assertNull(new UrlConfigurationOption().getAuthType());
    }

    @Test
    void default_url_test() {
        assertThat(new UrlConfigurationOption().getUrl(), equalTo(null));
    }
}
