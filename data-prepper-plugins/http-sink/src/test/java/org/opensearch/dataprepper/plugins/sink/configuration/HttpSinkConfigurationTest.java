/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.yaml.snakeyaml.Yaml;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HttpSinkConfigurationTest {

    @Test
    void default_worker_test() {
        assertThat(new HttpSinkConfiguration().getWorkers(), CoreMatchers.equalTo(1));
    }

    @Test
    void default_codec_test() {
        assertNull(new HttpSinkConfiguration().getCodec());
    }

    @Test
    void default_proxy_test() {
        assertNull(new HttpSinkConfiguration().getProxy());
    }

    @Test
    void default_http_method_test() {
        assertThat(new HttpSinkConfiguration().getHttpMethod(), CoreMatchers.equalTo("POST"));
    }

    @Test
    void default_auth_type_test() {
        assertNull(new HttpSinkConfiguration().getAuthType());
    }

    @Test
    void get_urls_test() {
        assertThat(new HttpSinkConfiguration().getUrlConfigurationOptions(), equalTo(null));
    }

    @Test
    void get_authentication_test() {
        assertNull(new HttpSinkConfiguration().getAuthentication());
    }

    @Test
    void default_insecure_test() {
        assertThat(new HttpSinkConfiguration().isInsecure(), equalTo(false));
    }

    @Test
    void default_awsSigv4_test() {
        assertThat(new HttpSinkConfiguration().isAwsSigv4(), equalTo(false));
    }

    @Test
    void get_ssl_certificate_file_test() {
        assertNull(new HttpSinkConfiguration().getSslCertificateFile());
    }

    @Test
    void get_ssl_key_file_test() {
        assertNull(new HttpSinkConfiguration().getSslKeyFile());
    }

    @Test
    void default_buffer_type_test() {
        assertThat(new HttpSinkConfiguration().getBufferType(), equalTo(BufferTypeOptions.INMEMORY));
    }

    @Test
    void get_threshold_options_test() {
        assertNull(new HttpSinkConfiguration().getThresholdOptions());
    }

    @Test
    void default_max_upload_retries_test() {
        assertThat(new HttpSinkConfiguration().getMaxUploadRetries(), equalTo(5));
    }

    @Test
    void get_aws_authentication_options_test() {
        assertNull(new HttpSinkConfiguration().getAwsAuthenticationOptions());
    }

    @Test
    void get_custom_header_options_test() {
        assertNull(new HttpSinkConfiguration().getCustomHeaderOptions());
    }
}
