/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.CustomHeaderOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;

import java.util.List;

public class HttpSinkConfiguration {

    private static final int DEFAULT_UPLOAD_RETRIES = 5;

    private static final int DEFAULT_CONNECTION_RETRIES = 5;

    @NotNull
    @JsonProperty("urls")
    private List<UrlConfigurationOption> urlConfigurationOptions;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("http_method")
    private String httpMethod;

    @JsonProperty("proxy")
    private String proxy;

    @JsonProperty("auth_type")
    private String authType;

    private PluginModel authentication;

    @JsonProperty("ssl")
    private boolean ssl;

    @JsonProperty("ssl_certificate_file")
    private String sslCertificateFile;

    @JsonProperty("ssl_key_file")
    private String sslKeyFile;

    @JsonProperty("aws_sigv4")
    private boolean awsSigv4;

    @JsonProperty("buffer_type")
    private BufferTypeOptions bufferType = BufferTypeOptions.INMEMORY;

    @JsonProperty("threshold")
    @NotNull
    private ThresholdOptions thresholdOptions;

    @JsonProperty("max_retries")
    private int maxUploadRetries = DEFAULT_UPLOAD_RETRIES;

    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("custom_header")
    private CustomHeaderOptions customHeaderOptions;

    private int maxConnectionRetries = DEFAULT_CONNECTION_RETRIES;

    public List<UrlConfigurationOption> getUrlConfigurationOptions() {
        return urlConfigurationOptions;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public String getProxy() {
        return proxy;
    }

    public String getAuthType() {
        return authType;
    }

    public PluginModel getAuthentication() {
        return authentication;
    }

    public boolean isSsl() {
        return ssl;
    }

    public String getSslCertificateFile() {
        return sslCertificateFile;
    }

    public String getSslKeyFile() {
        return sslKeyFile;
    }

    public boolean isAwsSigv4() {
        return awsSigv4;
    }

    public BufferTypeOptions getBufferType() {
        return bufferType;
    }

    public ThresholdOptions getThresholdOptions() {
        return thresholdOptions;
    }

    public int getMaxUploadRetries() {
        return maxUploadRetries;
    }

    public CustomHeaderOptions getCustomHeaderOptions() {
        return customHeaderOptions;
    }

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public int getMaxConnectionRetries() {
        return maxConnectionRetries;
    }
}
