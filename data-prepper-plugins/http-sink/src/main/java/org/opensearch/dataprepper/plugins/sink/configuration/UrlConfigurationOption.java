/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;

public class UrlConfigurationOption {

    @NotNull
    @JsonProperty("url")
    private String url;

    @JsonProperty("workers")
    private Integer workers;

    @JsonProperty("proxy")
    private String proxy;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("http_method")
    private String httpMethod;

    @JsonProperty("auth_type")
    private String authType;

    public String getUrl() {
        return url;
    }

    public Integer getWorkers() {
        return workers;
    }

    public String getProxy() {
        return proxy;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public String getAuthType() {
        return authType;
    }

}
