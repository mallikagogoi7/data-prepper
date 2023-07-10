/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.net.MalformedURLException;
import java.net.URL;

public class UrlConfigurationOption {

    private static final int DEFAULT_WORKERS = 1;

    private static final String HTTPS = "https";

    private static final String AWS_HOST_CHECK1 = "amazonaws.com";

    private static final String AWS_HOST_CHECK2 = "api.aws";


    @NotNull
    @JsonProperty("url")
    private String url;

    @JsonProperty("workers")
    private Integer workers = DEFAULT_WORKERS;

    @JsonProperty("proxy")
    private String proxy;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("http_method")
    private HTTPMethodOptions httpMethod;

    @JsonProperty("auth_type")
    private AuthTypeOptions authType;

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

    public HTTPMethodOptions getHttpMethod() {
        return httpMethod;
    }

    public AuthTypeOptions getAuthType() {
        return authType;
    }

    public boolean isValidAWSUrl() throws MalformedURLException {
        URL parsedUrl = new URL(url);
        if(parsedUrl.getProtocol().equals(HTTPS) && (parsedUrl.getHost().contains(AWS_HOST_CHECK1) ||parsedUrl.getHost().contains(AWS_HOST_CHECK2))){
            return true;
        }
        return false;
    }

}
