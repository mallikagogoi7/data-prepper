/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.QueryParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;

import java.util.List;

/**
 * Reference for Open Search Configuration
 */
public class OpenSearchSourceConfiguration {

    @JsonProperty("max_retries")
    private Integer maxRetries;

    @NotNull
    @JsonProperty("hosts")
    private List<String> hosts;

    @JsonProperty("username")
    private String username;

    @JsonProperty("password")
    private String password;

    @JsonProperty("connection")
    private ConnectionConfiguration connectionConfiguration;

    @JsonProperty("indices")
    private IndexParametersConfiguration indexParametersConfiguration;

    @JsonProperty("aws")
    private AwsAuthenticationConfiguration awsAuthenticationOptions;

    @JsonProperty("scheduling")
    private SchedulingParameterConfiguration schedulingParameterConfiguration;

    @JsonProperty("query")
    private QueryParameterConfiguration queryParameterConfiguration;

    @JsonProperty("search_options")
    private SearchConfiguration searchConfiguration;

    private List<IndicesRecord> indicesList;

    public List<IndicesRecord> getIndicesList() { return indicesList;  }

    public void setIndicesList(List<IndicesRecord> indicesList) {  this.indicesList = indicesList;  }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public List<String> getHosts() { return hosts;  }
    public void setHosts(List<String> hosts) { this.hosts = hosts; }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public ConnectionConfiguration getConnectionConfiguration() {
        return connectionConfiguration;
    }

    public IndexParametersConfiguration getIndexParametersConfiguration() {
        return indexParametersConfiguration;
    }

    public AwsAuthenticationConfiguration getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public SchedulingParameterConfiguration getSchedulingParameterConfiguration() {
        return schedulingParameterConfiguration;
    }

    public QueryParameterConfiguration getQueryParameterConfiguration() {
        return queryParameterConfiguration;
    }

    public SearchConfiguration getSearchConfiguration() {
        return searchConfiguration;
    }

}
