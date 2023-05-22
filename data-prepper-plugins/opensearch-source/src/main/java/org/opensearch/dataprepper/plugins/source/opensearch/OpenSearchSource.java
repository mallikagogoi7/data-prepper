/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.json.simple.JSONObject;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.connection.PrepareConnection;
import org.opensearch.dataprepper.plugins.source.opensearch.service.BackoffService;
import org.opensearch.dataprepper.plugins.source.opensearch.model.SourceInfo;
import org.opensearch.dataprepper.plugins.source.opensearch.service.DataSourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Reference to Source class for OpenSearch
 */
@DataPrepperPlugin(name = "opensearch", pluginType = Source.class, pluginConfigurationType = OpenSearchSourceConfiguration.class)
public class OpenSearchSource implements Source<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSource.class);

    private static final String ELASTIC_SEARCH = "elasticsearch";

    private static final String CLUSTER_HEALTH_STATUS = "status";
    private static final String NODES = "nodes";
    private static final String VERSIONS = "versions";
    private static final String CLUSTER_HEALTH_STATUS_RED = "red";

    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @DataPrepperPluginConstructor
    public OpenSearchSource(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        startProcess(openSearchSourceConfiguration, buffer);
    }

    private void startProcess(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final Buffer<Record<Event>> buffer) {
        try {
            DataSourceService dataSourceService = new DataSourceService(new PrepareConnection());
            SourceInfo sourceInfo = new SourceInfo(dataSourceService.getSourceInfo(openSearchSourceConfiguration));
            LOG.info("Datasource is : {} ", sourceInfo.getDataSource());
            JSONObject healthCheckResponse = dataSourceService.callToHealthCheck(openSearchSourceConfiguration);
            sourceInfo.setHealthStatus(healthCheck(healthCheckResponse));
            sourceInfo.setOsVersion(getOSVersion(healthCheckResponse));
            if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                dataSourceService.setCatIndices(openSearchSourceConfiguration, sourceInfo.getDataSource());
                dataSourceService.versionCheck(openSearchSourceConfiguration, sourceInfo, buffer);
            } else {
                BackoffService backoff = new BackoffService(openSearchSourceConfiguration.getMaxRetries());
                backoff.waitUntilNextTry();
                while (backoff.shouldRetry()) {
                    if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                        backoff.doNotRetry();
                        break;
                    } else {
                        backoff.errorOccured();
                    }
                }

            }
        } catch (Exception e) {
            LOG.error("Error occured while starting the process", e);
        }


    }

    private boolean healthCheck(JSONObject healthCheckResponse) {
        String status = (String) healthCheckResponse.get(CLUSTER_HEALTH_STATUS);
        if (status.equalsIgnoreCase(CLUSTER_HEALTH_STATUS_RED)) {
            return false;
        } else {
            return true;
        }
    }

    private String getOSVersion(JSONObject healthCheckResponse) {
        Map<String, String> nodesMap = ((Map) healthCheckResponse.get(NODES));
        for (Map.Entry<String, String> entry : nodesMap.entrySet()) {
            if (entry.getKey().equals(VERSIONS)) {
                return String.valueOf(entry.getValue());

            }
        }
        return null;
    }

    @Override
    public void stop() {

    }
}
