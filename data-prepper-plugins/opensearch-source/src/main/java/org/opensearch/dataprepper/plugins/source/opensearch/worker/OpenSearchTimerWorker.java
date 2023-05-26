/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.model.ServiceInfo;
import org.opensearch.dataprepper.plugins.source.opensearch.service.ElasticSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.OpenSearchService;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.TimerTask;

public class OpenSearchTimerWorker extends TimerTask {

    private OpenSearchService openSearchService;

    private ElasticSearchService elasticSearchService;

    private OpenSearchSourceConfiguration sourceConfig;

    private Buffer buffer;

    private ServiceInfo serviceInfo;

    private String host;

    private static final String OPEN_SEARCH_DISTRIBUTION = "opensearch";

    public OpenSearchTimerWorker(
            final OpenSearchService openSearchService,
            final ElasticSearchService elasticSearchService,
            final OpenSearchSourceConfiguration sourceConfig,
            final Buffer buffer,
            final ServiceInfo serviceInfo,
            final String host) {
        this.openSearchService = openSearchService;
        this.elasticSearchService = elasticSearchService;
        this.sourceConfig = sourceConfig;
        this.serviceInfo = serviceInfo;
        this.host = host;
        this.buffer = buffer;

    }

    @Override
    public void run() {
        for (int jobCount = 1; sourceConfig.getSchedulingParameterConfiguration().getJobCount() >= jobCount; jobCount++) {
            if (OPEN_SEARCH_DISTRIBUTION.equals(serviceInfo.getDistribution())) {
                openSearchService.processIndexes(serviceInfo.getVersion(),
                        sourceConfig.getIndexParametersConfiguration(),
                        getUrl(),
                        sourceConfig.getSearchConfiguration().getBatchSize(), sourceConfig.getMaxRetries(),
                        sourceConfig.getQueryParameterConfiguration() != null ?
                                sourceConfig.getQueryParameterConfiguration().getFields() : null,
                        sourceConfig.getSearchConfiguration() != null ?
                                sourceConfig.getSearchConfiguration().getSorting() : null
                        , buffer);
            } else {
                elasticSearchService.processIndexes(serviceInfo.getVersion(),
                        sourceConfig.getIndexParametersConfiguration(),
                        getUrl(),
                        sourceConfig.getSearchConfiguration().getBatchSize(),
                        sourceConfig.getQueryParameterConfiguration().getFields(),
                        sourceConfig.getSearchConfiguration().getSorting());
            }
        }
    }

    private URL getUrl() {
        URL url = null;
        try {
            url = new URL(host);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        return url;
    }

}
