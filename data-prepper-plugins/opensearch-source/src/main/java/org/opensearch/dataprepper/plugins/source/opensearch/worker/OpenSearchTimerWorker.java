/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.model.ServiceInfo;
import org.opensearch.dataprepper.plugins.source.opensearch.service.ElasticSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.OpenSearchService;
import com.linecorp.armeria.client.retry.Backoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.TimerTask;

public class OpenSearchTimerWorker extends TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchTimerWorker.class);
    private OpenSearchService openSearchService;

    private ElasticSearchService elasticSearchService;

    private OpenSearchSourceConfiguration sourceConfig;

    private Buffer buffer;

    private ServiceInfo serviceInfo;

    private String host;
    private int failedAttemptCount;

    private final Backoff standardBackoff;
    private static final String OPEN_SEARCH_DISTRIBUTION = "opensearch";

    public OpenSearchTimerWorker(
            final OpenSearchService openSearchService,
            final ElasticSearchService elasticSearchService,
            final OpenSearchSourceConfiguration sourceConfig,
            final Buffer buffer,
            final ServiceInfo serviceInfo,
            final String host,
            final Backoff standardBackoff) {
        this.openSearchService = openSearchService;
        this.elasticSearchService = elasticSearchService;
        this.sourceConfig = sourceConfig;
        this.serviceInfo = serviceInfo;
        this.host = host;
        this.buffer = buffer;
        this.standardBackoff =standardBackoff;
        failedAttemptCount = 0;

    }

    @Override
    public void run() {
        for (int jobCount = 1; sourceConfig.getSchedulingParameterConfiguration().getJobCount() >= jobCount; jobCount++) {
            try {
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
            }catch(OpenSearchException |ElasticsearchException searchException){
                applyBackoff();
            }
        }
    }


    private void applyBackoff() {
        final long delayMillis = standardBackoff.nextDelayMillis(++failedAttemptCount);
        if (delayMillis < 0) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retries exhausted");
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        LOG.info("Pausing processing for {}.{} seconds due to an error in processing.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException e){
            LOG.error("Thread is interrupted with retry.", e);
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
