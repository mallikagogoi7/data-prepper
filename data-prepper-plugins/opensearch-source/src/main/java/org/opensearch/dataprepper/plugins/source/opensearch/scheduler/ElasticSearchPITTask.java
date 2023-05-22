/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.scheduler;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearch.service.ElasticSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.OpenSearchSourceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

/**
 * Reference to Schedular for ElasticSearch without Pagination
 */
public class ElasticSearchPITTask extends TimerTask {
    
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchPITTask.class);
    
    private OpenSearchSourceConfiguration openSearchSourceConfiguration = null;

    private ElasticsearchClient esClient = null;

    private Buffer<Record<Event>> buffer = null;

    private ElasticSearchService elasticSearchService = null;

    public ElasticSearchPITTask(OpenSearchSourceConfiguration openSearchSourceConfiguration , Buffer<Record<Event>> buffer , ElasticsearchClient esClient ) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.buffer = buffer;
        elasticSearchService = new ElasticSearchService(esClient);
    }

    @Override
    public void run() {
        int numRuns = 0;
        while (numRuns++ <= openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount()) {
            elasticSearchService.searchPitIndexes(null, openSearchSourceConfiguration, buffer);
        }
    }
}
