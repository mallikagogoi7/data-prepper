/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.scheduler;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearch.service.OpenSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.OpenSearchSourceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

/**
 * Reference to Schedular for Open Search
 */
public class OpenSearchPITTask extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchPITTask.class);
    OpenSearchSourceConfiguration openSearchSourceConfiguration = null;
    OpenSearchClient osClient=null;

    Buffer<Record<Event>> buffer = null;

    OpenSearchService openSearchService = null;

    List<IndicesRecord> indicesList = new ArrayList<>();
    public OpenSearchPITTask(OpenSearchSourceConfiguration openSearchSourceConfiguration , Buffer<Record<Event>> buffer , OpenSearchClient osClient , List<IndicesRecord> indicesList) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.buffer = buffer;
        this.indicesList = indicesList;
        openSearchService = new OpenSearchService(osClient);
    }

    @Override
    public void run() {
        int numRuns = 0;
        while (numRuns++ <= openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount()) {
            try {
                   openSearchService.generatePitId(openSearchSourceConfiguration , buffer, indicesList);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
