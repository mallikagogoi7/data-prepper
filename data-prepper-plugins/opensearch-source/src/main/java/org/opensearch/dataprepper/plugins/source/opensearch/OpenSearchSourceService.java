/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.model.ServiceInfo;
import org.opensearch.dataprepper.plugins.source.opensearch.service.ElasticSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.HostsService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.OpenSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.OpenSearchTimerWorker;

import java.time.Duration;
import java.util.Timer;

/**
 *  Service that will call both worker classes
 */
public class OpenSearchSourceService {

    private final OpenSearchService openSearchService;

    private final ElasticSearchService elasticSearchService;

    private final HostsService hostsService;

    private final OpenSearchSourceConfiguration sourceConfig;

    private final Timer timer = new Timer();

    private final Buffer<Record<Event>> buffer;

    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();
    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();
    static final double JITTER_RATE = 0.20;

    public OpenSearchSourceService(final OpenSearchSourceConfiguration sourceConfig,
                                   final HostsService hostsService,
                                   final OpenSearchService openSearchService,
                                   final ElasticSearchService elasticSearchService,
                                   final Buffer<Record<Event>> buffer){
        this.sourceConfig = sourceConfig;
        this.hostsService = hostsService;
        this.openSearchService = openSearchService;
        this.elasticSearchService =elasticSearchService;
        this.buffer = buffer;
    }

    public void processHosts(){
        sourceConfig.getHosts().forEach(host ->{
            final ServiceInfo serviceInfo = hostsService.findServiceDetailsByUrl(host);
            IndexParametersConfiguration index = sourceConfig.getIndexParametersConfiguration();
            final Backoff backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                    .withMaxAttempts(Integer.MAX_VALUE);
            timer.scheduleAtFixedRate(new OpenSearchTimerWorker(openSearchService,elasticSearchService,
                            sourceConfig,buffer,serviceInfo,host, backoff),
                    sourceConfig.getSchedulingParameterConfiguration().getStartTime().getSecond(),
                    sourceConfig.getSchedulingParameterConfiguration().getRate().toMillis());
        });
    }
    public void stop(){
        timer.cancel();
    }
}
