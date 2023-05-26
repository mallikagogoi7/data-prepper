/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.model.ServiceInfo;
import org.opensearch.dataprepper.plugins.source.opensearch.service.ElasticSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.OpenSearchService;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.TimerTask;
import java.util.stream.Collectors;

public class OpenSearchTimerWorker extends TimerTask {

    private OpenSearchService openSearchService;

    private ElasticSearchService elasticSearchService;

    private OpenSearchSourceConfiguration sourceConfig;

    private ServiceInfo serviceInfo;

    private String host;

    private static final String OPEN_SEARCH_DISTRIBUTION = "opensearch";

    public OpenSearchTimerWorker(
                                 final OpenSearchService openSearchService,
                                 final ElasticSearchService elasticSearchService,
                                 final OpenSearchSourceConfiguration sourceConfig,
                                 final ServiceInfo serviceInfo,
                                 final String host) {
        this.openSearchService = openSearchService;
        this.elasticSearchService = elasticSearchService;
        this.sourceConfig = sourceConfig;
        this.serviceInfo = serviceInfo;
        this.host = host;
    }

    @Override
    public void run() {
        for(int jobCount=1 ; sourceConfig.getSchedulingParameterConfiguration().getJobCount() >= jobCount; jobCount++) {
            if (OPEN_SEARCH_DISTRIBUTION.equals(serviceInfo.getDistribution())) {

                openSearchService.processIndexes(serviceInfo.getVersion(),
                        getIndexList(),
                        getUrl(),
                        sourceConfig.getSearchConfiguration().getBatchSize(),
                        sourceConfig.getQueryParameterConfiguration().getFields(),
                        sourceConfig.getSearchConfiguration().getSorting());
            } else {
                elasticSearchService.processIndexes(serviceInfo.getVersion(),
                        getIndexList(),
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

    private String getIndexList()
    {
        List<String> include = sourceConfig.getIndexParametersConfiguration().getInclude();
        List<String> exclude = sourceConfig.getIndexParametersConfiguration().getExclude();
        String includeIndexes = null;
        String excludeIndexes = null;
        StringBuilder indexList = new StringBuilder();
        if(!include.isEmpty())
            includeIndexes = include.stream().collect(Collectors.joining(","));
        if(!exclude.isEmpty())
            excludeIndexes = exclude.stream().collect(Collectors.joining(",-*"));
        indexList.append(includeIndexes);
        indexList.append(",-*"+excludeIndexes);
        return indexList.toString();
    }
}
