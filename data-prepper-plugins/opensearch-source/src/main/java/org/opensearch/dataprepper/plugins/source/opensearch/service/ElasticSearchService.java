/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ClearScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchClientBuilder;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SortingConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ElasticSearch service related implementation
 */
public class ElasticSearchService {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchService.class);
    private OpenSearchClientBuilder clientBuilder;
    public ElasticSearchService(OpenSearchClientBuilder clientBuilder){
        this.clientBuilder = clientBuilder;
    }
    private static final Integer BATCH_SIZE_VALUE = 1000;

    private static final int ELASTIC_SEARCH_VERSION = 7100;

    private static final int SEARCH_AFTER_SIZE = 100;

    private static final String TIME_VALUE = "24h";

    public void processIndexes(final Integer version,
                               final IndexParametersConfiguration indexParametersConfiguration,
                               final URL url,
                               final Integer batchSize, List<String> fields, List<SortingConfiguration> sorting) {
        ElasticsearchClient client = clientBuilder.createElasticSearchClient(url);
        String indexList= null;
        try {
            indexList = getIndexList(getFilteredIndices(indexParametersConfiguration,client));
        } catch (IOException | ParseException e) {
            LOG.error("Operation failed ",e);
        }
        List<JSONObject> recordsMap = null;
        if(version > ELASTIC_SEARCH_VERSION) {
            if(batchSize > BATCH_SIZE_VALUE) {
                searchIndexesForPagination(client, indexList, fields, sorting, batchSize, 0L);
            }
            else {
                recordsMap = searchIndexes(client, indexList, fields);
            }
        }else{
            recordsMap =scrollIndexesByIndexAndUrl(client, indexList, url.getHost(),batchSize);
            //deleteScrollId(client, recordsMap.get("scroll_id"));
        }
        // push recordsMap to Buffer
    }




    public List<JSONObject> searchIndexes(final ElasticsearchClient client, final String indexList, List<String> fields) {
        List<JSONObject> jsonObjects = null;
        SearchResponse searchResponse = null;
        try {
            String[] queryParam = fields.get(0).split(":");
            searchResponse = client.search(SearchRequest
                    .of(e -> e.index(indexList.toString()).query(q -> q.match(t -> t
                                    .field(queryParam[0].trim())
                                    .query(queryParam[1].trim())))), JSONObject.class);
            LOG.info("Search Response {} ", searchResponse);
            jsonObjects = searchResponse.hits().hits();

        } catch (Exception ex) {
            LOG.error("Error while processing searchIndexes " , ex);
        }
        return jsonObjects;
    }
    public void searchIndexesForPagination(final ElasticsearchClient client, final String indexList, List<String> fields, List<SortingConfiguration> sorting, int currentBatchSize, long currentSearchAfterValue){
        SearchResponse response = getSearchResponse(client, indexList, fields,sorting,currentSearchAfterValue);
        currentBatchSize = currentBatchSize - SEARCH_AFTER_SIZE;
        currentSearchAfterValue = getSortValueFromResponse(response);
        if(currentBatchSize > 0) {
            searchIndexesForPagination(client, indexList, fields,sorting,currentBatchSize, currentSearchAfterValue);
        }
    }

    public List<JSONObject> scrollIndexesByIndexAndUrl(ElasticsearchClient client, final String indexList, final String url, Integer batchSize) {
        List<JSONObject> jsonObjects= null;
        SearchRequest searchRequest = SearchRequest
                .of(e -> e.index(indexList).size(batchSize).scroll(scr -> scr.time(TIME_VALUE)));
        try {
            SearchResponse response = client.search(searchRequest, ObjectNode.class);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return jsonObjects;
    }

    public boolean deleteScrollId(final ElasticsearchClient client, final String scrollId) {
        try {
            ClearScrollRequest scrollRequest = new ClearScrollRequest.Builder().scrollId(scrollId).build();
            ClearScrollResponse clearScrollResponse = client.clearScroll(scrollRequest);
            LOG.info("Delete Scroll ID Response " + clearScrollResponse);
            return clearScrollResponse.succeeded();
        }catch(IOException ex) {
            LOG.error("Error while deleting scrollId ", ex);
        }
        return false;
    }

    private List<SortOptions> getSortOption(List<SortingConfiguration> sorting) {
        List<SortOptions> sortOptionsList = new ArrayList<>();
        for(int sortIndex = 0 ; sortIndex < sorting.size() ; sortIndex++) {

            String sortOrder = sorting.get(sortIndex).getOrder();
            SortOrder order = sortOrder.toLowerCase().equalsIgnoreCase("asc") ? SortOrder.Asc : SortOrder.Desc;
            int finalSortIndex = sortIndex;
            SortOptions sortOptions = new SortOptions.Builder().field(f -> f.field(sorting.get(finalSortIndex).getSortKey()).order(order)).build();
            sortOptionsList.add(sortOptions);
        }
        return sortOptionsList;
    }

    private long getSortValueFromResponse(SearchResponse response)  {
        HitsMetadata hitsMetadata = response.hits();
        int size = hitsMetadata.hits().size();
        long sortValue = 0;
        if(size != 0) {
            try {
                sortValue = ((Hit<Object>) hitsMetadata.hits().get(size - 1)).sort().get(0).longValue();
                LOG.info("extractSortValue : " + sortValue);
            }catch(Exception e){
                LOG.error("Error while getting sort value from search api of elastic search ", e);
            }
        }
        return sortValue;
    }

    public SearchResponse getSearchResponse(final ElasticsearchClient client, final String indexList, List<String> fields, List<SortingConfiguration> sorting, final long searchAfter) {
        SearchResponse response = null;
        SearchRequest searchRequest = null;

        if (fields != null) {
            String[] queryParam = fields.get(0).split(":");

            searchRequest = SearchRequest
                    .of(e -> e.index(indexList.toString()).size(SEARCH_AFTER_SIZE).query(q -> q.match(t -> t
                                    .field(queryParam[0].trim())
                                    .query(queryParam[1].trim()))).searchAfter(s -> s.stringValue(String.valueOf(searchAfter)))
                            .sort(getSortOption(sorting)));
        } else {
            searchRequest = SearchRequest
                    .of(e -> e.index(indexList.toString()).size(SEARCH_AFTER_SIZE).searchAfter(s -> s.stringValue(String.valueOf(searchAfter)))
                            .sort(getSortOption(sorting)));
        }
        try {
            response = client.search(searchRequest, JSONObject.class);
            LOG.info("Response of getSearchForSort : {} ", response);
        }catch (IOException ex){
            LOG.error("Error while processing searchIndexes " , ex);
        }
        return response;
    }

    private String getIndexList(final List<String> filteredIndexList) {
        String includeIndexes = null;
        StringBuilder indexList = new StringBuilder();
        if (!filteredIndexList.isEmpty()) {
            includeIndexes = filteredIndexList.stream().collect(Collectors.joining(","));
        }
        indexList.append(includeIndexes);
        return indexList.toString();
    }

    private List<String> getFilteredIndices(final IndexParametersConfiguration indexParametersConfiguration,
                                            final ElasticsearchClient elasticsearchClient) throws IOException, ParseException {
        List<org.opensearch.client.opensearch.cat.indices.IndicesRecord> catIndices = new ArrayList<>();
        if (indexParametersConfiguration != null && indexParametersConfiguration.getInclude() == null ||
                indexParametersConfiguration != null && indexParametersConfiguration.getInclude().isEmpty()) {
            catIndices = callCatElasticIndices(elasticsearchClient);


            if (indexParametersConfiguration != null && indexParametersConfiguration.getExclude() != null
                    && !indexParametersConfiguration.getExclude().isEmpty()) {
                catIndices = catIndices.stream().filter(c -> !(indexParametersConfiguration.getExclude().contains(c.index()))).
                        collect(Collectors.toList());
            }

        }
        else{
            catIndices= getOpenSearchIndicesRecords(indexParametersConfiguration,catIndices);
        }
        return catIndices.stream().map(c->c.index()).collect(Collectors.toList());
    }

    public List<org.opensearch.client.opensearch.cat.indices.IndicesRecord> callCatElasticIndices(final ElasticsearchClient client) throws IOException, ParseException {
        List<org.opensearch.client.opensearch.cat.indices.IndicesRecord> indicesRecords=new ArrayList<>();
        client.cat().indices().valueBody().forEach(elasticSearchCatIndex->{
            indicesRecords.add(new ObjectMapper().convertValue(elasticSearchCatIndex, org.opensearch.client.opensearch.cat.indices.IndicesRecord.class));
        });
        return indicesRecords;
    }
    private  List<org.opensearch.client.opensearch.cat.indices.IndicesRecord> getOpenSearchIndicesRecords(final IndexParametersConfiguration indexParametersConfiguration,
                                                                                                          final List<org.opensearch.client.opensearch.cat.indices.IndicesRecord>  indicesRecords) {
        if (indexParametersConfiguration.getExclude() != null
                && !indexParametersConfiguration.getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = indexParametersConfiguration.getInclude().stream()
                    .filter(index -> !(indexParametersConfiguration.getExclude().contains(index))).collect(Collectors.toList());
            indexParametersConfiguration.setInclude(filteredIncludeIndexes);
        }
        indexParametersConfiguration.getInclude().forEach(index -> {
            org.opensearch.client.opensearch.cat.indices.IndicesRecord indexRecord =
                    new org.opensearch.client.opensearch.cat.indices.IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }
}
