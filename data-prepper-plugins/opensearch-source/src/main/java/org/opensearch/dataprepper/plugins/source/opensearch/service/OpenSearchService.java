/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.client.opensearch.core.ClearScrollRequest;
import org.opensearch.client.opensearch.core.ClearScrollResponse;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchClientBuilder;
import org.opensearch.dataprepper.plugins.source.opensearch.client.HttpCustomClient;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SortingConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * OpenSearch service related implementation
 */
public class OpenSearchService {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchService.class);

    private OpenSearchClientBuilder clientBuilder;

    public OpenSearchService(OpenSearchClientBuilder clientBuilder) {

        this.clientBuilder = clientBuilder;
    }

    private static final Integer BATCH_SIZE_VALUE = 1000;

    private static final int OPEN_SEARCH_VERSION = 130;

    private static final String PIT_ID = "pit_id";

    private static final String POINT_IN_TIME_KEEP_ALIVE = "keep_alive";

    private static final String KEEP_ALIVE_VALUE = "24h";

    private ObjectMapper mapper = new ObjectMapper();


    public String getPITId(final String index, OpenSearchClient openSearchClient, Integer maxRetries) {
        try {
            Map pitResponse = getPitInformation(index, openSearchClient);
            return pitResponse.get(PIT_ID).toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("No pit id fetched ");
    }

    private Map getPitInformation(String index, OpenSearchClient openSearchClient) throws IOException {
        PitRequest pitRequest = new PitBuilder().index(new StringBuilder(index)).build();
        Map<String, String> params = new HashMap<>();
        params.put(POINT_IN_TIME_KEEP_ALIVE, KEEP_ALIVE_VALUE);
        pitRequest.setQueryParameters(params);
        Map pitResponse = openSearchClient._transport().performRequest(pitRequest, PitRequest.ENDPOINT, openSearchClient._transportOptions());
        LOG.debug("Fetch Pit Response :: " + pitResponse);
        return pitResponse;
    }

    public Map<String, Object> searchIndexesByPITId(final String pitId, final Integer batchSize,
                                                    final List<String> queryFields,
                                                    final List<SortingConfiguration> sortingConfigurations,
                                                    final HttpCustomClient customHttpClient,
                                                    Buffer buffer) {
        Map response = searchIndexeByPitId(pitId, batchSize, queryFields, sortingConfigurations, customHttpClient);
        LOG.debug(" Search Pit Response " + response);
        try {
            writeClusterDataToBuffer(mapper.writeValueAsString(response), buffer);
        } catch (TimeoutException | JsonProcessingException e) {
            LOG.error("Write operation failed " + e);
        }
        return response;
    }

    public void writeClusterDataToBuffer(final String responseBody, final Buffer<Record<Event>> buffer) throws TimeoutException {
        try {
            LOG.info("Write to buffer code started {} ", buffer);
            final JsonParser jsonParser = new JsonFactory().createParser(responseBody);
            final Map<String, Object> innerJson = mapper.readValue(jsonParser, Map.class);
            Event event = JacksonLog.builder().withData(innerJson).build();
            Record<Event> jsonRecord = new Record<>(event);
            LOG.info("Data is pushed to buffer {} ", jsonRecord);
            buffer.write(jsonRecord, 1200);

        } catch (Exception e) {
            LOG.error("Unable to parse json data [{}], assuming plain text", responseBody, e);
            final Map<String, Object> plainMap = new HashMap<>();
            plainMap.put("message", responseBody);
            Event event = JacksonLog.builder().withData(plainMap).build();
            Record<Event> jsonRecord = new Record<>(event);
            buffer.write(jsonRecord, 1200);
        }
    }


    private Map searchIndexeByPitId(final String pitId, Integer batchSize, List<String> queryFields, List<SortingConfiguration> sortingConfigurations,
                                    HttpCustomClient customHttpClient) {

        SearchPitIndexRequest searchPitIndexRequest = createSearchIndexByPitRequest(pitId, batchSize, queryFields, sortingConfigurations);
        try {
            return customHttpClient.execute(Map.class, searchPitIndexRequest, "GET", "_search");
        } catch (IOException e) {
            LOG.error("search Pit Index failed");
        }
        throw new RuntimeException("search Pit Index failed");
    }

    private SearchPitIndexRequest createSearchIndexByPitRequest(final String pitId, final Integer batchSize, final List<String> queryFields,
                                                                List<SortingConfiguration> sorting) {
        SearchPitIndexRequest searchPitIndexRequest = new SearchPitIndexRequest();
        SearchPitIndexRequest.PitInformation pitInfo = new SearchPitIndexRequest.PitInformation();
        pitInfo.setId(pitId);
        pitInfo.setKeepAlive(KEEP_ALIVE_VALUE);

        if (queryFields != null && !queryFields.isEmpty()) {
            final Map<String, String> queryMap = new HashMap<>();

            queryFields.forEach(q -> {
                String[] queryObj = q.split(":");
                queryMap.put(queryObj[0].trim(), queryObj[1].trim());
            });
            searchPitIndexRequest.setQuery(queryMap);
        }

        if (sorting != null && !sorting.isEmpty()) {
            searchPitIndexRequest.setSort(sorting);
        }

        if (BATCH_SIZE_VALUE > batchSize) {
            searchPitIndexRequest.setSize(batchSize);
        }
        searchPitIndexRequest.setPit(pitInfo);

        return searchPitIndexRequest;
    }


    public Map<String, Object> searchIndexesByPITIdForPagination(String pitId, Integer batchSize, List<String> queryFields, List<SortingConfiguration>
            sortingConfigurations
            , HttpCustomClient httpCustomClient, Buffer buffer) {

        int sizeForPagination = 100;
        List<Integer> searchAfter = new ArrayList<>();
        while (batchSize > 0) {
            SearchPitIndexRequest searchIndexByPitRequest = createSearchIndexByPitRequest(pitId, batchSize, queryFields, sortingConfigurations);
            if (searchAfter != null && !searchAfter.isEmpty()) {
                searchIndexByPitRequest.setSearchAfter(searchAfter);
            }
            try {
                PitSearchResponse response = httpCustomClient.execute(PitSearchResponse.class, searchIndexByPitRequest, "GET", "_search");
                writeClusterDataToBuffer(mapper.writeValueAsString(response), buffer);
                List<Hit> hits = response.getHits().getHits();
                Hit lastHit = new Hit();
                if (!hits.isEmpty()) {
                    lastHit = hits.get(hits.size() - 1);
                }

                searchAfter = lastHit.getSort();
            } catch (IOException e) {
                LOG.error("search Pit Index failed");
            } catch (TimeoutException e) {
                LOG.error("Write operation failed " + e);
            }
            batchSize = batchSize - sizeForPagination;

        }

        throw new RuntimeException("search Pit Index failed");
    }


    public Map<String, Object> scrollIndexesByIndexAndUrl(final String index, final OpenSearchClient client,
                                                          final Buffer buffer) {
        // if response is not 200 then will call BackoffService for retry
        ScrollRequest scrollRequest = new ScrollRequest();
        scrollRequest.setIndex(new StringBuilder(index));
        scrollRequest.setSize(BATCH_SIZE_VALUE);
        Map<String, Object> response = null;
        try {
            response = client._transport().performRequest(scrollRequest, ScrollRequest.ENDPOINT, client._transportOptions());
            writeClusterDataToBuffer(mapper.writeValueAsString(response), buffer);
        } catch (IOException e) {
            LOG.error("search scroll Index failed");
        } catch (TimeoutException e) {
            LOG.error("Write operation failed " + e);
        }
        return response;
    }

    public boolean deletePITId(String pitId, HttpCustomClient httpCustomClient) {
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put(PIT_ID, pitId);
        DeletePitResponse deletePITResponse = null;
        try {
            deletePITResponse = httpCustomClient.execute(DeletePitResponse.class, inputMap, "DELETE",
                    "_search/point_in_time");
        } catch (IOException e) {
            LOG.error(" Delete operation failed " + e);
        }
        if (deletePITResponse != null) {
            return deletePITResponse.getPits().get(0).getSuccessful();
        } else {
            throw new RuntimeException(" Delete operation failed ");
        }
    }

    public boolean deleteScrollId(final String scrollId, final OpenSearchClient client) {
        if(scrollId==null){
            throw new IllegalArgumentException("Invalid scroll Id");
        }

        ClearScrollRequest scrollRequest = new ClearScrollRequest.Builder().scrollId(scrollId).build();
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = client.clearScroll(scrollRequest);
        } catch (IOException e) {
            LOG.error("Delete operation failed", e);
        }
        LOG.debug("Delete Scroll ID Response " + clearScrollResponse);
        LOG.debug("Delete successful " + clearScrollResponse.succeeded());
        return clearScrollResponse.succeeded();

    }

    public void processIndexes(final Integer version,
                               final IndexParametersConfiguration indexParametersConfiguration,
                               final URL url,
                               final Integer batchSize,
                               final Integer maxRetries,
                               final List<String> queryFields,
                               final List<SortingConfiguration> sortingConfigurations,
                               final Buffer buffer) {
        OpenSearchClient openSearchClient = clientBuilder.createOpenSearchClient(url);
        String indexList = null;
        try {
            indexList = getIndexList(getFilteredIndices(indexParametersConfiguration, openSearchClient));
        } catch (IOException e) {
            LOG.error("Cat indices fetch failed ", e);
        }
        Map<String, Object> recordsMap = null;
        if (version > OPEN_SEARCH_VERSION) {
            String pitId = getPITId(indexList, openSearchClient, maxRetries);
            HttpCustomClient customHttpClient = clientBuilder.createCustomHttpClient(url);
            if (batchSize > BATCH_SIZE_VALUE) {
                recordsMap = searchIndexesByPITIdForPagination(pitId, batchSize, queryFields, sortingConfigurations,
                        clientBuilder.createCustomHttpClient(url), buffer);
            } else {
                recordsMap = searchIndexesByPITId(pitId, batchSize, queryFields, sortingConfigurations,
                        clientBuilder.createCustomHttpClient(url), buffer);
            }
           // deletePITId(recordsMap.get(PIT_ID).toString(), customHttpClient);
        } else {
            recordsMap = scrollIndexesByIndexAndUrl(indexList, openSearchClient, buffer);
            deleteScrollId(recordsMap.get("_scroll_id")!=null ?
                    recordsMap.get("_scroll_id").toString():null, openSearchClient);
        }


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
                                            final OpenSearchClient openSearchClient) throws IOException {
        List<IndicesRecord> catIndices = new ArrayList<>();
        if (indexParametersConfiguration != null && indexParametersConfiguration.getInclude() == null ||
                indexParametersConfiguration != null && indexParametersConfiguration.getInclude().isEmpty()) {
            catIndices = openSearchClient.cat().indices().valueBody();


            if (indexParametersConfiguration != null && indexParametersConfiguration.getExclude() != null
                    && !indexParametersConfiguration.getExclude().isEmpty()) {
                catIndices = catIndices.stream().filter(c -> !(indexParametersConfiguration.getExclude().contains(c.index()))).
                        collect(Collectors.toList());
            }

        } else {
            catIndices = getOpenSearchIndicesRecords(indexParametersConfiguration, catIndices);
        }
        return catIndices.stream().map(c -> c.index()).collect(Collectors.toList());
    }


    private List<IndicesRecord> getOpenSearchIndicesRecords(final IndexParametersConfiguration indexParametersConfiguration,
                                                            final List<IndicesRecord> indicesRecords) {
        if (indexParametersConfiguration.getExclude() != null
                && !indexParametersConfiguration.getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = indexParametersConfiguration.getInclude().stream()
                    .filter(index -> !(indexParametersConfiguration.getExclude().contains(index))).collect(Collectors.toList());
            indexParametersConfiguration.setInclude(filteredIncludeIndexes);
        }
        indexParametersConfiguration.getInclude().forEach(index -> {
            IndicesRecord indexRecord =
                    new IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }
}
