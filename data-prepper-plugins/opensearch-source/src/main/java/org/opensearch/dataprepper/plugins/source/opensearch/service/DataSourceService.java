/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.connection.PrepareConnection;
import org.opensearch.dataprepper.plugins.source.opensearch.model.SourceInfo;
import org.opensearch.dataprepper.plugins.source.opensearch.scheduler.ElasticSearchPITPaginationTask;
import org.opensearch.dataprepper.plugins.source.opensearch.scheduler.ElasticSearchPITTask;
import org.opensearch.dataprepper.plugins.source.opensearch.scheduler.OpenSearchPITTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.ArrayList;

/**
 * Reference to Connection Info of OpenSearch, along with health check
 */
public class DataSourceService {


    private static final Logger LOG = LoggerFactory.getLogger(DataSourceService.class);

    private String datasource;

    private static final String GET_REQUEST_MEHTOD = "GET";

    private static final String CONTENT_TYPE = "content-type";

    private static final String CONTENT_TYPE_VALUE = "application/json";

    private static final String VERSION = "version";

    private static final String DISTRIBUTION = "distribution";

    private static final String ELASTIC_SEARCH = "elasticsearch";

    private static final String CLUSTER_STATS_ENDPOINTS = "_cluster/stats";

    private static final String CLUSTER_HEALTH_STATUS = "status";

    private static final String CLUSTER_HEALTH_STATUS_RED = "red";

    private static final String NODES = "nodes";

    private static final String VERSIONS = "versions";

    private static final String REGULAR_EXPRESSION = "[^a-zA-Z0-9]";

    private static final String OPEN_SEARCH = "opensearch";

    private static final int VERSION_1_3_0 = 130;

    private static final int VERSION_7_10_0 = 7100;

    private static final Integer BATCH_SIZE_VALUE = 1000;

    private final JsonFactory jsonFactory = new JsonFactory();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Map<String, String> indexMap;

    private OpenSearchClient openSearchClient;
    private ElasticsearchClient elasticsearchClient;
    private PrepareConnection prepareConnection;

    public DataSourceService(PrepareConnection prepareConnection) {
        this.prepareConnection = prepareConnection;
    }

    public static Logger getLOG() {
        return LOG;
    }

    public OpenSearchClient getOpenSearchClient() {
        return openSearchClient;
    }

    public ElasticsearchClient getElasticsearchClient() {
        return elasticsearchClient;
    }


    /**
     * @param openSearchSourceConfiguration
     * @return This method will help to identify the source information eg(opensearch,elasticsearch)
     */
    public String getSourceInfo(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        try {
            JSONParser jsonParser = new JSONParser();
            StringBuilder response = new StringBuilder();
            if (StringUtils.isBlank(openSearchSourceConfiguration.getHosts().get(0)))
                throw new IllegalArgumentException("Hostname cannot be null or empty");
            URL obj = new URL(openSearchSourceConfiguration.getHosts().get(0));
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod(GET_REQUEST_MEHTOD);
            con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
            int responseCode = con.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                LOG.info("Response is  : {} ", response);
            } else {
                LOG.error("GET request did not work.");
            }
            JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
            Map<String, String> versionMap = ((Map) jsonObject.get(VERSION));
            for (Map.Entry<String, String> entry : versionMap.entrySet()) {
                if (entry.getKey().equals(DISTRIBUTION)) {
                    datasource = String.valueOf(entry.getValue());
                }
            }
        } catch (Exception e) {
            LOG.error("Error while getting data source", e);
        }
        if (datasource == null)
            datasource = ELASTIC_SEARCH;
        return datasource;
    }

    public JSONObject callToHealthCheck(final OpenSearchSourceConfiguration openSearchSourceConfiguration) throws IOException, ParseException {
        URL obj = new URL(openSearchSourceConfiguration.getHosts().get(0) + CLUSTER_STATS_ENDPOINTS);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod(GET_REQUEST_MEHTOD);
        con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
        int responseCode = con.getResponseCode();
        JSONParser jsonParser = new JSONParser();
        StringBuilder response = new StringBuilder();
        String status;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            LOG.info("Response is {} ", response);
        } else {
            LOG.error("Connection is down");
        }
        JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
        return jsonObject;
    }

    public void versionCheck(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final SourceInfo sourceInfo
            , Buffer<Record<Event>> buffer, List<IndicesRecord> indicesList)  {
        int osVersionIntegerValue = Integer.parseInt(sourceInfo.getOsVersion().replaceAll(REGULAR_EXPRESSION, ""));
        if (sourceInfo.getDataSource().equalsIgnoreCase(OPEN_SEARCH)) {
            if (osVersionIntegerValue >= VERSION_1_3_0) {
                new Timer().scheduleAtFixedRate(new OpenSearchPITTask(openSearchSourceConfiguration, buffer, openSearchClient, indicesList), openSearchSourceConfiguration.getSchedulingParameterConfiguration().getStartTime().getSecond(), openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate().toMillis());
            } else {

            }

        } else {
            if (osVersionIntegerValue >= VERSION_7_10_0) {
                if (BATCH_SIZE_VALUE <= openSearchSourceConfiguration.getSearchConfiguration().getBatchSize()) {
                    LOG.info(" %%%%%%%%%%%%%%% batch size:" + openSearchSourceConfiguration.getSearchConfiguration().getBatchSize());
                    if (!openSearchSourceConfiguration.getSearchConfiguration().getSorting().isEmpty()) {
                      LOG.info(" %%%%%%%    inside if condition %%%%%%% ");
                        new Timer().scheduleAtFixedRate(new ElasticSearchPITPaginationTask(openSearchSourceConfiguration, buffer, elasticsearchClient), openSearchSourceConfiguration.getSchedulingParameterConfiguration().getStartTime().getSecond(), openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate().toMillis());
                    } else {
                        LOG.info("Sort must contain at least one field");
                    }
                } else {
                    new Timer().scheduleAtFixedRate(new ElasticSearchPITTask(openSearchSourceConfiguration, buffer, elasticsearchClient), openSearchSourceConfiguration.getSchedulingParameterConfiguration().getStartTime().getSecond(), openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate().toMillis());
                }
            } else {

            }
        }

    }

    public void writeClusterDataToBuffer(final String responseBody,final Buffer<Record<Event>> buffer) throws TimeoutException {
        try {
            final JsonParser jsonParser = jsonFactory.createParser(responseBody);
            Record<Event> jsonRecord = new Record<>(JacksonLog.builder().withData(jsonParser.readValuesAs(Map.class)).build());
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

    public List<IndicesRecord> callCatElasticIndices(final ElasticsearchClient client) throws IOException,ParseException {
        List<IndicesRecord> indicesRecords=new ArrayList<>();
        client.cat().indices().valueBody().forEach(elasticSearchCatIndex->{
            indicesRecords.add(objectMapper.convertValue(elasticSearchCatIndex,IndicesRecord.class));
        });
        return indicesRecords;
    }

    public  List<IndicesRecord> getOpenSearchIndicesRecords(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                            final List<IndicesRecord>  indicesRecords) {
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().stream()
                    .filter(index -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(index))).collect(Collectors.toList());
            openSearchSourceConfiguration.getIndexParametersConfiguration().setInclude(filteredIncludeIndexes);
        }
        openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().forEach(index -> {
            IndicesRecord indexRecord =
                    new IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }

    public  List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord> getElasticSearchIndicesRecords(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                                                                            final List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord>  indicesRecords) {
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().stream()
                    .filter(index -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(index))).collect(Collectors.toList());
            openSearchSourceConfiguration.getIndexParametersConfiguration().setInclude(filteredIncludeIndexes);
        }
        openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().forEach(index -> {
            co.elastic.clients.elasticsearch.cat.indices.IndicesRecord indexRecord =
                    new co.elastic.clients.elasticsearch.cat.indices.IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }

    public List<IndicesRecord> getCatIndices(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final String datasource) throws IOException, ParseException {
        List<IndicesRecord> catIndices = new ArrayList<>();
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude() == null ||
                openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().isEmpty()) {
            if (OPEN_SEARCH.equalsIgnoreCase(datasource)) {
                openSearchClient= prepareConnection.prepareOpensearchConnection(openSearchSourceConfiguration);
                catIndices = openSearchClient.cat().indices().valueBody();
            } else {
                elasticsearchClient = prepareConnection.prepareElasticSearchConnection(openSearchSourceConfiguration);
                catIndices = callCatElasticIndices(elasticsearchClient);
            }

            if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                    && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
                catIndices = catIndices.stream().filter(c -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(c.index()))).
                        collect(Collectors.toList());
            }
        } else {
            catIndices = getOpenSearchIndicesRecords(openSearchSourceConfiguration, catIndices);
        }
    return catIndices;
    }

}