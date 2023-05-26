
package org.opensearch.dataprepper.plugins.source.opensearch.model;

import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import org.opensearch.dataprepper.plugins.source.opensearch.codec.JacksonValueParser;

import java.util.HashMap;
import java.util.Map;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class ScrollRequest {
    private StringBuilder index;
    private Integer size;
    private static final String GET_REQUEST_MEHTOD = "GET";
    private static final String SEARCHURL = "/_search";

    public ScrollRequest(ScrollBuilder scrollBuilder) {
    }
    public ScrollRequest(){

    }

    public StringBuilder getIndex() {
        return index;
    }

    public void setIndex(StringBuilder index) {
        this.index = index;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    static JsonpDeserializer<String> newResponseParser;
    final static JsonpDeserializer<Map> deserializer = new JacksonValueParser<>(Map.class);

    public static final Endpoint<ScrollRequest, Map, ErrorResponse> ENDPOINT =
            new SimpleEndpoint<>(
                    r -> GET_REQUEST_MEHTOD,
                    r -> "http://localhost:9200/" + r.index + SEARCHURL,
                    r -> {
                        Map<String, String> params = new HashMap<>();
                        params.put("scroll", "10m");
                        return params;
                    },
                    SimpleEndpoint.emptyMap(), false,
                    deserializer
            );

}