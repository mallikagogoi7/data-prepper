/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.opensearch.dataprepper.plugins.sink.configuration.BearerTokenOptions;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Map;

public class OAuthRefreshTokenManager {

    public static final String BASIC = "Basic ";

    public static final String BEARER = "Bearer ";

    public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";

    private final ObjectMapper objectMapper;

    private HttpClientBuilder httpClientBuilder;


    public OAuthRefreshTokenManager(final HttpClientBuilder httpClientBuilder){
        this.httpClientBuilder = httpClientBuilder;
        this.objectMapper = new ObjectMapper();
    }

    public String getRefreshToken(final BearerTokenOptions bearerTokenOptions) {
        HttpPost request = new HttpPost(bearerTokenOptions.getTokenURL());
        request.setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_X_WWW_FORM_URLENCODED);
        request.setHeader(HttpHeaders.AUTHORIZATION, BASIC + base64Encode(bearerTokenOptions.getClientId() + ":" + bearerTokenOptions.getClientSecret()));
        String requestBody = "grant_type=" + bearerTokenOptions.getGrantType() +"&refresh_token=" + bearerTokenOptions.getRefreshToken()+"&scope=" + bearerTokenOptions.getScope();
        request.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_FORM_URLENCODED));
        Map<String,String> accessTokenMap;
        try {
            ClassicHttpResponse response = (ClassicHttpResponse)httpClientBuilder.build().execute(request);
            accessTokenMap = objectMapper.readValue(response.getEntity().getContent(),Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return BEARER + accessTokenMap.get("access_token");
    }

    private static String base64Encode(String value) {
        return java.util.Base64.getEncoder().encodeToString(value.getBytes());
    }

    public boolean isTokenExpired(final String token){
        Base64.Decoder decoder = Base64.getUrlDecoder();
        String[] chunks = token.substring(6).split("\\.");
        final Map<String,Object> tokenDetails;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            tokenDetails = objectMapper.readValue(new String(decoder.decode(chunks[1])), Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        final String expTime = "1689861669"; //String.valueOf(tokenDetails.get("exp"));
        OffsetDateTime accessTokenExpTimeStamp = Instant.ofEpochMilli(Long.valueOf(expTime ) * 1000l).atOffset(ZoneOffset.UTC);
        final Instant systemCurrentTimeStamp = Instant.now().atOffset(ZoneOffset.UTC).toInstant();
        if(systemCurrentTimeStamp.compareTo(accessTokenExpTimeStamp.toInstant())>=0) {
            return true;
        }
        return false;
    }
}
