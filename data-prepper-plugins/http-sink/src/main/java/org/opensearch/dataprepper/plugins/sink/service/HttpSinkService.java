/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSession;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class HttpSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkService.class);

    private final Codec codec;

    private final HttpSinkConfiguration httpSinkConf;

    private final BufferFactory bufferFactory;

    private final Map<String,HttpAuthOptions> httpAuthOptions;

    private ExecutorService executorService;

    public HttpSinkService(final Codec codec,
                           final HttpSinkConfiguration httpSinkConf,
                           final BufferFactory bufferFactory,
                           final Map<String, HttpAuthOptions> httpAuthOptions){
        this.codec= codec;
        this.httpSinkConf = httpSinkConf;
        this.bufferFactory = bufferFactory;
        this.httpAuthOptions = httpAuthOptions;
    }

    public void processRecords(Collection<Record<Event>> records) {
        records.forEach(record -> {
            try{
                // logic to fetch the records in batch as per threshold limit -  checkThresholdExceed();
                // apply the codec
                // push to http end point based on workers - Callable<>
                final Event event = record.getData();
                final String encodedEvent = codec.parse(event);
                for(UrlConfigurationOption urlConfOption: httpSinkConf.getUrlConfigurationOptions()) {

                    ObjectMapper objectMapper = new ObjectMapper();
                    String requestBody = objectMapper
                            .writeValueAsString(encodedEvent);
                    HttpClientContext clientContext = HttpClientContext.create();
                    ClassicHttpRequest httpPost = ClassicRequestBuilder.post(urlConfOption.getUrl())
                            .setEntity(requestBody)
                            .build();
                    httpAuthOptions.get(urlConfOption.getUrl()).getCloseableHttpClient().execute(httpPost, clientContext, response -> {
                        LOG.info("Http Response code : " + response.getCode());
                        final HttpEntity entity = response.getEntity();
                        EntityUtils.consume(entity);
                        LOG.info("Request Body: " +response.getEntity());
                        return null;
                    });

                }

            }catch(Exception e){
                // In case of any exception, need to write the exception in dlq  - logFailureForDlqObjects();
                // In case of any exception, need to push the web hook url- logFailureForWebHook();
            }
        });
        //end to end ack
    }

    public static boolean checkThresholdExceed(final Buffer currentBuffer,
                                               final int maxEvents,
                                               final ByteCount maxBytes,
                                               final long maxCollectionDuration) {
        // logic for checking the threshold
        return true;
    }

    private void logFailureForDlqObjects(final List<DlqObject> dlqObjects, final Throwable failure){
        // logic for writing failure objects into dlq ( local file / s3)
    }

    private void logFailureForWebHook(final String message, final Throwable failure,final String url){
        // logic for pushing to web hook url.
    }
}
