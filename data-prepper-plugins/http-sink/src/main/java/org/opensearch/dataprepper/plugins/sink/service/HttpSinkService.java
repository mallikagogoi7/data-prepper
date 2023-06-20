/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.HttpSinkWorker;
import org.opensearch.dataprepper.plugins.sink.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkService.class);

    private final Codec codec;

    private final HttpSinkConfiguration httpSinkConf;

    private final BufferFactory bufferFactory;

    private final List<HttpAuthOptions> httpAuthOptions;

    private ExecutorService executorService;

    public HttpSinkService(final Codec codec,
                           final HttpSinkConfiguration httpSinkConf,
                           final BufferFactory bufferFactory,
                           final List<HttpAuthOptions> httpAuthOptions){
        this.codec= codec;
        this.httpSinkConf = httpSinkConf;
        this.bufferFactory = bufferFactory;
        this.httpAuthOptions = httpAuthOptions;
    }

    public void processRecords(Collection<Record<Event>> records) {
        records.forEach(record -> {
            // logic to fetch the records in batch as per threshold limit -  checkThresholdExceed();
            // apply the codec
//            TODO: Clarification: Do we really required workers here because each record will create multiple workers?
            httpAuthOptions.forEach(httpEndPointDetails -> {
                executorService = Executors.newFixedThreadPool(httpEndPointDetails.getWorkers());
                executorService.submit(new HttpSinkWorker(httpEndPointDetails));
            });
        });
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
