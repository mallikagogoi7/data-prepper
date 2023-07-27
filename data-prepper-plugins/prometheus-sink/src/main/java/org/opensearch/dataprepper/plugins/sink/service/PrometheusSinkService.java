/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.handler.MultiAuthPrometheusSinkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This service class contains logic for sending data to Prometheus Endpoints
 */
public class PrometheusSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkService.class);

    private final Lock reentrantLock;

    private final PrometheusSinkConfiguration prometheusSinkConfiguration;

    private WebhookService webhookService;

    private Buffer currentBuffer;

    private MultiAuthPrometheusSinkHandler multiAuthPrometheusSinkHandler;

    public PrometheusSinkService(final PrometheusSinkConfiguration prometheusSinkConfiguration,
                                 final WebhookService webhookService){
        this.prometheusSinkConfiguration = prometheusSinkConfiguration;
        this.reentrantLock = new ReentrantLock();
        this.webhookService = webhookService;
    }

    /**
     * This method process buffer records and send to Prometheus End points based on configured codec
     * @param records Collection of Event
     */
    public void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        try {
            records.forEach(record -> {
                final Event event = record.getData();
                try {
                    currentBuffer.writeEvent(event.toJsonString().getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                // TODO: threshold check
                // TODO: push current buffer data to prometheus endpoint
                // TODO: implement retry mechanism
                // TODO: push failed Data to DLQ
                });
        }finally {
            reentrantLock.unlock();
        }
    }

    /**
     * * This method logs Failed Data to DLQ and Webhook
     *  @param endPointResponses HttpEndPointResponses.
     */
    private void logFailedData(final HttpEndPointResponse endPointResponses) {
        //TODO: implementation pushing failed data to DLQ
    }

    /**
     * * This method pushes bufferData to configured HttpEndPoints
     *  @param currentBufferData bufferData.
     */
    private HttpEndPointResponse pushToEndPoint(final byte[] currentBufferData) {
        //TODO: implementation
        return null;
    }

    /**
     * * This method gets Auth Handler classes based on configuration
     *  @param authType AuthTypeOptions.
     *  @param authOptions HttpAuthOptions.Builder.
     */
    private HttpAuthOptions getAuthHandlerByConfig(final AuthTypeOptions authType,
                                                   final HttpAuthOptions.Builder authOptions){

        // TODO: implementation
        return multiAuthPrometheusSinkHandler.authenticate(authOptions);
    }

    /**
     * * This method build HttpAuthOptions class based on configurations
     *  @param prometheusSinkConfiguration PrometheusSinkConfiguration.
     */
    private Map<String,HttpAuthOptions> buildAuthHttpSinkObjectsByConfig(final PrometheusSinkConfiguration prometheusSinkConfiguration){
        //TODO: implementation
        return null;
    }

    /**
     * * This method adds custom Header in the request such as SageMaker
     *  @param classicRequestBuilder ClassicRequestBuilder.
     *  @param customHeaderOptions CustomHeaderOptions .
     */
    private void addCustomHeaders(final ClassicRequestBuilder classicRequestBuilder,
                                  final Map<String, List<String>> customHeaderOptions) {

        // TODO: implementation
    }

    /**
     * * builds ClassicRequestBuilder based on configured HttpMethod
     *  @param httpMethodOptions Http Method.
     */
    private ClassicRequestBuilder buildRequestByHTTPMethodType(final HTTPMethodOptions httpMethodOptions) {
        // TODO: implementation
        return null;
    }
}