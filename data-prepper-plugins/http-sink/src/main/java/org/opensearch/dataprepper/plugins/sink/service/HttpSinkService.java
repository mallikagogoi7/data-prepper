/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.DLQSink;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.opensearch.dataprepper.plugins.sink.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HttpSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkService.class);

    private final Codec codec;

    private final HttpSinkConfiguration httpSinkConf;

    private final BufferFactory bufferFactory;

    private final Map<String,HttpAuthOptions> httpAuthOptions;

    private final DLQSink dlqSink;

    private final PluginSetting pluginSetting;

    private ExecutorService executorService;

    private final Lock reentrantLock;

    public HttpSinkService(final Codec codec,
                           final HttpSinkConfiguration httpSinkConf,
                           final BufferFactory bufferFactory,
                           final Map<String, HttpAuthOptions> httpAuthOptions,
                           final DLQSink dlqSink,
                           final PluginSetting pluginSetting){
        this.codec= codec;
        this.httpSinkConf = httpSinkConf;
        this.bufferFactory = bufferFactory;
        this.httpAuthOptions = httpAuthOptions;
        this.dlqSink = dlqSink;
        this.pluginSetting = pluginSetting;
        reentrantLock = new ReentrantLock();
    }

    public void processRecords(Collection<Record<Event>> records) {
        reentrantLock.lock();
        AtomicInteger responseCode = new AtomicInteger();
        records.forEach(record -> {
            try{
                // logic to fetch the records in batch as per threshold limit -  checkThresholdExceed();
                final Event event = record.getData();
                for(UrlConfigurationOption urlConfOption: httpSinkConf.getUrlConfigurationOptions()) {
                    HttpClientContext clientContext = HttpClientContext.create();
                    final ClassicRequestBuilder classicHttpRequestBuilder =
                            httpAuthOptions.get(urlConfOption.getUrl()).getClassicHttpRequestBuilder();
                    classicHttpRequestBuilder.setEntity(codec.parse(event));
                    httpAuthOptions.get(urlConfOption.getUrl()).getCloseableHttpClient()
                            .execute(classicHttpRequestBuilder.build(), clientContext, response -> {
                        LOG.info("Http Response code : " + response.getCode());
                        responseCode.set(response.getCode());
                        final HttpEntity entity = response.getEntity();
                        EntityUtils.consume(entity);
                        LOG.info("Request Body: " +response.getEntity());
                        return null;
                    });

                }
            }catch(Exception e){
                FailedDlqData failedDlqData = new FailedDlqData(responseCode.get(), e.getMessage(), record.getData().getEventHandle());
                logFailureForDlqObjects(failedDlqData);
                // In case of any exception, need to write the exception in dlq  - logFailureForDlqObjects();
                // In case of any exception, need to push the web hook url- logFailureForWebHook();
            }
        });
        reentrantLock.unlock();
    }

    public static boolean checkThresholdExceed(final Buffer currentBuffer,
                                               final int maxEvents,
                                               final ByteCount maxBytes,
                                               final long maxCollectionDuration) {
        // logic for checking the threshold
        return true;
    }

    private void logFailureForDlqObjects(final FailedDlqData failedDlqData){
            dlqSink.perform(pluginSetting, failedDlqData);
    }

    private void logFailureForWebHook(final String message, final Throwable failure,final String url){
        // logic for pushing to web hook url.
    }
}
