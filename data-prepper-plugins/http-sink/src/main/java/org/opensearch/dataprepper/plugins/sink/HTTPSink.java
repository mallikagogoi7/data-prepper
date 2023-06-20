/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.plugins.sink.accumulator.*;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;

import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.service.HttpSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@DataPrepperPlugin(name = "http", pluginType = Sink.class, pluginConfigurationType = HttpSinkConfiguration.class)
public class HTTPSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);
    private final HttpSinkConfiguration httpSinkConfiguration;
    private volatile boolean sinkInitialized;

    private final Codec codec;

    HttpSinkService service;

    private final BufferFactory bufferFactory;

    private final Lock reentrantLock;

    private Buffer currentBuffer;

    private AuthHandler authHandler;

    @DataPrepperPluginConstructor
    public HTTPSink(final PluginSetting pluginSetting,
                  final HttpSinkConfiguration httpSinkConfiguration,
                  final PluginFactory pluginFactory) {
        super(pluginSetting);
        this.httpSinkConfiguration = httpSinkConfiguration;
        final PluginModel codecConfiguration = httpSinkConfiguration.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        reentrantLock = new ReentrantLock();
        codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
        sinkInitialized = Boolean.FALSE;
        if (httpSinkConfiguration.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
            bufferFactory = new LocalFileBufferFactory();
        } else {
            bufferFactory = new InMemoryBufferFactory();
        }
        authHandler = new AuthHandler();
        service = new HttpSinkService(codec,httpSinkConfiguration, bufferFactory, authHandler);
    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        try {
            doInitializeInternal();
        } catch (InvalidPluginConfigurationException e) {
            LOG.error("Invalid plugin configuration, Hence failed to initialize http-sink plugin.");
            this.shutdown();
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to initialize http-sink plugin.");
            this.shutdown();
            throw e;
        }
    }

    private void doInitializeInternal() {
        sinkInitialized = Boolean.TRUE;
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }
        //reentrantLock.lock();
        if (currentBuffer == null) {
            currentBuffer = bufferFactory.getBuffer();
        }
        for (final Record<Event> record : records) {
            final Event event = record.getData();
            final String encodedEvent;
            try {
                encodedEvent = codec.parse(event);
                service.sendHttpRequest(encodedEvent);
            }catch (IOException | InterruptedException | URISyntaxException e) {
                LOG.error("Exception while write event into buffer :", e);
            }
          //  reentrantLock.unlock();
        }
    }
}