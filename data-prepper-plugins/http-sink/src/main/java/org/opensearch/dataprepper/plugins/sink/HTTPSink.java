/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.opensearch.dataprepper.plugins.sink.accumulator.*;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;

import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.opensearch.dataprepper.plugins.sink.handler.BasicAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.handler.MultiAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.BearerTokenAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.SecuredAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.service.HttpSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@DataPrepperPlugin(name = "http", pluginType = Sink.class, pluginConfigurationType = HttpSinkConfiguration.class)
public class HTTPSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);

    private final HttpSinkConfiguration httpSinkConfiguration;

    private volatile boolean sinkInitialized;

    private final Codec codec;

    private HttpSinkService httpSinkService;

    private final BufferFactory bufferFactory;

    private Buffer currentBuffer;

    @DataPrepperPluginConstructor
    public HTTPSink(final PluginSetting pluginSetting,
                  final HttpSinkConfiguration httpSinkConfiguration,
                  final PluginFactory pluginFactory) {
        super(pluginSetting);
        this.httpSinkConfiguration = httpSinkConfiguration;
        final PluginModel codecConfiguration = httpSinkConfiguration.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
        sinkInitialized = Boolean.FALSE;
        if (httpSinkConfiguration.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
            bufferFactory = new LocalFileBufferFactory();
        } else {
            bufferFactory = new InMemoryBufferFactory();
        }

        final List<HttpAuthOptions> httpAuthOptions = getClassicHttpRequestList(httpSinkConfiguration.getUrlConfigurationOptions());
        this.httpSinkService = new HttpSinkService(codec,httpSinkConfiguration, bufferFactory,httpAuthOptions);
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
        if (currentBuffer == null) {
            currentBuffer = bufferFactory.getBuffer();
        }
        httpSinkService.processRecords(records);
    }

    private Optional<CloseableHttpClient> getAuthHandlerByConfig(final HttpSinkConfiguration sinkConfiguration){
        MultiAuthHttpSinkHandler multiAuthHttpSinkHandler = null;
        // AWS Sigv4 - check
        switch(sinkConfiguration.getAuthType()){
            case "http_basic":
                multiAuthHttpSinkHandler = new BasicAuthHttpSinkHandler();
                break;
            case "bearer_token":
                multiAuthHttpSinkHandler = new BearerTokenAuthHttpSinkHandler();
                break;
            case "mtls":
                multiAuthHttpSinkHandler = new SecuredAuthHttpSinkHandler();
                break;
        }

        return Optional.empty();
    }

    private List<HttpAuthOptions> getClassicHttpRequestList(final List<UrlConfigurationOption> urlConfigurationOption){
        // logic for create auth handler for each url based on provided configuration - getAuthHandlerByConfig()
        // logic for request preparation for each url
        //logic for worker is not there in url level then verify the gloabl workers if global workers also not defined then default 1
        // logic for get the Proxy object if url level proxy enabled else look the global proxy.
        // Aws SageMaker headers if headers found in the configuration
        return List.of();
    }

}