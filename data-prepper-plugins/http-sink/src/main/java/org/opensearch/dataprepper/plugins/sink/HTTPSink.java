/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.util.TimeValue;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.LocalFileBufferFactory;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.service.HttpSinkService;
import org.opensearch.dataprepper.plugins.sink.service.WebhookService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;

@DataPrepperPlugin(name = "http", pluginType = Sink.class, pluginConfigurationType = HttpSinkConfiguration.class)
public class HTTPSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);

    private static final String BUCKET = "bucket";
    private static final String KEY_PATH = "key_path_prefix";

    private final AwsCredentialsSupplier awsCredentialsSupplier;

    private WebhookService webhookService;

    private volatile boolean sinkInitialized;

    private final OutputCodec codec;

    private final HttpSinkService httpSinkService;

    private final BufferFactory bufferFactory;

    private DlqPushHandler dlqPushHandler;

    private final SinkContext sinkContext;

    @DataPrepperPluginConstructor
    public HTTPSink(final PluginSetting pluginSetting,
                    final HttpSinkConfiguration httpSinkConfiguration,
                    final PluginFactory pluginFactory,
                    final PipelineDescription pipelineDescription,
                    final AwsCredentialsSupplier awsCredentialsSupplier,
                    final SinkContext sinkContext) {
        super(pluginSetting);
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.sinkContext = sinkContext;
        final PluginModel codecConfiguration = httpSinkConfiguration.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        codecPluginSettings.setPipelineName(pipelineDescription.getPipelineName());
        this.codec = pluginFactory.loadPlugin(OutputCodec.class, codecPluginSettings);
        this.sinkInitialized = Boolean.FALSE;
        if (httpSinkConfiguration.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
            this.bufferFactory = new LocalFileBufferFactory();
        } else {
            this.bufferFactory = new InMemoryBufferFactory();
        }

        if(httpSinkConfiguration.getDlqFile() != null)
            this.dlqPushHandler = new DlqPushHandler(httpSinkConfiguration.getDlqFile(), pluginFactory,
                    null, null, null, null);

        else if(Objects.nonNull(httpSinkConfiguration.getDlq()))
            this.dlqPushHandler = new DlqPushHandler(httpSinkConfiguration.getDlqFile(), pluginFactory,
                    httpSinkConfiguration.getDlq().getPluginSettings().get(BUCKET).toString(), httpSinkConfiguration.getAwsAuthenticationOptions()
                    .getAwsStsRoleArn(), httpSinkConfiguration.getAwsAuthenticationOptions().getAwsRegion().toString(),
                    httpSinkConfiguration.getDlq().getPluginSettings().get(KEY_PATH).toString());

        final HttpRequestRetryStrategy httpRequestRetryStrategy = new DefaultHttpRequestRetryStrategy(httpSinkConfiguration.getMaxUploadRetries(),
                TimeValue.of(httpSinkConfiguration.getHttpRetryInterval()));

        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRetryStrategy(httpRequestRetryStrategy);

        if(Objects.nonNull(httpSinkConfiguration.getWebhookURL()))
            this.webhookService = new WebhookService(httpSinkConfiguration.getWebhookURL(),
                    httpClientBuilder,pluginMetrics,httpSinkConfiguration);

        this.httpSinkService = new HttpSinkService(codec,
                httpSinkConfiguration,
                bufferFactory,
                dlqPushHandler,
                codecPluginSettings,
                webhookService,
                httpClientBuilder,
                pluginMetrics,
                awsCredentialsSupplier,
                Objects.nonNull(sinkContext) ? sinkContext.getTagsTargetKey() : null);
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
        httpSinkService.output(records);
    }
}