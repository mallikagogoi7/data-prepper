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
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.LocalFileBufferFactory;
import org.opensearch.dataprepper.plugins.sink.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.service.PrometheusSinkAwsService;
import org.opensearch.dataprepper.plugins.sink.service.PrometheusSinkService;
import org.opensearch.dataprepper.plugins.sink.service.WebhookService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;

@DataPrepperPlugin(name = "prometheus", pluginType = Sink.class, pluginConfigurationType = PrometheusSinkConfiguration.class)
public class PrometheusSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSink.class);

    private static final String BUCKET = "bucket";
    private static final String KEY_PATH = "key_path_prefix";

    private WebhookService webhookService;

    private volatile boolean sinkInitialized;

    private final PrometheusSinkService prometheusSinkService;

    private final BufferFactory bufferFactory;

    private DlqPushHandler dlqPushHandler;

    @DataPrepperPluginConstructor
    public PrometheusSink(final PluginSetting pluginSetting,
                    final PrometheusSinkConfiguration prometheusSinkConfiguration,
                    final PluginFactory pluginFactory,
                    final PipelineDescription pipelineDescription,
                    final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);
        final PluginModel codecConfiguration = prometheusSinkConfiguration.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        codecPluginSettings.setPipelineName(pipelineDescription.getPipelineName());
        this.sinkInitialized = Boolean.FALSE;
        if (prometheusSinkConfiguration.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
            this.bufferFactory = new LocalFileBufferFactory();
        } else {
            this.bufferFactory = new InMemoryBufferFactory();
        }

        this.dlqPushHandler = new DlqPushHandler(prometheusSinkConfiguration.getDlqFile(), pluginFactory,
                String.valueOf(prometheusSinkConfiguration.getDlqPluginSetting().get(BUCKET)),
                prometheusSinkConfiguration.getDlqStsRoleARN()
                ,prometheusSinkConfiguration.getDlqStsRegion(),
                String.valueOf(prometheusSinkConfiguration.getDlqPluginSetting().get(KEY_PATH)));

        final HttpRequestRetryStrategy httpRequestRetryStrategy = new DefaultHttpRequestRetryStrategy(prometheusSinkConfiguration.getMaxUploadRetries(),
                TimeValue.of(prometheusSinkConfiguration.getHttpRetryInterval()));

        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRetryStrategy(httpRequestRetryStrategy);

        if(Objects.nonNull(prometheusSinkConfiguration.getWebhookURL()))
            this.webhookService = new WebhookService(prometheusSinkConfiguration.getWebhookURL(),
                    httpClientBuilder,pluginMetrics,prometheusSinkConfiguration);

        if(prometheusSinkConfiguration.isAwsSigv4() && prometheusSinkConfiguration.isValidAWSUrl()){
            PrometheusSinkAwsService.attachSigV4(prometheusSinkConfiguration, httpClientBuilder, awsCredentialsSupplier);
        }
        this.prometheusSinkService = new PrometheusSinkService(
                prometheusSinkConfiguration,
                bufferFactory,
                dlqPushHandler,
                codecPluginSettings,
                webhookService,
                httpClientBuilder,
                pluginMetrics,
                pluginSetting);
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
        prometheusSinkService.output(records);
    }
}