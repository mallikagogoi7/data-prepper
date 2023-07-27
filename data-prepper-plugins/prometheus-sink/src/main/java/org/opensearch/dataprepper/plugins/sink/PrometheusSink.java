/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;

import org.opensearch.dataprepper.plugins.sink.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.service.PrometheusSinkService;
import org.opensearch.dataprepper.plugins.sink.service.WebhookService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

@DataPrepperPlugin(name = "prometheus", pluginType = Sink.class, pluginConfigurationType = PrometheusSinkConfiguration.class)
public class PrometheusSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSink.class);

    private static final String BUCKET = "bucket";
    private static final String KEY_PATH = "key_path_prefix";

    private WebhookService webhookService;

    private volatile boolean sinkInitialized;

    private final PrometheusSinkService prometheusSinkService;

    @DataPrepperPluginConstructor
    public PrometheusSink(final PluginSetting pluginSetting,
                    final PrometheusSinkConfiguration prometheusSinkConfiguration,
                    final PluginFactory pluginFactory,
                    final PipelineDescription pipelineDescription,
                    final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);
        this.sinkInitialized = Boolean.FALSE;
        this.prometheusSinkService = new PrometheusSinkService(
                prometheusSinkConfiguration,
                webhookService);
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