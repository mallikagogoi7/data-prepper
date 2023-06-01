/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collection;

/**
 * Implementation class of http-sink plugin. It is responsible for receive the collection of
 * {@link Event} and upload to http resources based on thresholds configured.
 */
@DataPrepperPlugin(name = "http-sink", pluginType = Sink.class, pluginConfigurationType = HttpSinkConfiguration.class)
public class HttpSink extends AbstractSink<Record<Event>>
{

    private HttpSinkConfiguration httpSinkConfiguration;

    /**
     *
     * @param pluginSetting
     * @param httpSinkConfiguration
     * @param pluginFactory
     */
    @DataPrepperPluginConstructor
    public HttpSink(final PluginSetting pluginSetting, final HttpSinkConfiguration httpSinkConfiguration,
                  final PluginFactory pluginFactory) {
        super(pluginSetting);
        this.httpSinkConfiguration = httpSinkConfiguration;

    }

    @Override
    public void doInitialize() {

    }

    @Override
    public void doOutput(Collection<Record<Event>> records) {

    }

    @Override
    public boolean isReady() {
        return false;
    }
}