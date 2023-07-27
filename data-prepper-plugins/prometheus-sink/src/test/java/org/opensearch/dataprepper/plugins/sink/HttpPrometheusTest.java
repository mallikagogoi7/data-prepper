package org.opensearch.dataprepper.plugins.sink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpPrometheusTest {

     PrometheusSink prometheusSink;

    private PluginSetting pluginSetting;
    private PluginFactory pluginFactory;

    private PrometheusSinkConfiguration prometheusSinkConfiguration;

    private PipelineDescription pipelineDescription;

    private AwsCredentialsSupplier awsCredentialsSupplier;

    private SinkContext sinkContext;

    private ThresholdOptions thresholdOptions;

    private AwsAuthenticationOptions awsAuthenticationOptions;

    private OutputCodec codec;

    private HttpAuthOptions httpAuthOptions;

    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);
        pluginFactory = mock(PluginFactory.class);
        prometheusSinkConfiguration = mock(PrometheusSinkConfiguration.class);
        pipelineDescription = mock(PipelineDescription.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        thresholdOptions = mock(ThresholdOptions.class);
        sinkContext = mock(SinkContext.class);
        codec = mock(OutputCodec.class);
        httpAuthOptions = mock(HttpAuthOptions.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(pluginSetting.getPipelineName()).thenReturn("log-pipeline");
        PluginModel codecConfiguration = new PluginModel("http", new HashMap<>());
        when(prometheusSinkConfiguration.getCodec()).thenReturn(codecConfiguration);
        when(prometheusSinkConfiguration.getBufferType()).thenReturn(BufferTypeOptions.LOCALFILE);
        when(httpAuthOptions.getUrl()).thenReturn("http://localhost:8080");
        when(prometheusSinkConfiguration.getHttpMethod()).thenReturn(HTTPMethodOptions.POST);
        when(prometheusSinkConfiguration.getAuthType()).thenReturn(AuthTypeOptions.UNAUTHENTICATED);
        Map<String, Object> dlqSetting = new HashMap<>();
        dlqSetting.put("bucket", "dlq.test");
        dlqSetting.put("key_path_prefix", "\\dlq");
        PluginModel dlq = new PluginModel("s3",dlqSetting);
        when(prometheusSinkConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(prometheusSinkConfiguration.getDlqStsRoleARN()).thenReturn("arn:aws:iam::1234567890:role/app-test");
        when(prometheusSinkConfiguration.getDlqStsRegion()).thenReturn("ap-south-1");
        when(prometheusSinkConfiguration.getDlq()).thenReturn(dlq);
        when(prometheusSinkConfiguration.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getEventCount()).thenReturn(10);
        when(prometheusSinkConfiguration.getDlqFile()).thenReturn("\\dlq");
    }

    private PrometheusSink createObjectUnderTest() {
        return new PrometheusSink(pluginSetting, prometheusSinkConfiguration, pluginFactory, pipelineDescription,
                awsCredentialsSupplier);
    }
    @Test
    void test_http_sink_plugin_isReady_positive() {
        prometheusSink = createObjectUnderTest();
        Assertions.assertNotNull(prometheusSink);
        prometheusSink.doInitialize();
        assertTrue(prometheusSink.isReady(), "http sink is initialized and ready to work");
    }

    @Test
    void test_http_Sink_plugin_isReady_negative() {
        prometheusSink = createObjectUnderTest();
        Assertions.assertNotNull(prometheusSink);
        assertFalse(prometheusSink.isReady(), "httpSink sink is not initialized and not ready to work");
    }

    @Test
    void test_doOutput_with_empty_records() {
        prometheusSink = createObjectUnderTest();
        Assertions.assertNotNull(prometheusSink);
        prometheusSink.doInitialize();
        Collection<Record<Event>> records = new ArrayList<>();
        prometheusSink.doOutput(records);
    }
}
