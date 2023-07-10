/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.dlq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;

public class HttpSinkDlqUtilTest {

    private static final String BUCKET = "bucket";
    private static final String BUCKET_VALUE = "test";
    private static final String ROLE = "arn:aws:iam::524239988944:role/app-test";

    private static final String REGION = "ap-south-1";
    private static final String S3_PLUGIN_NAME = "s3";
    private static final String KEY_PATH_PREFIX = "key_path_prefix";

    private static final String KEY_PATH_PREFIX_VALUE = "dlq/";

    private static final String PIPELINE_NAME = "log-pipeline";

    private PluginModel pluginModel;

    private HttpSinkDlqUtil httpSinkDlqUtil;
    private PluginFactory pluginFactory;

    private AwsAuthenticationOptions awsAuthenticationOptions;

    private HttpSinkConfiguration httpSinkConfiguration;

    private DlqProvider dlqProvider;

    private DlqWriter dlqWriter;

    @BeforeEach
    public void setUp() throws Exception{
        pluginFactory = mock(PluginFactory.class);
        httpSinkConfiguration = mock(HttpSinkConfiguration.class);
        pluginModel = mock(PluginModel.class);
        awsAuthenticationOptions =  mock(AwsAuthenticationOptions.class);
        dlqProvider = mock(DlqProvider.class);
        dlqWriter = mock(DlqWriter.class);
    }

    @Test
    public void performTest() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put(BUCKET,BUCKET_VALUE);
        props.put(KEY_PATH_PREFIX,KEY_PATH_PREFIX_VALUE);

        when(pluginModel.getPluginSettings()).thenReturn(props);
        when(httpSinkConfiguration.getDlq()).thenReturn(pluginModel);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(ROLE);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(REGION));
        when(httpSinkConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(httpSinkConfiguration.getDlq()).thenReturn(pluginModel);
        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(dlqProvider);

        when(dlqProvider.getDlqWriter(Mockito.anyString())).thenReturn(Optional.of(dlqWriter));
        doNothing().when(dlqWriter).write(anyList(), anyString(), anyString());
        FailedDlqData failedDlqData = FailedDlqData.builder().build();
        httpSinkDlqUtil = new HttpSinkDlqUtil(pluginFactory, httpSinkConfiguration);

        PluginSetting pluginSetting = new PluginSetting(S3_PLUGIN_NAME, props);
        pluginSetting.setPipelineName(PIPELINE_NAME);
        httpSinkDlqUtil.perform(pluginSetting, failedDlqData);
        verify(dlqWriter).write(anyList(), anyString(), anyString());
    }
}
