/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
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
import org.opensearch.dataprepper.plugins.sink.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.certificate.HttpClientSSLConnectionManager;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.configuration.CustomHeaderOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.opensearch.dataprepper.plugins.sink.handler.BasicAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.BearerTokenAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.handler.MultiAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.service.HttpSinkService;
import org.opensearch.dataprepper.plugins.sink.service.WebhookService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@DataPrepperPlugin(name = "http", pluginType = Sink.class, pluginConfigurationType = HttpSinkConfiguration.class)
public class HTTPSink extends AbstractSink<Record<Event>> {

    public static final String X_AMZN_SAGE_MAKER_CUSTOM_ATTRIBUTES = "X-Amzn-SageMaker-Custom-Attributes";
    private static final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);
    public static final String X_AMZN_SAGE_MAKER_INFERENCE_ID = "X-Amzn-SageMaker-Inference-Id";
    public static final String X_AMZN_SAGE_MAKER_ENABLE_EXPLANATIONS = "X-Amzn-SageMaker-Enable-Explanations";
    public static final String X_AMZN_SAGE_MAKER_TARGET_VARIANT = "X-Amzn-SageMaker-Target-Variant";
    public static final String X_AMZN_SAGE_MAKER_TARGET_MODEL = "X-Amzn-SageMaker-Target-Model";
    public static final String X_AMZN_SAGE_MAKER_TARGET_CONTAINER_HOSTNAME = "X-Amzn-SageMaker-Target-Container-Hostname";
    public static final String PUT = "PUT";
    public static final String POST = "POST";
    public static final String HTTP_BASIC = "http_basic";
    public static final String BEARER_TOKEN = "bearer_token";
    public static final String MTLS = "mtls";
    public static final String UNAUTHENTICATED = "unauthenticated";

    private final HttpSinkConfiguration httpSinkConfiguration;

    private volatile boolean sinkInitialized;

    private final Codec codec;

    private HttpSinkService httpSinkService;

    private final BufferFactory bufferFactory;

    private Buffer currentBuffer;

    private final CertificateProviderFactory certificateProviderFactory;

    private final DLQSink dlqSink;

    @DataPrepperPluginConstructor
    public HTTPSink(final PluginSetting pluginSetting,
                    final HttpSinkConfiguration httpSinkConfiguration,
                    final PluginFactory pluginFactory,
                    final PipelineDescription pipelineDescription) {
        super(pluginSetting);
        this.httpSinkConfiguration = httpSinkConfiguration;
        final PluginModel codecConfiguration = httpSinkConfiguration.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        codecPluginSettings.setPipelineName(pipelineDescription.getPipelineName());
        codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
        sinkInitialized = Boolean.FALSE;
        if (httpSinkConfiguration.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
            bufferFactory = new LocalFileBufferFactory();
        } else {
            bufferFactory = new InMemoryBufferFactory();
        }
        this.certificateProviderFactory = new CertificateProviderFactory(httpSinkConfiguration);
        httpSinkConfiguration.validateAndInitializeCertAndKeyFileInS3();
        dlqSink = new DLQSink(pluginFactory,httpSinkConfiguration);
        this.httpSinkService = new HttpSinkService(codec,httpSinkConfiguration,
                bufferFactory,buildAuthHttpSinkObjectsByConfig(httpSinkConfiguration),
                dlqSink, codecPluginSettings);
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
        httpSinkService.processRecords(records);
    }

    private HttpAuthOptions getAuthHandlerByConfig(final String authType,
                                                   final HttpAuthOptions authOptions){
        HttpClientConnectionManager httpClientConnectionManager = null;
        if (httpSinkConfiguration.isSsl() || httpSinkConfiguration.useAcmCertForSSL()) {
            httpClientConnectionManager = new HttpClientSSLConnectionManager()
                    .createHttpClientConnectionManager(httpSinkConfiguration, certificateProviderFactory);
        }
        MultiAuthHttpSinkHandler multiAuthHttpSinkHandler = null;
        // TODO: AWS Sigv4 - check
        switch(authType){
            case HTTP_BASIC:
                multiAuthHttpSinkHandler = new BasicAuthHttpSinkHandler(httpSinkConfiguration,httpClientConnectionManager);
                break;
            case BEARER_TOKEN:
                multiAuthHttpSinkHandler = new BearerTokenAuthHttpSinkHandler(httpClientConnectionManager);
                break;
            case UNAUTHENTICATED:
            default:
                return authOptions.setCloseableHttpClient(HttpClients.createMinimal());
        }
        return multiAuthHttpSinkHandler.authenticate(authOptions);
    }

    private Map<String,HttpAuthOptions> buildAuthHttpSinkObjectsByConfig(final HttpSinkConfiguration httpSinkConfiguration){
        final List<UrlConfigurationOption> urlConfigurationOptions = httpSinkConfiguration.getUrlConfigurationOptions();

        final Map<String,HttpAuthOptions> authMap = new HashMap<>(urlConfigurationOptions.size());
        urlConfigurationOptions.forEach( urlOption -> {
            final HttpAuthOptions authOptions = new HttpAuthOptions();
            final String httpMethodString = Objects.nonNull(urlOption.getHttpMethod()) ? urlOption.getHttpMethod() : httpSinkConfiguration.getHttpMethod();
            final String authType = Objects.nonNull(urlOption.getAuthType()) ? urlOption.getAuthType() : httpSinkConfiguration.getAuthType();
            final String proxyUrlString =  Objects.nonNull(urlOption.getProxy()) ? urlOption.getProxy() : httpSinkConfiguration.getProxy();

            final ClassicRequestBuilder classicRequestBuilder = buildRequestByHTTPMethodType(httpMethodString).setUri(urlOption.getUrl());

            if(Objects.nonNull(httpSinkConfiguration.getCustomHeaderOptions()))
                addSageMakerHeaders(classicRequestBuilder,httpSinkConfiguration.getCustomHeaderOptions());

            authOptions.setUrl(urlOption.getUrl());
            authOptions.setProxy(proxyUrlString);
            authOptions.setClassicHttpRequestBuilder(classicRequestBuilder);

            authMap.put(urlOption.getUrl(),getAuthHandlerByConfig(authType,authOptions));
        });
        return authMap;

    }

    private void addSageMakerHeaders(final ClassicRequestBuilder classicRequestBuilder,
                                               final CustomHeaderOptions customHeaderOptions) {
        classicRequestBuilder.addHeader(X_AMZN_SAGE_MAKER_CUSTOM_ATTRIBUTES,customHeaderOptions.getCustomAttributes());
        classicRequestBuilder.addHeader(X_AMZN_SAGE_MAKER_INFERENCE_ID,customHeaderOptions.getInferenceId());
        classicRequestBuilder.addHeader(X_AMZN_SAGE_MAKER_ENABLE_EXPLANATIONS,customHeaderOptions.getEnableExplanations());
        classicRequestBuilder.addHeader(X_AMZN_SAGE_MAKER_TARGET_VARIANT,customHeaderOptions.getTargetVariant());
        classicRequestBuilder.addHeader(X_AMZN_SAGE_MAKER_TARGET_MODEL,customHeaderOptions.getTargetModel());
        classicRequestBuilder.addHeader(X_AMZN_SAGE_MAKER_TARGET_CONTAINER_HOSTNAME,customHeaderOptions.getTargetContainerHostname());
    }

    private ClassicRequestBuilder buildRequestByHTTPMethodType(final String httpMethod) {
        final ClassicRequestBuilder classicRequestBuilder;
        switch(httpMethod){
            case PUT:
                classicRequestBuilder = ClassicRequestBuilder.put();
                break;
            case POST:
            default:
                classicRequestBuilder = ClassicRequestBuilder.post();
                break;
        }
        return classicRequestBuilder;
    }

}