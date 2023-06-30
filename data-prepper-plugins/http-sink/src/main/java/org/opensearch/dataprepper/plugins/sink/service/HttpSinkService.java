/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.certificate.HttpClientSSLConnectionManager;
import org.opensearch.dataprepper.plugins.sink.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.CustomHeaderOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.opensearch.dataprepper.plugins.sink.dlq.DLQSink;
import org.opensearch.dataprepper.plugins.sink.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.handler.BasicAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.BearerTokenAuthHttpSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.handler.MultiAuthHttpSinkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HttpSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkService.class);

    public static final TimeValue DEFAULT_HTTP_RETRY_INTERVAL = TimeValue.ofSeconds(30);

    public static final int HTTP_MAX_RETRIES = 5;

    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();

    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();

    static final double JITTER_RATE = 0.20;

    public static final String X_AMZN_SAGE_MAKER_CUSTOM_ATTRIBUTES = "X-Amzn-SageMaker-Custom-Attributes";

    public static final String X_AMZN_SAGE_MAKER_INFERENCE_ID = "X-Amzn-SageMaker-Inference-Id";

    public static final String X_AMZN_SAGE_MAKER_ENABLE_EXPLANATIONS = "X-Amzn-SageMaker-Enable-Explanations";

    public static final String X_AMZN_SAGE_MAKER_TARGET_VARIANT = "X-Amzn-SageMaker-Target-Variant";

    public static final String X_AMZN_SAGE_MAKER_TARGET_MODEL = "X-Amzn-SageMaker-Target-Model";

    public static final String X_AMZN_SAGE_MAKER_TARGET_CONTAINER_HOSTNAME = "X-Amzn-SageMaker-Target-Container-Hostname";

    private final Codec codec;

    private final Collection<EventHandle> bufferedEventHandles;

    private final HttpSinkConfiguration httpSinkConfiguration;

    private final BufferFactory bufferFactory;

    private final Map<String,HttpAuthOptions> httpAuthOptions;

    private final DLQSink dlqSink;

    private final PluginSetting pluginSetting;

    private final Lock reentrantLock;
    private final CertificateProviderFactory certificateProviderFactory;

    private final HttpClientBuilder httpClientBuilder;

    private final int maxEvents;

    private final ByteCount maxBytes;

    private final long maxCollectionDuration;

    private WebhookService webhookService;

    private HttpClientConnectionManager httpClientConnectionManager;

    private Buffer currentBuffer;

    public HttpSinkService(final Codec codec,
                           final HttpSinkConfiguration httpSinkConfiguration,
                           final BufferFactory bufferFactory,
                           final CertificateProviderFactory certificateProviderFactory,
                           final DLQSink dlqSink,
                           final PluginSetting pluginSetting,
                           final WebhookService webhookService){
        this.codec= codec;
        this.httpSinkConfiguration = httpSinkConfiguration;
        this.bufferFactory = bufferFactory;
        this.httpAuthOptions = buildAuthHttpSinkObjectsByConfig(httpSinkConfiguration);
        this.dlqSink = dlqSink;
        this.pluginSetting = pluginSetting;
        reentrantLock = new ReentrantLock();
        this.webhookService = webhookService;
        this.certificateProviderFactory = certificateProviderFactory;
        this.bufferedEventHandles = new LinkedList<>();

        this.maxEvents = httpSinkConfiguration.getThresholdOptions().getEventCount();
        this.maxBytes = httpSinkConfiguration.getThresholdOptions().getMaximumSize();
        this.maxCollectionDuration = httpSinkConfiguration.getThresholdOptions().getEventCollectTimeOut().getSeconds();

        if (httpSinkConfiguration.isSsl() || httpSinkConfiguration.useAcmCertForSSL()) {
            httpClientConnectionManager = new HttpClientSSLConnectionManager()
                    .createHttpClientConnectionManager(httpSinkConfiguration, certificateProviderFactory);
        }
        final HttpRequestRetryStrategy httpRequestRetryStrategy = new DefaultHttpRequestRetryStrategy(HTTP_MAX_RETRIES,
                DEFAULT_HTTP_RETRY_INTERVAL);

        this.httpClientBuilder = HttpClients.custom()
                .setRetryStrategy(httpRequestRetryStrategy);
    }

    public void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        if (currentBuffer == null) {
            this.currentBuffer = bufferFactory.getBuffer();
        }
        records.forEach(record -> {
            final Event event = record.getData();
            try {
                currentBuffer.writeEvent(event.toJsonString().getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if(event.getEventHandle() != null) {
                this.bufferedEventHandles.add(event.getEventHandle());
            }
            if(checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration)){
                final List<HttpEndPointResponse> failedHttpEndPointResponses = pushToEndPoint(getCurrentBufferData(currentBuffer));
                if(!failedHttpEndPointResponses.isEmpty()) {
                    logFailedData(failedHttpEndPointResponses, getCurrentBufferData(currentBuffer));
                } else {
                    LOG.info("data pushed to all the end points successfully");
                }
                currentBuffer = bufferFactory.getBuffer();
                releaseEventHandles(Boolean.TRUE);
                }
        });
        reentrantLock.unlock();
    }

    private byte[] getCurrentBufferData(final Buffer currentBuffer) {
        try {
            return currentBuffer.getSinkBufferData();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void logFailedData(final List<HttpEndPointResponse> endPointResponses, final byte[] currentBufferData) {
        FailedDlqData failedDlqData =
                FailedDlqData.builder().withBufferData(new String(currentBufferData)).withEndPointResponses(endPointResponses).build();
        LOG.info("Failed to push the data. Failed DLQ Data: {}",failedDlqData);
        logFailureForDlqObjects(failedDlqData);
        if(Objects.nonNull(webhookService)){
            logFailureForWebHook(failedDlqData);
        }
    }

    private void releaseEventHandles(final boolean result) {
        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }
        bufferedEventHandles.clear();
    }

    private List<HttpEndPointResponse> pushToEndPoint(final byte[] currentBufferData) {
            List<HttpEndPointResponse> httpEndPointResponses = new ArrayList<>(httpSinkConfiguration.getUrlConfigurationOptions().size());
        for(UrlConfigurationOption urlConfOption: httpSinkConfiguration.getUrlConfigurationOptions()) {
            final ClassicRequestBuilder classicHttpRequestBuilder =
                    httpAuthOptions.get(urlConfOption.getUrl()).getClassicHttpRequestBuilder();
            classicHttpRequestBuilder.setEntity(new String(currentBufferData));
            final CloseableHttpResponse response;
            try {
                if(httpAuthOptions.get(urlConfOption.getUrl()).getProxy() != null) {
                    LOG.info("Execution using proxy");
                    URL targetUrl = new URL(urlConfOption.getUrl());
                    final HttpHost targetHost = new HttpHost(targetUrl.toURI().getScheme(), targetUrl.getHost(), targetUrl.getPort());

                    URL proxyUrl = new URL(urlConfOption.getProxy());
                    final HttpHost proxyHost = new HttpHost(proxyUrl.toURI().getScheme(), proxyUrl.getHost(), proxyUrl.getPort());

                    response = httpAuthOptions.get(urlConfOption.getUrl()).getHttpClientBuilder().setProxy(proxyHost).build()
                            .execute(targetHost, classicHttpRequestBuilder.build(), HttpClientContext.create());
                }else {
                    response = httpAuthOptions.get(urlConfOption.getUrl()).getHttpClientBuilder().build()
                            .execute(classicHttpRequestBuilder.build(), HttpClientContext.create());
                }

            } catch (IOException e) {
                LOG.error("Exception while pushing buffer data to end point. URL : {}, Exception : ",urlConfOption.getUrl(),e);
                httpEndPointResponses.add(new HttpEndPointResponse(urlConfOption.getUrl(),HttpStatus.SC_INTERNAL_SERVER_ERROR,e.getMessage()));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }


        }
        return httpEndPointResponses;
    }

    public static boolean checkThresholdExceed(final Buffer currentBuffer,
                                               final int maxEvents,
                                               final ByteCount maxBytes,
                                               final long maxCollectionDuration) {
        if (maxEvents > 0) {
            return currentBuffer.getEventCount() + 1 > maxEvents ||
                    currentBuffer.getDuration() > maxCollectionDuration ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        } else {
            return currentBuffer.getDuration() > maxCollectionDuration ||
                    currentBuffer.getSize() > maxBytes.getBytes();
        }
    }

    private void logFailureForDlqObjects(final FailedDlqData failedDlqData){
            dlqSink.perform(pluginSetting, failedDlqData);
    }

    private void logFailureForWebHook(final FailedDlqData failedDlqData){
        webhookService.pushWebhook(failedDlqData);
    }

    private HttpAuthOptions getAuthHandlerByConfig(final AuthTypeOptions authType,
                                                   final HttpAuthOptions.Builder authOptions){
        MultiAuthHttpSinkHandler multiAuthHttpSinkHandler = null;
        // TODO: AWS Sigv4 - check
        switch(authType) {
            case HTTP_BASIC:
                multiAuthHttpSinkHandler = new BasicAuthHttpSinkHandler(httpSinkConfiguration,httpClientConnectionManager);
                break;
            case BEARER_TOKEN:
                multiAuthHttpSinkHandler = new BearerTokenAuthHttpSinkHandler(httpSinkConfiguration,httpClientConnectionManager);
                break;
            case UNAUTHENTICATED:
            default:
                return authOptions.setHttpClientBuilder(HttpClients.custom()
                        .addResponseInterceptorLast(new FailedHttpResponseInterceptor(authOptions.getUrl()))).build();
        }
        return multiAuthHttpSinkHandler.authenticate(authOptions);
    }

    private Map<String,HttpAuthOptions> buildAuthHttpSinkObjectsByConfig(final HttpSinkConfiguration httpSinkConfiguration){
        final List<UrlConfigurationOption> urlConfigurationOptions = httpSinkConfiguration.getUrlConfigurationOptions();
        final Map<String,HttpAuthOptions> authMap = new HashMap<>(urlConfigurationOptions.size());
        urlConfigurationOptions.forEach( urlOption -> {
            final HTTPMethodOptions httpMethod = Objects.nonNull(urlOption.getHttpMethod()) ? urlOption.getHttpMethod() : httpSinkConfiguration.getHttpMethod();
            final AuthTypeOptions authType = Objects.nonNull(urlOption.getAuthType()) ? urlOption.getAuthType() : httpSinkConfiguration.getAuthType();
            final String proxyUrlString =  Objects.nonNull(urlOption.getProxy()) ? urlOption.getProxy() : httpSinkConfiguration.getProxy();
            final ClassicRequestBuilder classicRequestBuilder = buildRequestByHTTPMethodType(httpMethod).setUri(urlOption.getUrl());

            if(Objects.nonNull(httpSinkConfiguration.getCustomHeaderOptions()))
                addSageMakerHeaders(classicRequestBuilder,httpSinkConfiguration.getCustomHeaderOptions());

            final HttpAuthOptions.Builder authOptions = new HttpAuthOptions.Builder()
                    .setUrl(urlOption.getUrl())
                    .setProxy(proxyUrlString).setClassicHttpRequestBuilder(classicRequestBuilder)
                    .setHttpClientBuilder(httpClientBuilder);

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

    private ClassicRequestBuilder buildRequestByHTTPMethodType(final HTTPMethodOptions httpMethodOptions) {
        final ClassicRequestBuilder classicRequestBuilder;
        switch(httpMethodOptions){
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
