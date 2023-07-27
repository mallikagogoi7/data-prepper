/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.service;

import io.micrometer.core.instrument.Counter;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;

import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.FailedHttpResponseInterceptor;
import org.opensearch.dataprepper.plugins.sink.HttpEndPointResponse;
import org.opensearch.dataprepper.plugins.sink.OAuthAccessTokenManager;
import org.opensearch.dataprepper.plugins.sink.ThresholdValidator;
import org.opensearch.dataprepper.plugins.sink.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.certificate.HttpClientSSLConnectionManager;
import org.opensearch.dataprepper.plugins.sink.configuration.AuthTypeOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.HTTPMethodOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.handler.BasicAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.BearerTokenAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;
import org.opensearch.dataprepper.plugins.sink.handler.MultiAuthPrometheusSinkHandler;
import org.opensearch.dataprepper.plugins.sink.util.HttpSinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.http.HttpHeaders.AUTHORIZATION;

/**
 * This service class contains logic for sending data to Http Endpoints
 */
public class PrometheusSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusSinkService.class);

    public static final String HTTP_SINK_RECORDS_SUCCESS_COUNTER = "httpSinkRecordsSuccessPushToEndPoint";

    public static final String HTTP_SINK_RECORDS_FAILED_COUNTER = "httpSinkRecordsFailedToPushEndPoint";

    private final Collection<EventHandle> bufferedEventHandles;

    private final PrometheusSinkConfiguration prometheusSinkConfiguration;

    private final BufferFactory bufferFactory;

    private final Map<String,HttpAuthOptions> httpAuthOptions;

    private DlqPushHandler dlqPushHandler;

    private final PluginSetting pluginSetting;

    private final Lock reentrantLock;

    private final HttpClientBuilder httpClientBuilder;

    private final int maxEvents;

    private final ByteCount maxBytes;

    private final long maxCollectionDuration;

    private final Counter httpSinkRecordsSuccessCounter;

    private final Counter httpSinkRecordsFailedCounter;

    private final OAuthAccessTokenManager oAuthRefreshTokenManager;

    private CertificateProviderFactory certificateProviderFactory;

    private WebhookService webhookService;

    private HttpClientConnectionManager httpClientConnectionManager;

    private Buffer currentBuffer;

    private final PluginSetting httpPluginSetting;

    private MultiAuthPrometheusSinkHandler multiAuthPrometheusSinkHandler;

    public PrometheusSinkService(final PrometheusSinkConfiguration prometheusSinkConfiguration,
                                 final BufferFactory bufferFactory,
                                 final DlqPushHandler dlqPushHandler,
                                 final PluginSetting pluginSetting,
                                 final WebhookService webhookService,
                                 final HttpClientBuilder httpClientBuilder,
                                 final PluginMetrics pluginMetrics,
                                 final PluginSetting httpPluginSetting){
        this.prometheusSinkConfiguration = prometheusSinkConfiguration;
        this.bufferFactory = bufferFactory;
        this.dlqPushHandler = dlqPushHandler;
        this.pluginSetting = pluginSetting;
        this.reentrantLock = new ReentrantLock();
        this.webhookService = webhookService;
        this.bufferedEventHandles = new LinkedList<>();
        this.httpClientBuilder = httpClientBuilder;
        this.maxEvents = prometheusSinkConfiguration.getThresholdOptions().getEventCount();
        this.maxBytes = prometheusSinkConfiguration.getThresholdOptions().getMaximumSize();
        this.maxCollectionDuration = prometheusSinkConfiguration.getThresholdOptions().getEventCollectTimeOut().getSeconds();
        this.httpPluginSetting = httpPluginSetting;
		this.oAuthRefreshTokenManager = new OAuthAccessTokenManager(httpClientBuilder);
        if (prometheusSinkConfiguration.isSsl() || prometheusSinkConfiguration.useAcmCertForSSL()) {
            this.certificateProviderFactory = new CertificateProviderFactory(prometheusSinkConfiguration);
            prometheusSinkConfiguration.validateAndInitializeCertAndKeyFileInS3();
            this.httpClientConnectionManager = new HttpClientSSLConnectionManager()
                    .createHttpClientConnectionManager(prometheusSinkConfiguration, certificateProviderFactory);
        }
        this.httpAuthOptions = buildAuthHttpSinkObjectsByConfig(prometheusSinkConfiguration);
        this.httpSinkRecordsSuccessCounter = pluginMetrics.counter(HTTP_SINK_RECORDS_SUCCESS_COUNTER);
        this.httpSinkRecordsFailedCounter = pluginMetrics.counter(HTTP_SINK_RECORDS_FAILED_COUNTER);
    }

    /**
     * This method process buffer records and send to Http End points based on configured codec
     * @param records Collection of Event
     */
    public void output(Collection<Record<Event>> records) {
        reentrantLock.lock();
        if (currentBuffer == null) {
            this.currentBuffer = bufferFactory.getBuffer();
        }
        try {
            records.forEach(record -> {
                final Event event = record.getData();
                try {
                    currentBuffer.writeEvent(event.toJsonString().getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (event.getEventHandle() != null) {
                    this.bufferedEventHandles.add(event.getEventHandle());
                }
                if (ThresholdValidator.checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration)) {
                    final HttpEndPointResponse failedHttpEndPointResponses = pushToEndPoint(getCurrentBufferData(currentBuffer));
                    if (failedHttpEndPointResponses != null) {
                        logFailedData(failedHttpEndPointResponses);
                    } else {
                        LOG.info("data pushed to the end point successfully");
                    }
                    currentBuffer = bufferFactory.getBuffer();
                    releaseEventHandles(Boolean.TRUE);

                }});

        }finally {
            reentrantLock.unlock();
        }
    }

    private byte[] getCurrentBufferData(final Buffer currentBuffer) {
        try {
            return currentBuffer.getSinkBufferData();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * * This method logs Failed Data to DLQ and Webhook
     *  @param endPointResponses HttpEndPointResponses.
     */
    private void logFailedData(final HttpEndPointResponse endPointResponses) {
        FailedDlqData failedDlqData =
                FailedDlqData.builder()
                        .withUrl(endPointResponses.getUrl())
                        .withMessage(endPointResponses.getErrMessage())
                        .withStatus(endPointResponses.getStatusCode()).build();
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

    /**
     * * This method pushes bufferData to configured HttpEndPoints
     *  @param currentBufferData bufferData.
     */
    private HttpEndPointResponse pushToEndPoint(final byte[] currentBufferData) {
        HttpEndPointResponse httpEndPointResponses = null;
        final ClassicRequestBuilder classicHttpRequestBuilder =
                httpAuthOptions.get(prometheusSinkConfiguration.getUrl()).getClassicHttpRequestBuilder();
        classicHttpRequestBuilder.setEntity(new String(currentBufferData));
        try {
           if(AuthTypeOptions.BEARER_TOKEN.equals(prometheusSinkConfiguration.getAuthType()))
                    refreshTokenIfExpired(classicHttpRequestBuilder.getFirstHeader(AUTHORIZATION).getValue(),prometheusSinkConfiguration.getUrl());

            httpAuthOptions.get(prometheusSinkConfiguration.getUrl()).getHttpClientBuilder().build()
                    .execute(classicHttpRequestBuilder.build(), HttpClientContext.create());
            LOG.info("No of Records successfully pushed to endpoint {}", prometheusSinkConfiguration.getUrl() +" " + currentBuffer.getEventCount());
            httpSinkRecordsSuccessCounter.increment(currentBuffer.getEventCount());
        } catch (IOException e) {
            httpSinkRecordsFailedCounter.increment(currentBuffer.getEventCount());
            LOG.info("No of Records failed to push endpoint {}",currentBuffer.getEventCount());
            LOG.error("Exception while pushing buffer data to end point. URL : {}, Exception : ", prometheusSinkConfiguration.getUrl(), e);
            httpEndPointResponses = new HttpEndPointResponse(prometheusSinkConfiguration.getUrl(), HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return httpEndPointResponses;
    }

    /**
     * * This method sends Failed objects to DLQ
     *  @param failedDlqData FailedDlqData.
     */
    private void logFailureForDlqObjects(final FailedDlqData failedDlqData){
        dlqPushHandler.perform(httpPluginSetting, failedDlqData);
    }

    /**
     * * This method push Failed objects to Webhook
     *  @param failedDlqData FailedDlqData.
     */
    private void logFailureForWebHook(final FailedDlqData failedDlqData){
        webhookService.pushWebhook(failedDlqData);
    }

    /**
     * * This method gets Auth Handler classes based on configuration
     *  @param authType AuthTypeOptions.
     *  @param authOptions HttpAuthOptions.Builder.
     */
    private HttpAuthOptions getAuthHandlerByConfig(final AuthTypeOptions authType,
                                                   final HttpAuthOptions.Builder authOptions){
        switch(authType) {
            case HTTP_BASIC:
                multiAuthPrometheusSinkHandler = new BasicAuthPrometheusSinkHandler(
                        prometheusSinkConfiguration.getAuthentication().getHttpBasic().getUsername(),
                        prometheusSinkConfiguration.getAuthentication().getHttpBasic().getPassword(),
                         httpClientConnectionManager);
                break;
            case BEARER_TOKEN:
                multiAuthPrometheusSinkHandler = new BearerTokenAuthPrometheusSinkHandler(
                        prometheusSinkConfiguration.getAuthentication().getBearerTokenOptions(),
                        httpClientConnectionManager,oAuthRefreshTokenManager);
                break;
            case UNAUTHENTICATED:
            default:
                return authOptions.setHttpClientBuilder(httpClientBuilder
                        .setConnectionManager(httpClientConnectionManager)
                        .addResponseInterceptorLast(new FailedHttpResponseInterceptor(authOptions.getUrl()))).build();
        }
        return multiAuthPrometheusSinkHandler.authenticate(authOptions);
    }

    /**
     * * This method build HttpAuthOptions class based on configurations
     *  @param prometheusSinkConfiguration PrometheusSinkConfiguration.
     */
    private Map<String,HttpAuthOptions> buildAuthHttpSinkObjectsByConfig(final PrometheusSinkConfiguration prometheusSinkConfiguration){
        final Map<String,HttpAuthOptions> authMap = new HashMap<>();

        final HTTPMethodOptions httpMethod = prometheusSinkConfiguration.getHttpMethod();
        final AuthTypeOptions authType =  prometheusSinkConfiguration.getAuthType();
        final String proxyUrlString =  prometheusSinkConfiguration.getProxy();
        final ClassicRequestBuilder classicRequestBuilder = buildRequestByHTTPMethodType(httpMethod).setUri(prometheusSinkConfiguration.getUrl());



        if(Objects.nonNull(prometheusSinkConfiguration.getCustomHeaderOptions()))
            addCustomHeaders(classicRequestBuilder,prometheusSinkConfiguration.getCustomHeaderOptions());

        if(Objects.nonNull(proxyUrlString)) {
            httpClientBuilder.setProxy(HttpSinkUtil.getHttpHostByURL(HttpSinkUtil.getURLByUrlString(proxyUrlString)));
            LOG.info("sending data via proxy {}",proxyUrlString);
        }

        final HttpAuthOptions.Builder authOptions = new HttpAuthOptions.Builder()
                .setUrl(prometheusSinkConfiguration.getUrl())
                .setClassicHttpRequestBuilder(classicRequestBuilder)
                .setHttpClientBuilder(httpClientBuilder);

        authMap.put(prometheusSinkConfiguration.getUrl(),getAuthHandlerByConfig(authType,authOptions));
        return authMap;
    }

    /**
     * * This method adds SageMakerHeaders as custom Header in the request
     *  @param classicRequestBuilder ClassicRequestBuilder.
     *  @param customHeaderOptions CustomHeaderOptions .
     */
    private void addCustomHeaders(final ClassicRequestBuilder classicRequestBuilder,
                                  final Map<String, List<String>> customHeaderOptions) {

        customHeaderOptions.forEach((k, v) -> classicRequestBuilder.addHeader(k,v.toString()));
    }

    /**
     * * builds ClassicRequestBuilder based on configured HttpMethod
     *  @param httpMethodOptions Http Method.
     */
    private ClassicRequestBuilder buildRequestByHTTPMethodType(final HTTPMethodOptions httpMethodOptions) {
        final ClassicRequestBuilder classicRequestBuilder;
        switch (httpMethodOptions) {
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

    private void refreshTokenIfExpired(final String token,final String url){
        if(oAuthRefreshTokenManager.isTokenExpired(token)) {
            httpAuthOptions.get(url).getClassicHttpRequestBuilder()
                    .setHeader(AUTHORIZATION, oAuthRefreshTokenManager.getAccessToken(prometheusSinkConfiguration.getAuthentication().getBearerTokenOptions()));
        }
    }
}