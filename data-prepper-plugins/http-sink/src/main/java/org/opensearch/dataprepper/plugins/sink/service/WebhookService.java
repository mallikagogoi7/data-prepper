package org.opensearch.dataprepper.plugins.sink.service;

import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.opensearch.dataprepper.plugins.sink.dlq.FailedDlqData;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

public class WebhookService {

    private URL url;

    public WebhookService(final String url){
        try {
            this.url = new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public void pushWebhook(final FailedDlqData failedDlqData){
        final HttpHost targetHost;
        final CloseableHttpResponse webhookResp;
        try {
            targetHost = new HttpHost(url.toURI().getScheme(), url.getHost(), url.getPort());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        final ClassicRequestBuilder classicHttpRequestBuilder =
                ClassicRequestBuilder.post().setEntity(failedDlqData.toString()).setUri(url.toString());
        try {
            webhookResp = HttpClients.createDefault()
                    .execute(targetHost, classicHttpRequestBuilder.build(), HttpClientContext.create());
            if(webhookResp.getCode() != HttpStatus.SC_OK || webhookResp.getCode() != HttpStatus.SC_CREATED){
                // call backoff till get the success 200
            }
        } catch (IOException e) {
            // call backoff incase any exception
        }

    }
}
