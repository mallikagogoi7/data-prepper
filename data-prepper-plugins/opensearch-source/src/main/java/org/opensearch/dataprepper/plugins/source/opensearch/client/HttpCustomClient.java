package org.opensearch.dataprepper.plugins.source.opensearch.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;

public class HttpCustomClient {

    private URL url;

    public HttpCustomClient(URL url) {
        this.url = url;
    }

    private ObjectMapper mapper = new ObjectMapper();

    public <T> T execute(final Class<T> responseType, final Object requestObject, final String httpMethod,final String uri) throws IOException {
        StringEntity requestEntity = new StringEntity(mapper.writeValueAsString(requestObject));
        URI httpUri = URI.create(url.getProtocol()+"://"+url.getAuthority()+"/"+uri);
        HttpUriRequestBase operationRequest = new HttpUriRequestBase(httpMethod, httpUri);
        operationRequest.setHeader("Accept", ContentType.APPLICATION_JSON);
        operationRequest.setHeader("Content-type", ContentType.APPLICATION_JSON);
        operationRequest.setEntity(requestEntity);

        CloseableHttpResponse closeableHttpResponse = getCloseableHttpResponse(operationRequest);
        T response = mapper.readValue(readBuffer(closeableHttpResponse).toString(), responseType);
        return response;
    }

    private StringBuffer readBuffer(CloseableHttpResponse pitCloseableResponse) throws IOException {
        StringBuffer result = new StringBuffer();
        BufferedReader reader = new BufferedReader(new InputStreamReader(pitCloseableResponse.getEntity().getContent()));
        String line = "";
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        return result;
    }

    private CloseableHttpResponse getCloseableHttpResponse(final HttpUriRequestBase operationRequest) throws IOException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse pitResponse = httpClient.execute(operationRequest);
        return pitResponse;
    }

}
