package org.opensearch.dataprepper.plugins.sink.handler;

import org.apache.hc.client5.http.config.TlsConfig;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Optional;

public class SecuredAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {

    private final HttpSinkConfiguration sinkConfiguration;

    public SecuredAuthHttpSinkHandler(final HttpSinkConfiguration sinkConfiguration){
        this.sinkConfiguration = sinkConfiguration;
    }

    private static final Logger LOG = LoggerFactory.getLogger(SecuredAuthHttpSinkHandler.class);

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions httpAuthOptions) {

        // logic here to read the certs from ACM/S3/local
        // SSL Sigv4 validation and verification and make connection

        final SSLContext sslContext = sinkConfiguration.getSslCertificateFile() != null ? getCAStrategy(Path.of(sinkConfiguration.getSslCertificateFile())) : getTrustAllStrategy();

        SSLConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactoryBuilder.create()
                .setSslContext(sslContext)
                .build();

        HttpClientConnectionManager clientConnectionManager =  PoolingHttpClientConnectionManagerBuilder.create()
                .setSSLSocketFactory(sslSocketFactory)
                .setDefaultTlsConfig(TlsConfig.custom()
                        .setHandshakeTimeout(Timeout.ofSeconds(30))
                        .setSupportedProtocols(TLS.V_1_3)
                        .build())
                .build();

        httpAuthOptions.setHttpClientConnectionManager(clientConnectionManager);
        return httpAuthOptions;
    }

    private SSLContext getCAStrategy(Path certPath) {
        LOG.info("Using the cert provided in the config.");
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate trustedCa;
            try (InputStream is = Files.newInputStream(certPath)) {
                trustedCa = factory.generateCertificate(is);
            }
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null);
            return sslContextBuilder.build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private SSLContext getTrustAllStrategy() {
        LOG.info("Using the trust all strategy");
        final TrustStrategy trustStrategy = new TrustAllStrategy();
        try {
            return SSLContexts.custom().loadTrustMaterial(null, trustStrategy).build();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
