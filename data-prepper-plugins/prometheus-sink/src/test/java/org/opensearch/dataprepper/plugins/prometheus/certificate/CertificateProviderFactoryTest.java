/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.prometheus.certificate;

import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.acm.ACMCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.file.FileCertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.s3.S3CertificateProvider;
import org.opensearch.dataprepper.plugins.sink.prometheus.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CertificateProviderFactoryTest {
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    private PrometheusSinkConfiguration prometheusSinkConfiguration;

    private AwsAuthenticationOptions awsAuthenticationOptions;
    private CertificateProviderFactory certificateProviderFactory;

    @BeforeEach
    void setUp() {
        prometheusSinkConfiguration = mock(PrometheusSinkConfiguration.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
    }

    @Test
    void getCertificateProviderFileCertificateProviderSuccess() {
        when(prometheusSinkConfiguration.isSsl()).thenReturn(true);
        when(prometheusSinkConfiguration.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(prometheusSinkConfiguration.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);

        certificateProviderFactory = new CertificateProviderFactory(prometheusSinkConfiguration);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(FileCertificateProvider.class));
    }

    @Test
    void getCertificateProviderS3ProviderSuccess() {
        when(prometheusSinkConfiguration.isSslCertAndKeyFileInS3()).thenReturn(true);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("us-east-1"));
        when(prometheusSinkConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(prometheusSinkConfiguration.getSslCertificateFile()).thenReturn("s3://data/certificate/test_cert.crt");
        when(prometheusSinkConfiguration.getSslKeyFile()).thenReturn("s3://data/certificate/test_decrypted_key.key");

        certificateProviderFactory = new CertificateProviderFactory(prometheusSinkConfiguration);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(S3CertificateProvider.class));
    }

    @Test
    void getCertificateProviderAcmProviderSuccess() {
        when(prometheusSinkConfiguration.useAcmCertForSSL()).thenReturn(true);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("us-east-1"));
        when(prometheusSinkConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(prometheusSinkConfiguration.getAcmCertificateArn()).thenReturn("arn:aws:acm:us-east-1:account:certificate/1234-567-856456");

        certificateProviderFactory = new CertificateProviderFactory(prometheusSinkConfiguration);
        final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();

        assertThat(certificateProvider, IsInstanceOf.instanceOf(ACMCertificateProvider.class));
    }
}