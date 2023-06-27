/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertFalse;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.util.List;

public class HttpSinkConfiguration {

    private static final int DEFAULT_UPLOAD_RETRIES = 5;

    private static final String DEFAULT_HTTP_METHOD = "POST";

    private static final int DEFAULT_WORKERS = 1;

    private static final boolean DEFAULT_INSECURE = false;

    static final boolean DEFAULT_SSL = true;

    private static final String S3_PREFIX = "s3://";

    static final String SSL_KEY_CERT_FILE = "sslKeyCertChainFile";
    static final String SSL_KEY_FILE = "sslKeyFile";
    static final String ACM_CERT_ARN = "acmCertificateArn";
    static final String ACM_PRIVATE_KEY_PASSWORD = "acmPrivateKeyPassword";
    static final String ACM_CERT_ISSUE_TIME_OUT_MILLIS = "acmCertIssueTimeOutMillis";
    static final String PATH = "path";
    static final String SSL = "ssl";
    static final String USE_ACM_CERT_FOR_SSL = "useAcmCertForSSL";

    static final String AWS_REGION = "awsRegion";

    static final boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;

    static final int DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS = 120000;

    @NotNull
    @JsonProperty("urls")
    private List<UrlConfigurationOption> urlConfigurationOptions;

    @JsonProperty("workers")
    private Integer workers = DEFAULT_WORKERS;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("http_method")
    private String httpMethod = DEFAULT_HTTP_METHOD;

    @JsonProperty("proxy")
    private String proxy;

    @JsonProperty("auth_type")
    private String authType;

    private PluginModel authentication;

    @JsonProperty("insecure")
    private boolean insecure = DEFAULT_INSECURE;

    @JsonProperty("ssl_certificate_file")
    private String sslCertificateFile;

    @JsonProperty("ssl_key_file")
    private String sslKeyFile;

    @JsonProperty("aws_sigv4")
    private boolean awsSigv4;

    @JsonProperty("buffer_type")
    //private BufferTypeOptions bufferType = BufferTypeOptions.INMEMORY;
    private String  bufferType = "in_memory";  //TODO: change to BufferTypeOptions

    @JsonProperty("threshold")
    private ThresholdOptions thresholdOptions;

    @JsonProperty("max_retries")
    private int maxUploadRetries = DEFAULT_UPLOAD_RETRIES;

    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("custom_header")
    private CustomHeaderOptions customHeaderOptions;

    @JsonProperty("dlq_file")
    private String dlqFile;

    private PluginModel dlq;

    @JsonProperty(USE_ACM_CERT_FOR_SSL)
    private boolean useAcmCertForSSL = DEFAULT_USE_ACM_CERT_FOR_SSL;

    @JsonProperty(ACM_PRIVATE_KEY_PASSWORD)
    private String acmPrivateKeyPassword;

    @JsonProperty(ACM_CERT_ARN)
    private String acmCertificateArn;

    @JsonProperty(ACM_CERT_ISSUE_TIME_OUT_MILLIS)
    private long acmCertIssueTimeOutMillis = DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS;

    @JsonProperty(SSL)
    private boolean ssl = DEFAULT_SSL;

    private boolean sslCertAndKeyFileInS3;

    public boolean isSsl() {
        return ssl;
    }

    public String getAcmPrivateKeyPassword() {
        return acmPrivateKeyPassword;
    }

    public boolean isSslCertAndKeyFileInS3() {
        return sslCertAndKeyFileInS3;
    }

    public long getAcmCertIssueTimeOutMillis() {
        return acmCertIssueTimeOutMillis;
    }

    public boolean useAcmCertForSSL() {
        return useAcmCertForSSL;
    }

    public void validateAndInitializeCertAndKeyFileInS3() {
        boolean certAndKeyFileInS3 = false;
        if (useAcmCertForSSL) {
            validateSSLArgument(String.format("%s is enabled", USE_ACM_CERT_FOR_SSL), acmCertificateArn, ACM_CERT_ARN);
            validateSSLArgument(String.format("%s is enabled", USE_ACM_CERT_FOR_SSL), awsAuthenticationOptions.getAwsRegion().toString(), AWS_REGION);
        } else if(ssl) {
            validateSSLCertificateFiles();
            certAndKeyFileInS3 = isSSLCertificateLocatedInS3();
            if (certAndKeyFileInS3) {
                validateSSLArgument("The certificate and key files are located in S3", awsAuthenticationOptions.getAwsRegion().toString(), AWS_REGION);
            }
        }
        sslCertAndKeyFileInS3 = certAndKeyFileInS3;
    }
    private void validateSSLArgument(final String sslTypeMessage, final String argument, final String argumentName) {
        if (StringUtils.isEmpty(argument)) {
            throw new IllegalArgumentException(String.format("%s, %s can not be empty or null", sslTypeMessage, argumentName));
        }
    }

    private void validateSSLCertificateFiles() {
        validateSSLArgument(String.format("%s is enabled", SSL), sslCertificateFile, SSL_KEY_CERT_FILE);
        validateSSLArgument(String.format("%s is enabled", SSL), sslKeyFile, SSL_KEY_FILE);
    }

    private boolean isSSLCertificateLocatedInS3() {
        return sslCertificateFile.toLowerCase().startsWith(S3_PREFIX) &&
                sslKeyFile.toLowerCase().startsWith(S3_PREFIX);
    }

    public String getAcmCertificateArn() {
        return acmCertificateArn;
    }

    public List<UrlConfigurationOption> getUrlConfigurationOptions() {
        return urlConfigurationOptions;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public String getProxy() {
        return proxy;
    }

    public String getAuthType() {
        return authType;
    }

    public PluginModel getAuthentication() {
        return authentication;
    }

    public boolean isInsecure() {
        return insecure;
    }

    public String getSslCertificateFile() {
        return sslCertificateFile;
    }

    public String getSslKeyFile() {
        return sslKeyFile;
    }

    public boolean isAwsSigv4() {
        return awsSigv4;
    }

    public String getBufferType() {
        return bufferType;
    }

    public ThresholdOptions getThresholdOptions() {
        return thresholdOptions;
    }

    public int getMaxUploadRetries() {
        return maxUploadRetries;
    }

    public CustomHeaderOptions getCustomHeaderOptions() {
        return customHeaderOptions;
    }

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public Integer getWorkers() {
        return workers;
    }

    public String getDlqFile() {
        return dlqFile;
    }

    public PluginModel getDlq() {
        return dlq;
    }
}
