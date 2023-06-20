/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;

public final class ClientFactory {
    private ClientFactory() { }

    static S3Client createS3Client(final Region region, final String roleArn, final Map<String, String> stsHeader, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(region, roleArn, stsHeader);
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

        return S3Client.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(builder -> builder.numRetries(5).build())
                        .build())
                .build();
    }

    private static AwsCredentialsOptions convertToCredentialsOptions(final Region region, final String roleArn, final Map<String, String> stsHeader) {
        return AwsCredentialsOptions.builder()
                .withRegion(region)
                .withStsRoleArn(roleArn)
                .withStsHeaderOverrides(stsHeader)
                .build();
    }
}
