/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.when;

public class AwsRequestSigningApacheInterceptorTest {

    AwsRequestSigningApacheInterceptor awsInterceptor;



    @Mock
    private HttpRequest httpRequest;

    @Mock
    private EntityDetails entityDetails;

    @Mock
    private HttpContext httpContext;
    public void process_test(){
    }
}
