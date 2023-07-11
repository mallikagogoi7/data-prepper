# OpenSearch Sink

This is the Data Prepper Http sink plugin that sends records to http/https endpoints. You can use the sink to send data to arbitrary HTTP Endpoints.


## Usages

The Http sink should be configured as part of Data Prepper pipeline yaml file.

### Response status

* `200`: the request data has been successfully pushed to http endpoint.
* `500`: internal server error while process the request data.
* `400`: bad request error
* `404`: the http endpoint is not reachable
* `501`: the server does not recognize the request method and is incapable of supporting it for any resource

### HTTP Basic authentication
```
pipeline:
  ...
  sink:
  - http:
      authentication:
        http_basic:
          username: my-user
          password: my_s3cr3t
```

### HTTP Bearer token authentication
```
pipeline:
  ...
  sink:
  - http:
      authentication:
         bearer_token:
          token: my-token
```

## Configuration

- `urls` (Optional) : URL configurations. See [URL Configuration](#url_configuration) for details.

- `ssl_certificate_file`(optional): CA certificate that is pem encoded. Accepts both .pem or .crt. This enables the client to trust the CA that has signed the certificate that the Http Endpoint is using.
  Default is null.

- `aws_sigv4`: A boolean flag to sign the HTTP request with AWS credentials. Only applies to Amazon OpenSearch Service. See [security](security.md) for details. Default to `false`.

- `aws_region`: A String represents the region of Amazon OpenSearch Service domain, e.g. us-west-2. Only applies to Amazon OpenSearch Service. Defaults to `us-east-1`.

- `aws_sts_role_arn`: A IAM role arn which the sink plugin will assume to sign request to Amazon OpenSearch Service. If not provided the plugin will use the default credentials.

- `aws_sts_external_id`: An optional external ID to use when assuming an IAM role.

- `aws_sts_header_overrides`: An optional map of header overrides to make when assuming the IAM role for the sink plugin.

- `insecure`: A boolean flag to turn off SSL certificate verification. If set to true, CA certificate verification will be turned off and insecure HTTP requests will be sent. Default to `false`.

- `aws` (Optional) : AWS configurations. See [AWS Configuration](#aws_configuration) for details. SigV4 is enabled by default when this option is used. If this option is present, `aws_` options are not expected to be present. If any of `aws_` options are present along with this, error is thrown.

- `socket_timeout`(optional): An integer value indicates the timeout in milliseconds for waiting for data (or, put differently, a maximum period inactivity between two consecutive data packets). A timeout value of zero is interpreted as an infinite timeout. If this timeout value is either negative or not set, the underlying Apache HttpClient would rely on operating system settings for managing socket timeouts.

- `connect_timeout`(optional): An integer value indicates the timeout in milliseconds used when requesting a connection from the connection manager. A timeout value of zero is interpreted as an infinite timeout. If this timeout value is either negative or not set, the underlying Apache HttpClient would rely on operating system settings for managing connection timeouts.

- `username`(optional): A String of username used in the [internal users](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles/) of OpenSearch cluster. Default is null.

- `password`(optional): A String of password used in the [internal users](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles/) of OpenSearch cluster. Default is null.

- `proxy`(optional): A String of the address of a forward HTTP proxy. The format is like "<host-name-or-ip>:\<port\>". Examples: "example.com:8100", "http://example.com:8100", "112.112.112.112:8100". Note: port number cannot be omitted.

- `dlq_file`(optional): A String of absolute file path for DLQ failed output records. Defaults to null.
  If not provided, failed records will be written into the default data-prepper log file (`logs/Data-Prepper.log`). If the `dlq` option is present along with this, an error is thrown.

- `dlq` (optional): DLQ configurations. See [DLQ](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/failures-common/src/main/java/org/opensearch/dataprepper/plugins/dlq/README.md) for details. If the `dlq_file` option is present along with this, an error is thrown.

- `max_retries`(optional): A number indicating the maximum number of times OpenSearch Sink should try to push the data to the OpenSearch server before considering it as failure. Defaults to `Integer.MAX_VALUE`.
  If not provided, the sink will try to push the data to OpenSearch server indefinitely because default value is very high and exponential backoff would increase the waiting time before retry.

- `bulk_size` (optional): A long of bulk size in bulk requests in MB. Default to 5 MB. If set to be less than 0,
  all the records received from the upstream prepper at a time will be sent as a single bulk request.
  If a single record turns out to be larger than the set bulk size, it will be sent as a bulk request of a single document.

- `flush_timeout` (optional): A long of the millisecond duration to try packing a bulk request up to the bulk_size before flushing.
  If this timeout expires before a bulk request has reached the bulk_size, the request will be flushed as-is. Set to -1 to disable
  the flush timeout and instead flush whatever is present at the end of each batch. Default is 60,000, or one minute.

- `document_id_field` (optional): A string of document identifier which is used as `id` for the document when it is stored in the OpenSearch. Each incoming record is searched for this field and if it is present, it is used as the id for the document, if it is not present, a unique id is generated by the OpenSearch when storing the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the document id

- `routing_field` (optional): A string of routing field which is used as hash for generating sharding id for the document when it is stored in the OpenSearch. Each incoming record is searched for this field and if it is present, it is used as the routing field for the document, if it is not present, default routing mechanism used by the OpenSearch when storing the document. Standard Data Prepper Json pointer syntax is used for retrieving the value. If the field has "/" in it then the incoming record is searched in the json sub-objects instead of just in the root of the json object. For example, if the field is specified as `info/id`, then the root of the event is searched for `info` and if it is found, then `id` is searched inside it. The value specified for `id` is used as the routing id

- `ism_policy_file` (optional): A String of absolute file path or AWS S3 URI for an ISM (Index State Management) policy JSON file. This policy file is effective only when there is no built-in policy file for the index type. For example, `custom` index type is currently the only one without a built-in policy file, thus it would use the policy file here if it's provided through this parameter. OpenSearch documentation has more about [ISM policies.](https://opensearch.org/docs/latest/im-plugin/ism/policies/)

- `s3_aws_region` (optional): A String represents the region of S3 bucket to read `template_file` or `ism_policy_file`, e.g. us-west-2. Only applies to Amazon OpenSearch Service. Defaults to `us-east-1`.

- `s3_aws_sts_role_arn` (optional): An IAM role arn which the sink plugin will assume to read `template_file` or `ism_policy_file` from S3. If not provided the plugin will use the default credentials.

- `s3_aws_sts_external_id` (optional): An external ID that be attached to Assume Role requests.

- `trace_analytics_raw`: No longer supported starting Data Prepper 2.0. Use `index_type` instead.

- `trace_analytics_service_map`: No longer supported starting Data Prepper 2.0. Use `index_type` instead.

- `document_root_key`: The key in the event that will be used as the root in the document. The default is the root of the event. If the key does not exist the entire event is written as the document. If the value at the `document_root_key` is a basic type (ie String, int, etc), the document will have a structure of `{"data": <value of the document_root_key>}`. For example, If we have the following sample event:

```
{
    status: 200,
    message: null,
    metadata: {
        sourceIp: "123.212.49.58",
        destinationIp: "79.54.67.231",
        bytes: 3545,
        duration: "15 ms"
    }
}
```
With the `document_root_key` set to `status`. The document structure would be `{"data": 200}`. Alternatively if, the `document_root_key` was provided as `metadata`. The document written to OpenSearch would be:

```
{
    sourceIp: "123.212.49.58"
    destinationIp: "79.54.67.231"
    bytes: 3545,
    duration: "15 ms"
}
```

### <a name="url_configuration">URL Configuration</a>

* `url` : The http/https endpoint url.
* `http_method` (Optional) : HttpMethod to be used. Default is POST.
* `auth_type` (Optional): Authentication type configuration. By default, this runs an unauthenticated server.
* `proxy` (Optional): A String of the address of a forward HTTP proxy. The format is like "<host-name-or-ip>:\<port\>". Examples: "example.com:8100", "http://example.com:8100", "112.112.112.112:8100". Note: port number cannot be omitted.

### <a name="aws_configuration">AWS Configuration</a>

* `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).
* `sts_role_arn` (Optional) : The STS role to assume for requests to AWS. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).
* `sts_header_overrides` (Optional): A map of header overrides to make when assuming the IAM role for the sink plugin.
* `serverless` (Optional): A boolean flag to indicate the OpenSearch backend is Amazon OpenSearch Serverless. Default to `false`.

## Metrics

### Counter

- `httpSinkRecordsSuccessCounter`: measures total number of records successfully pushed to http end points (200 response status code) by HTTP sink plugin.
- `httpSinkRecordsFailedCounter`: measures total number of records failed to pushed to http end points (500/400/404/501 response status code) by HTTP sink plugin.

### End-to-End acknowledgements

If the events received by the OpenSearch Sink have end-to-end acknowledgements enabled (which is tracked using the presence of EventHandle in the event received for processing), then upon successful posting to OpenSearch or upon successful write to DLQ, a positive acknowledgement is sent to the acknowledgementSetManager, otherwise a negative acknowledgement is sent.

## Developer Guide

This plugin is compatible with Java 8. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
