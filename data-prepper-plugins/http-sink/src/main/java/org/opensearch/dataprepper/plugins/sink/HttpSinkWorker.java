package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.plugins.sink.handler.HttpAuthOptions;

public class HttpSinkWorker implements Runnable {

    private final HttpAuthOptions httpEndPointDetails;

    public HttpSinkWorker(final HttpAuthOptions httpEndPointDetails){
        this.httpEndPointDetails = httpEndPointDetails;
    }

    @Override
    public void run() {
        try{
            // push to http end point based on workers - Callable<>
        }catch(Exception e){
            // In case of any exception, need to write the exception in dlq  - logFailureForDlqObjects();
            // In case of any exception, need to push the web hook url- logFailureForWebHook();
        }
    }
}
