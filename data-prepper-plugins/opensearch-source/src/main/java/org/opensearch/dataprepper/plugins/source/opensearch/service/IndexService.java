/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.service;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.OpenSearchSourceConfiguration;

import java.io.IOException;

public interface IndexService {

    void generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer) throws IOException;

    void searchPitIndexes(final String pitID, final OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer);


}