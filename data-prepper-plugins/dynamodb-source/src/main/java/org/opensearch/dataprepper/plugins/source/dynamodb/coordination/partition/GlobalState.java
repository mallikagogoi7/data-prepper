/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.SourcePartition;

import java.util.Map;
import java.util.Optional;

/**
 * Global State is a special type of partition. The partition type is null.
 * You can't acquire (own) a Global State.
 * However, you can read and update Global State whenever required.
 * The progress state is a Map object.
 */
public class GlobalState extends SourcePartition<Map<String, Object>> {

    private final String stateName;

    private Map<String, Object> state;

    public GlobalState(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.stateName = sourcePartitionStoreItem.getSourcePartitionKey();
        this.state = convertStringToPartitionProgressState(null, sourcePartitionStoreItem.getPartitionProgressState());

    }

    public GlobalState(String stateName, Optional<Map<String, Object>> state) {
        this.stateName = stateName;
        this.state = state.orElse(null);

    }

    @Override
    public String getPartitionType() {
        return null;
    }

    @Override
    public String getPartitionKey() {
        return stateName;
    }

    @Override
    public Optional<Map<String, Object>> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

    public void setProgressState(Map<String, Object> state) {
        this.state = state;
    }


}
