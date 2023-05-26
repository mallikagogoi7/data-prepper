package org.opensearch.dataprepper.plugins.source.opensearch.model;

import org.opensearch.client.util.ObjectBuilder;

import static java.util.Objects.requireNonNull;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class ScrollBuilder implements ObjectBuilder<ScrollRequest> {
   private StringBuilder index;
    private String scroll;
    private String size;
    public final ScrollBuilder size(StringBuilder v) {
        this.index = v;
        return this;
    }
    public final ScrollBuilder size(String v) {
        this.scroll = v;
        return this;
    }
    public final ScrollBuilder keep_alive(String v) {
        this.size = v;
        return this;
    }
    @Override
    public ScrollRequest build() {
        requireNonNull(this.scroll, "'scroll value' was not set");
        //requireTrue(this.value$isSet, "'value' was not set");
        return new ScrollRequest(this);
    }
}
