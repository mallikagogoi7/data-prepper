/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.model;

public class SourceInfo {

    private String osVersion;

    private String dataSource;

    private Boolean healthStatus = true;

    public SourceInfo(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public Boolean getHealthStatus() {
        return healthStatus;
    }

    public void setHealthStatus(Boolean healthStatus) {
        this.healthStatus = healthStatus;
    }
}
