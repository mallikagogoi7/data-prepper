package org.opensearch.dataprepper.plugins.source.opensearch.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SortingConfiguration;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SearchPitIndexRequest implements Serializable {

    private Integer size;

    private PitInformation pit;

    private Map<String,String> query;

    private List<SortingConfiguration> sort;

    @JsonProperty("search_after")
    private List<Integer> searchAfter;

    public List<Integer> getSearchAfter() {
        return searchAfter;
    }

    public void setSearchAfter(List<Integer> searchAfter) {
        this.searchAfter = searchAfter;
    }

    public List<SortingConfiguration> getSort() {
        return sort;
    }

    public void setSort(List<SortingConfiguration> sort) {
        this.sort = sort;
    }

    public Map<String, String> getQuery() {
        return query;
    }

    public void setQuery(Map<String, String> query) {
        this.query = query;
    }


    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public static class PitInformation {
        private String id;

        @JsonProperty("keep_alive")
        private String keepAlive;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getKeepAlive() {
            return keepAlive;
        }

        public void setKeepAlive(String keepAlive) {
            this.keepAlive = keepAlive;
        }
    }

    public PitInformation getPit() {
        return pit;
    }

    public void setPit(PitInformation pit) {
        this.pit = pit;
    }


}
