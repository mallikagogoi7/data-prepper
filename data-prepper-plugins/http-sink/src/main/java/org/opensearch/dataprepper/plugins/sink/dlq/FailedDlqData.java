package org.opensearch.dataprepper.plugins.sink.dlq;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.util.Objects;

public class FailedDlqData {

    private final int status;

    private final String message;

    @JsonIgnore
    private final EventHandle eventHandle;

    public FailedDlqData(final int status,
                          final String message,
                          final EventHandle eventHandle) {
        this.status = status;
        Objects.requireNonNull(message);
        this.message = message;
        this.eventHandle = eventHandle;
    }

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }
    public EventHandle getEventHandle() {
        return eventHandle;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FailedDlqData that = (FailedDlqData) o;
        return Objects.equals(status, that.status) &&
            Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, message);
    }

    @Override
    public String toString() {
        return "FailedDlqData{" +
            ", status='" + status + '\'' +
            ", message='" + message + '\'' +
            '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private EventHandle eventHandle;

        private int status = 0;

        private String message;

        public Builder withStatus(final int status) {
            this.status = status;
            return this;
        }

        public Builder withMessage(final String message) {
            this.message = message;
            return this;
        }

        public Builder withEventHandle(final EventHandle eventHandle) {
            this.eventHandle = eventHandle;
            return this;
        }

        public FailedDlqData build() {
            return new FailedDlqData(status, message, eventHandle);
        }
    }
}
