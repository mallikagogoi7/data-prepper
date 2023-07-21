package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class BearerTokenOptions {

    @JsonProperty("client_id")
    @NotNull(message = "client id type is mandatory for refresh token")
    private String clientId;

    @JsonProperty("client_secret")
    @NotNull(message = "client secret type is mandatory for refresh token")
    private String clientSecret;

    @JsonProperty("token_url")
    @NotNull(message = "token url type is mandatory for refresh token")
    private String tokenURL;

    @JsonProperty("grant_type")
    @NotNull(message = "grant type is mandatory for refresh token")
    private String grantType;

    @JsonProperty("refresh_token")
    private String refreshToken;

    @JsonProperty("scope")
    @NotNull(message = "scope is mandatory for refresh token")
    private String scope;

    public String getScope() {
        return scope;
    }

    public String getGrantType() {
        return grantType;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getTokenURL() {
        return tokenURL;
    }

}
