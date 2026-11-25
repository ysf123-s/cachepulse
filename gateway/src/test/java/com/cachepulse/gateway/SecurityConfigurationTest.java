package com.cachepulse.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;

import static org.assertj.core.api.Assertions.assertThat;

/** 令牌请求头（设计文档第 14.1 节第 9 条）：优先 X-CachePulse-Token，其次 Authorization: Bearer。 */
class SecurityConfigurationTest {

    @Test
    void readsTheCachePulseTokenHeader() {
        assertThat(token(MockServerHttpRequest.get("/api/sync/status")
                .header(SecurityConfiguration.TOKEN_HEADER, " jwt-from-page "))).isEqualTo("jwt-from-page");
    }

    @Test
    void cachePulseTokenWinsOverAuthorization() {
        assertThat(token(MockServerHttpRequest.get("/api/sync/status")
                .header(SecurityConfiguration.TOKEN_HEADER, "jwt-from-page")
                .header(HttpHeaders.AUTHORIZATION, "Basic aW5zdGFib3g6cGFzcw=="))).isEqualTo("jwt-from-page");
    }

    @Test
    void fallsBackToAuthorizationBearer() {
        assertThat(token(MockServerHttpRequest.get("/api/sync/status")
                .header(HttpHeaders.AUTHORIZATION, "Bearer jwt-from-script"))).isEqualTo("jwt-from-script");
    }

    @Test
    void blankCachePulseTokenFallsBackToAuthorizationBearer() {
        assertThat(token(MockServerHttpRequest.get("/api/sync/status")
                .header(SecurityConfiguration.TOKEN_HEADER, "  ")
                .header(HttpHeaders.AUTHORIZATION, "Bearer jwt-from-script"))).isEqualTo("jwt-from-script");
    }

    @Test
    void ignoresAuthorizationBasicAndMissingHeaders() {
        // 公网入口的 Nginx 会把 Authorization 换成访问 Instabox 端口的 Basic 凭据。
        assertThat(token(MockServerHttpRequest.get("/api/sync/status")
                .header(HttpHeaders.AUTHORIZATION, "Basic aW5zdGFib3g6cGFzcw=="))).isNull();
        assertThat(token(MockServerHttpRequest.get("/api/sync/status"))).isNull();
    }

    private static String token(MockServerHttpRequest.BaseBuilder<?> request) {
        Authentication authentication = SecurityConfiguration.tokenConverter()
                .convert(MockServerWebExchange.from(request)).block();
        return authentication == null ? null : ((BearerTokenAuthenticationToken) authentication).getToken();
    }
}
