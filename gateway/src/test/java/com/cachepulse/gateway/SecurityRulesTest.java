package com.cachepulse.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.web.server.WebFilterChainProxy;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

/** 鉴权规则（设计文档第 9.2 节）：/api/** 的 GET 公开，写操作要令牌，/internal/** 一律拒绝。 */
class SecurityRulesTest {

    private final WebTestClient client = client();

    @Test
    void readOnlyAdminApisAreOpen() {
        for (String path : new String[] {"/api/sync/status", "/api/platform/events", "/api/platform/subscriptions",
                "/api/cache/groups/users/entries", "/api/users/1", "/platform-dashboard.html"}) {
            client.get().uri(path).exchange().expectStatus().isOk();
        }
    }

    @Test
    void writesStillNeedAToken() {
        client.post().uri("/api/platform/subscriptions").exchange().expectStatus().isUnauthorized();
        client.put().uri("/api/users/1").exchange().expectStatus().isUnauthorized();
        client.delete().uri("/api/cache/groups/users/entries").exchange().expectStatus().isUnauthorized();
        client.post().uri("/api/platform/replay-jobs").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void writesWithABadTokenAreRejected() {
        client.post().uri("/api/platform/subscriptions").header(SecurityConfiguration.TOKEN_HEADER, "forged")
                .exchange().expectStatus().isUnauthorized();
    }

    @Test
    void publicBatchQueriesAndLoginStayOpen() {
        client.post().uri("/api/users/batch").exchange().expectStatus().isOk();
        client.post().uri("/auth/token").exchange().expectStatus().isOk();
    }

    @Test
    void internalApisStayBlockedEvenForReads() {
        // denyAll：没登录返回 401，登录了也是 403；GET /api/** 公开不会放开它
        client.get().uri("/internal/events/abc").exchange().expectStatus().isUnauthorized();
        client.get().uri("/api/internal/events/abc").exchange().expectStatus().isUnauthorized();
    }

    private static WebTestClient client() {
        ReactiveJwtDecoder rejectAll = token -> Mono.error(new BadJwtException("test decoder rejects every token"));
        var chain = new SecurityConfiguration().securityWebFilterChain(ServerHttpSecurity.http(), rejectAll);
        return WebTestClient.bindToWebHandler(exchange -> {
                    exchange.getResponse().setStatusCode(HttpStatus.OK);
                    return exchange.getResponse().setComplete();
                })
                .webFilter(new WebFilterChainProxy(chain))
                .build();
    }
}
