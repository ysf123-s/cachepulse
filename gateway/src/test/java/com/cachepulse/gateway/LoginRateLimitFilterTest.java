package com.cachepulse.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.filter.ratelimit.RateLimiter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/** 登录接口不经过路由，路由上的限流管不到它；这里单独按客户端 IP 限次数（设计文档 14.1 第 18 条）。 */
class LoginRateLimitFilterTest {
    private static final String CLIENT_IP_HEADER = "X-CachePulse-Client-IP";

    @SuppressWarnings("unchecked")
    private final RateLimiter<Object> limiter = mock(RateLimiter.class);
    private final KeyResolver clientIp = new RateLimitConfiguration().ipKeyResolver(
            new GatewayProperties("admin", null, null, null, 0, CLIENT_IP_HEADER));
    @SuppressWarnings("unchecked")
    private final LoginRateLimitFilter filter = new LoginRateLimitFilter(limiter, clientIp,
            new TraceIdHeaderFilter(mock(ObjectProvider.class)));
    private final AtomicInteger reachedLogin = new AtomicInteger();
    private final WebTestClient client = WebTestClient.bindToWebHandler(exchange -> {
                reachedLogin.incrementAndGet();
                exchange.getResponse().setStatusCode(HttpStatus.OK);
                return exchange.getResponse().setComplete();
            })
            .webFilter(filter)
            .build();

    @Test
    void limitAveragesFiveLoginsAMinuteWithBurstsOfTen() {
        var limit = LoginRateLimitFilter.LIMIT;
        assertThat(60 * limit.getReplenishRate() / limit.getRequestedTokens()).isEqualTo(5);
        assertThat(limit.getBurstCapacity() / limit.getRequestedTokens()).isEqualTo(10);
        assertThat(LoginRateLimitFilter.RETRY_AFTER_SECONDS).isEqualTo(12);
    }

    @Test
    void loginWithinTheLimitGoesThroughAndIsCountedPerClientIp() {
        allow(true);

        client.post().uri("/auth/token").header(CLIENT_IP_HEADER, "203.0.113.7").exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-RateLimit-Remaining", "108");

        verify(limiter).isAllowed(LoginRateLimitFilter.LIMIT_ID, "203.0.113.7");
        assertThat(reachedLogin).hasValue(1);
    }

    @Test
    void loginOverTheLimitGets429WithoutCheckingThePassword() {
        allow(false);

        client.post().uri("/auth/token").header(CLIENT_IP_HEADER, "203.0.113.7").exchange()
                .expectStatus().isEqualTo(HttpStatus.TOO_MANY_REQUESTS)
                .expectHeader().valueEquals(HttpHeaders.RETRY_AFTER, "12")
                .expectBody()
                .jsonPath("$.status").isEqualTo(429)
                .jsonPath("$.code").isEqualTo("LOGIN_RATE_LIMITED");

        assertThat(reachedLogin).as("the password check never runs").hasValue(0);
    }

    @Test
    void encodedLoginPathIsLimitedToo() {
        allow(false);
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.POST, URI.create("/auth/%74oken")));
        WebFilterChain chain = ex -> {
            reachedLogin.incrementAndGet();
            return Mono.empty();
        };

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(reachedLogin).hasValue(0);
    }

    @Test
    void otherRequestsAreNotCounted() {
        client.get().uri("/auth/token").exchange().expectStatus().isOk();
        client.post().uri("/api/users/batch").exchange().expectStatus().isOk();
        client.get().uri("/api/users/1").exchange().expectStatus().isOk();

        verify(limiter, never()).isAllowed(anyString(), anyString());
        assertThat(reachedLogin).hasValue(3);
    }

    private void allow(boolean allowed) {
        when(limiter.isAllowed(eq(LoginRateLimitFilter.LIMIT_ID), anyString())).thenReturn(Mono.just(
                new RateLimiter.Response(allowed, Map.of("X-RateLimit-Remaining", allowed ? "108" : "0"))));
    }
}
