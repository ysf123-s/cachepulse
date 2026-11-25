package com.cachepulse.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

/** 公网请求都经同一个代理地址进来，限流要按代理写入的客户端 IP 计数，否则所有人共用一个额度。 */
class RateLimitConfigurationTest {
    private static final String HEADER = "X-CachePulse-Client-IP";

    @Test
    void countsByTheHeaderTheFrontProxyWrites() {
        KeyResolver resolver = resolver(HEADER, 0);

        assertThat(key(resolver, "203.0.113.7")).isEqualTo("203.0.113.7");
        assertThat(key(resolver, "2001:db8::1")).isEqualTo("2001:db8::1");
        assertThat(key(resolver, "203.0.113.7, 10.0.0.1")).as("first entry of a list").isEqualTo("203.0.113.7");
        assertThat(key(resolver, "198.51.100.2")).isNotEqualTo(key(resolver, "203.0.113.7"));
    }

    @Test
    void fallsBackToTheConnectionAddressWhenTheHeaderIsMissingOrNotAnIp() {
        KeyResolver resolver = resolver(HEADER, 0);

        assertThat(key(resolver, null)).isEqualTo("172.19.0.1");
        assertThat(key(resolver, "not an ip")).isEqualTo("172.19.0.1");
        assertThat(key(resolver, "203.0.113.7}{")).isEqualTo("172.19.0.1");
    }

    @Test
    void ignoresTheHeaderUnlessConfigured() {
        assertThat(key(resolver(null, 0), "203.0.113.7")).isEqualTo("172.19.0.1");
        assertThat(key(resolver("", 0), "203.0.113.7")).isEqualTo("172.19.0.1");
    }

    private static KeyResolver resolver(String header, int trustedProxies) {
        return new RateLimitConfiguration().ipKeyResolver(
                new GatewayProperties("admin", null, null, null, trustedProxies, header));
    }

    private static String key(KeyResolver resolver, String headerValue) {
        MockServerHttpRequest.BaseBuilder<?> request = MockServerHttpRequest.get("/api/users/1")
                .remoteAddress(new InetSocketAddress("172.19.0.1", 40000));
        if (headerValue != null) {
            request.header(HEADER, headerValue);
        }
        return resolver.resolve(MockServerWebExchange.from(request)).block();
    }
}
