package com.cachepulse.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.security.Principal;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class IdentityRelayFilterTest {
    private final IdentityRelayFilter filter = new IdentityRelayFilter();

    @Test
    void replacesForgedIdentityWithTheJwtSubjectAndDropsCredentials() {
        Jwt jwt = Jwt.withTokenValue("signed-token").header("alg", "HS256").subject("admin").build();

        HttpHeaders forwarded = relay(new JwtAuthenticationToken(jwt));

        assertThat(forwarded.get(IdentityRelayFilter.USER_HEADER)).containsExactly("admin");
        assertCredentialsRemoved(forwarded);
        assertThat(forwarded.getFirst("X-Other")).isEqualTo("kept");
    }

    @Test
    void anonymousRequestsForwardNoIdentity() {
        HttpHeaders forwarded = relay(null);

        assertThat(forwarded.containsKey(IdentityRelayFilter.USER_HEADER)).isFalse();
        assertCredentialsRemoved(forwarded);
    }

    @Test
    void nonJwtPrincipalForwardsNoIdentity() {
        HttpHeaders forwarded = relay(new TestingAuthenticationToken("someone", "secret"));

        assertThat(forwarded.containsKey(IdentityRelayFilter.USER_HEADER)).isFalse();
    }

    private HttpHeaders relay(Principal principal) {
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/api/platform/events")
                .header(IdentityRelayFilter.USER_HEADER, "hacker")
                .header(IdentityRelayFilter.INTERNAL_TOKEN_HEADER, "guessed")
                .header(HttpHeaders.AUTHORIZATION, "Bearer signed-token")
                .header(SecurityConfiguration.TOKEN_HEADER, "signed-token")
                .header("X-Other", "kept"));
        if (principal != null) {
            exchange = exchange.mutate().principal(Mono.just(principal)).build();
        }
        AtomicReference<ServerWebExchange> forwarded = new AtomicReference<>();
        GatewayFilterChain chain = next -> {
            forwarded.set(next);
            return Mono.empty();
        };

        filter.filter(exchange, chain).block();

        assertThat(forwarded.get()).as("chain called").isNotNull();
        return forwarded.get().getRequest().getHeaders();
    }

    private static void assertCredentialsRemoved(HttpHeaders headers) {
        assertThat(headers.containsKey(IdentityRelayFilter.INTERNAL_TOKEN_HEADER)).isFalse();
        assertThat(headers.containsKey(HttpHeaders.AUTHORIZATION)).isFalse();
        assertThat(headers.containsKey(SecurityConfiguration.TOKEN_HEADER)).isFalse();
        assertThat(headers.getOrEmpty(IdentityRelayFilter.USER_HEADER)).doesNotContain("hacker");
    }
}
