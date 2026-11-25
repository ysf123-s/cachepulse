package com.cachepulse.gateway;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Optional;

/**
 * 身份透传：先删掉客户端自带的 X-User-Id（防伪造）和内部令牌，再按校验过的 JWT 写入用户名。
 * 下游服务只认网关写入的身份头；JWT 本身不再往下传，避免出现在下游日志里。
 */
@Component
public class IdentityRelayFilter implements GlobalFilter, Ordered {
    public static final String USER_HEADER = "X-User-Id";
    public static final String INTERNAL_TOKEN_HEADER = "X-Internal-Token";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        return exchange.getPrincipal()
                .filter(JwtAuthenticationToken.class::isInstance)
                .map(principal -> Optional.of(((JwtAuthenticationToken) principal).getToken().getSubject()))
                .defaultIfEmpty(Optional.empty())
                .flatMap(user -> {
                    ServerHttpRequest request = exchange.getRequest().mutate().headers(headers -> {
                        headers.remove(USER_HEADER);
                        headers.remove(INTERNAL_TOKEN_HEADER);
                        headers.remove(HttpHeaders.AUTHORIZATION);
                        headers.remove(SecurityConfiguration.TOKEN_HEADER);
                        user.ifPresent(name -> headers.set(USER_HEADER, name));
                    }).build();
                    return chain.filter(exchange.mutate().request(request).build());
                });
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
