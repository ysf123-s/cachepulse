package com.cachepulse.gateway;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.support.ipresolver.RemoteAddressResolver;
import org.springframework.cloud.gateway.support.ipresolver.XForwardedRemoteAddressResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

/**
 * 公开查询接口按客户端 IP 限流（RedisRateLimiter，计数在 Redis DB 1，多个网关实例共用）。
 * 公网入口前面有一层反向代理，客户端 IP 取 X-Forwarded-For 里代理追加的那一项；直接对外暴露时把信任层数设为 0。
 */
@Configuration(proxyBeanMethods = false)
public class RateLimitConfiguration {

    @Bean
    public KeyResolver ipKeyResolver(GatewayProperties properties) {
        RemoteAddressResolver resolver = properties.trustedProxies() > 0
                ? XForwardedRemoteAddressResolver.maxTrustedIndex(properties.trustedProxies())
                : new RemoteAddressResolver() { };
        return exchange -> {
            InetSocketAddress address = resolver.resolve(exchange);
            String key = address == null || address.getAddress() == null ? "unknown" : address.getAddress().getHostAddress();
            return Mono.just(key);
        };
    }
}
