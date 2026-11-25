package com.cachepulse.gateway;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.support.ipresolver.RemoteAddressResolver;
import org.springframework.cloud.gateway.support.ipresolver.XForwardedRemoteAddressResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.regex.Pattern;

/**
 * 公开查询接口按客户端 IP 限流（RedisRateLimiter，计数在 Redis DB 1，多个网关实例共用）。
 *
 * <p>客户端 IP 的来源按顺序：</p>
 * <ol>
 *   <li>配置了 {@code client-ip-header} 时，取最前面那层反向代理写入的这个头。公网入口是
 *   华为云 Nginx → Instabox 端口转发 → 本机 TCP 转发 → 网关，后两层只转发字节、不加请求头，
 *   网关看到的来源地址都是同一个，所以要由 Nginx 用 {@code proxy_set_header} 写入 $remote_addr（会覆盖客户端自带的值）。</li>
 *   <li>否则取 X-Forwarded-For 里倒数第 {@code trusted-proxies} 项（为 0 时不看它）。</li>
 *   <li>都没有时用连接的来源地址。</li>
 * </ol>
 * 只有经过那层代理才能访问网关时，这个头才可信；网关不能直接对外暴露。
 */
@Configuration(proxyBeanMethods = false)
public class RateLimitConfiguration {
    /** 只接受 IPv4、IPv6 字面量，避免任意字符串变成 Redis key。 */
    private static final Pattern IP_LITERAL = Pattern.compile("[0-9A-Fa-f:.]{2,45}");

    @Bean
    public KeyResolver ipKeyResolver(GatewayProperties properties) {
        RemoteAddressResolver fallback = properties.trustedProxies() > 0
                ? XForwardedRemoteAddressResolver.maxTrustedIndex(properties.trustedProxies())
                : new RemoteAddressResolver() { };
        String header = properties.clientIpHeader();
        return exchange -> Mono.just(clientIp(exchange, header, fallback));
    }

    static String clientIp(ServerWebExchange exchange, String header, RemoteAddressResolver fallback) {
        if (header != null && !header.isBlank()) {
            String value = exchange.getRequest().getHeaders().getFirst(header);
            if (value != null) {
                String first = value.split(",", 2)[0].trim();
                if (IP_LITERAL.matcher(first).matches()) {
                    return first;
                }
            }
        }
        InetSocketAddress address = fallback.resolve(exchange);
        return address == null || address.getAddress() == null ? "unknown" : address.getAddress().getHostAddress();
    }
}
