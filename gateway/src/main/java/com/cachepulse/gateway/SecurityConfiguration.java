package com.cachepulse.gateway;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;
import org.springframework.security.oauth2.server.resource.web.server.authentication.ServerBearerTokenAuthenticationConverter;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.authentication.ServerAuthenticationConverter;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * 鉴权规则（设计文档第 9.2 节）：/api/** 的 GET 都公开，管理页面不登录也能看；
 * 用户、部门的按 ID 批量查询也公开；其余写操作要 JWT。公开请求都按客户端 IP 限流。
 * /internal/** 只在内部网络调用，网关直接拒绝。
 */
@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
public class SecurityConfiguration {
    public static final String TOKEN_HEADER = "X-CachePulse-Token";

    @Bean
    public SecretKey jwtSigningKey(GatewayProperties properties) {
        return new SecretKeySpec(properties.jwtKey(), "HmacSHA256");
    }

    @Bean
    public ReactiveJwtDecoder jwtDecoder(SecretKey key) {
        return NimbusReactiveJwtDecoder.withSecretKey(key).macAlgorithm(MacAlgorithm.HS256).build();
    }

    @Bean
    public JwtEncoder jwtEncoder(SecretKey key) {
        return new NimbusJwtEncoder(new ImmutableSecret<>(key));
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http, ReactiveJwtDecoder jwtDecoder) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .logout(ServerHttpSecurity.LogoutSpec::disable)
                .authorizeExchange(rules -> rules
                        .pathMatchers("/internal/**", "/api/internal/**").denyAll()
                        .pathMatchers(HttpMethod.POST, "/auth/token").permitAll()
                        .pathMatchers("/actuator/health", "/actuator/health/**").permitAll()
                        .pathMatchers("/fallback/**").permitAll()
                        .pathMatchers(HttpMethod.GET, "/", "/*.html", "/*.css", "/*.js", "/favicon.ico").permitAll()
                        // 只读接口公开：管理页面不登录也能看；新建、修改、删除、重放、清缓存才要登录
                        .pathMatchers(HttpMethod.GET, "/api/**").permitAll()
                        // 按 ID 批量查询也是只读，和单条查询一样公开、限流
                        .pathMatchers(HttpMethod.POST, "/api/users/batch", "/api/departments/batch").permitAll()
                        .anyExchange().authenticated())
                .oauth2ResourceServer(server -> server.bearerTokenConverter(tokenConverter())
                        .jwt(jwt -> jwt.jwtDecoder(jwtDecoder)))
                .build();
    }

    /**
     * 令牌优先从 X-CachePulse-Token 读取，没有时再读 Authorization: Bearer。
     * 公网入口的 Nginx 会把 Authorization 换成访问 Instabox 端口的凭据，浏览器带的 Bearer 令牌到不了网关。
     */
    static ServerAuthenticationConverter tokenConverter() {
        ServerBearerTokenAuthenticationConverter standard = new ServerBearerTokenAuthenticationConverter();
        return exchange -> {
            String token = exchange.getRequest().getHeaders().getFirst(TOKEN_HEADER);
            if (token != null && !token.isBlank()) {
                return Mono.just(new BearerTokenAuthenticationToken(token.trim()));
            }
            return standard.convert(exchange);
        };
    }
}
