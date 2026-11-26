package com.cachepulse.gateway;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.filter.ratelimit.RateLimiter;
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.cloud.gateway.support.ConfigurationService;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatcher;
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatchers;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 登录接口按客户端 IP 限次数（设计文档 14.1 第 18 条）。/auth/token 是网关自己的接口，不经过路由，
 * 路由上的 RequestRateLimiter 管不到它；不限的话可以一直猜密码，每次 BCrypt 校验还要占几十毫秒 CPU。
 *
 * <p>每个 IP 平均每分钟 5 次，连续最多 10 次，超过返回 429。计数在 Redis，多个网关实例共用；
 * 用单独的 RedisRateLimiter 实例和单独的 key，不占路由的额度。Redis 出错时放行（RedisRateLimiter 的做法），
 * 登录本身仍要密码。客户端 IP 和路由限流用同一个 {@code ipKeyResolver}。</p>
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class LoginRateLimitFilter implements WebFilter {
    static final String LIMIT_ID = "auth-token";
    /** 每秒补 1 个令牌，每次登录用 12 个：平均每分钟 5 次；桶里最多 120 个，即连续 10 次。 */
    static final RedisRateLimiter.Config LIMIT = new RedisRateLimiter.Config()
            .setReplenishRate(1).setRequestedTokens(12).setBurstCapacity(120);
    /** 桶空了以后，攒够一次登录要的令牌最多要等这么久。 */
    static final long RETRY_AFTER_SECONDS = (LIMIT.getRequestedTokens() + LIMIT.getReplenishRate() - 1) / LIMIT.getReplenishRate();
    private static final ServerWebExchangeMatcher LOGIN =
            ServerWebExchangeMatchers.pathMatchers(HttpMethod.POST, "/auth/token");
    private static final Pattern TRACE_ID = Pattern.compile("[0-9a-f]{16,32}");

    private final RateLimiter<?> limiter;
    private final KeyResolver clientIp;
    private final TraceIdHeaderFilter traceIds;

    @Autowired
    public LoginRateLimitFilter(ReactiveStringRedisTemplate redis,
                                @Qualifier(RedisRateLimiter.REDIS_SCRIPT_NAME) RedisScript<List<Long>> script,
                                ConfigurationService configurationService,
                                @Qualifier("ipKeyResolver") KeyResolver clientIp,
                                TraceIdHeaderFilter traceIds) {
        this(loginLimiter(redis, script, configurationService), clientIp, traceIds);
    }

    LoginRateLimitFilter(RateLimiter<?> limiter, KeyResolver clientIp, TraceIdHeaderFilter traceIds) {
        this.limiter = limiter;
        this.clientIp = clientIp;
        this.traceIds = traceIds;
    }

    private static RedisRateLimiter loginLimiter(ReactiveStringRedisTemplate redis, RedisScript<List<Long>> script,
                                                 ConfigurationService configurationService) {
        RedisRateLimiter limiter = new RedisRateLimiter(redis, script, configurationService);
        limiter.getConfig().put(LIMIT_ID, LIMIT);
        return limiter;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // 和 SecurityConfiguration 一样按 PathPattern 匹配，编码过的写法（如 /auth/%74oken）也算登录
        return LOGIN.matches(exchange)
                .flatMap(match -> match.isMatch() ? limit(exchange, chain) : chain.filter(exchange));
    }

    private Mono<Void> limit(ServerWebExchange exchange, WebFilterChain chain) {
        return clientIp.resolve(exchange)
                .defaultIfEmpty("unknown")
                .flatMap(ip -> limiter.isAllowed(LIMIT_ID, ip))
                .flatMap(response -> {
                    response.getHeaders().forEach(exchange.getResponse().getHeaders()::set);
                    return response.isAllowed() ? chain.filter(exchange) : reject(exchange);
                });
    }

    private Mono<Void> reject(ServerWebExchange exchange) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        response.getHeaders().setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        response.getHeaders().set(HttpHeaders.RETRY_AFTER, Long.toString(RETRY_AFTER_SECONDS));
        String traceId = traceIds.traceId(exchange);
        String body = "{\"type\":\"about:blank\",\"title\":\"Too Many Requests\",\"status\":429,"
                + "\"detail\":\"Too many login attempts, try again later\",\"code\":\"LOGIN_RATE_LIMITED\""
                + (traceId != null && TRACE_ID.matcher(traceId).matches() ? ",\"traceId\":\"" + traceId + "\"" : "")
                + "}";
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8))));
    }
}
