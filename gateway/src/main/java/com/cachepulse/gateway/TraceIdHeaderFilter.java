package com.cachepulse.gateway;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingObservationHandler;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.server.reactive.observation.ServerRequestObservationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * 把本次请求的 traceId 写到响应头 X-Trace-Id，拿着它可以在各服务日志里查同一次请求。
 * trace 由网关的请求观测生成，经 traceparent 传给下游（Spring Cloud Gateway 自带）。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceIdHeaderFilter implements WebFilter {
    public static final String HEADER = "X-Trace-Id";
    private final ObjectProvider<Tracer> tracer;

    public TraceIdHeaderFilter(ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        exchange.getResponse().beforeCommit(() -> {
            String traceId = traceId(exchange);
            if (traceId != null) {
                exchange.getResponse().getHeaders().set(HEADER, traceId);
            }
            return Mono.empty();
        });
        return chain.filter(exchange);
    }

    public String traceId(ServerWebExchange exchange) {
        Span span = null;
        ServerRequestObservationContext context = ServerRequestObservationContext.findCurrent(exchange.getAttributes()).orElse(null);
        if (context != null) {
            TracingObservationHandler.TracingContext tracing = context.get(TracingObservationHandler.TracingContext.class);
            span = tracing == null ? null : tracing.getSpan();
        }
        if (span == null) {
            Tracer current = tracer.getIfAvailable();
            span = current == null ? null : current.currentSpan();
        }
        return span == null ? null : span.context().traceId();
    }
}
