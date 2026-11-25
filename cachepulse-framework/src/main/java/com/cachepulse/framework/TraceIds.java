package com.cachepulse.framework;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;

/** 读取当前请求的 traceId，没有链路追踪上下文时返回 null。 */
public class TraceIds {
    private final ObjectProvider<Tracer> tracer;

    public TraceIds(ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
    }

    public String current() {
        Tracer current = tracer.getIfAvailable();
        Span span = current == null ? null : current.currentSpan();
        return span == null ? null : span.context().traceId();
    }
}
