package com.cachepulse.framework;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/** 把 traceId 写到响应头 X-Trace-Id，方便拿着它去各服务日志里查同一次请求。 */
public class TraceIdResponseFilter extends OncePerRequestFilter {
    public static final String HEADER = "X-Trace-Id";
    private final TraceIds traceIds;

    public TraceIdResponseFilter(TraceIds traceIds) {
        this.traceIds = traceIds;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String traceId = traceIds.current();
        if (traceId != null) {
            response.setHeader(HEADER, traceId);
        }
        chain.doFilter(request, response);
    }
}
