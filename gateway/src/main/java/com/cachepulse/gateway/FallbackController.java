package com.cachepulse.gateway;

import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

/**
 * 路由熔断的降级：下游超时、熔断打开、没有注册的实例时返回 503，格式和各服务的 ProblemDetail 一致。
 * 不返回伪造数据；管理页面只看状态码和原始响应体。
 */
@RestController
public class FallbackController {
    private final TraceIdHeaderFilter traceIds;

    public FallbackController(TraceIdHeaderFilter traceIds) {
        this.traceIds = traceIds;
    }

    @RequestMapping("/fallback/{service}")
    public ResponseEntity<ProblemDetail> fallback(@PathVariable String service, ServerWebExchange exchange) {
        Throwable cause = exchange.getAttribute(ServerWebExchangeUtils.CIRCUITBREAKER_EXECUTION_EXCEPTION_ATTR);
        String reason = cause == null ? "unavailable" : cause.getClass().getSimpleName();
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                service + " is temporarily unavailable (" + reason + ")");
        problem.setProperty("code", "UPSTREAM_UNAVAILABLE");
        problem.setProperty("service", service);
        problem.setProperty("traceId", traceIds.traceId(exchange));
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON).body(problem);
    }
}
