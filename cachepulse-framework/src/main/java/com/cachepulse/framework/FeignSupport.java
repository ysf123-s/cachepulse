package com.cachepulse.framework;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.RequestInterceptor;
import feign.Response;
import feign.codec.ErrorDecoder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.io.InputStream;

/**
 * 服务间调用的公共约定：透传网关写入的用户身份、带上内部令牌；下游的 ProblemDetail 转成 RemoteCallException。
 * traceparent 由 Micrometer Tracing 自动加上（需开启 spring.cloud.openfeign.micrometer.enabled）。
 */
public final class FeignSupport {
    public static final String USER_HEADER = "X-User-Id";

    private FeignSupport() { }

    public static RequestInterceptor headers(String internalToken) {
        return template -> {
            if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attrs) {
                String user = attrs.getRequest().getHeader(USER_HEADER);
                if (user != null) {
                    template.header(USER_HEADER, user);
                }
            }
            if (internalToken != null && !internalToken.isBlank()) {
                template.header(InternalTokenFilter.HEADER, internalToken);
            }
        };
    }

    public static ErrorDecoder errorDecoder(ObjectMapper json) {
        return (methodKey, response) -> {
            String service = response.request() == null || response.request().requestTemplate() == null
                    || response.request().requestTemplate().feignTarget() == null
                    ? methodKey : response.request().requestTemplate().feignTarget().name();
            String code = "REMOTE_ERROR";
            String detail = "HTTP " + response.status() + " from " + service;
            String traceId = null;
            JsonNode body = read(json, response);
            if (body != null) {
                code = body.path("code").asText(code);
                detail = body.path("detail").asText(detail);
                traceId = body.path("traceId").isMissingNode() ? null : body.path("traceId").asText(null);
            }
            return new RemoteCallException(service, response.status(), code, detail, traceId, null);
        };
    }

    private static JsonNode read(ObjectMapper json, Response response) {
        if (response.body() == null) {
            return null;
        }
        try (InputStream in = response.body().asInputStream()) {
            return json.readTree(in);
        } catch (Exception e) {
            return null;
        }
    }
}
