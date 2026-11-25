package com.cachepulse.framework;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * 所有服务统一的错误响应：RFC 9457 ProblemDetail，扩展 code 和 traceId。HTTP 状态码沿用原项目约定：
 * 参数错误 400、不存在 404、状态冲突 409、依赖不可用 503，不改成"一律 200 再放业务码"。
 */
@Slf4j
@RestControllerAdvice
@Order(Ordered.LOWEST_PRECEDENCE)
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {
    private final TraceIds traceIds;

    public GlobalExceptionHandler(TraceIds traceIds) {
        this.traceIds = traceIds;
    }

    @ExceptionHandler(CachePulseException.class)
    public ResponseEntity<ProblemDetail> cachePulse(CachePulseException error) {
        if (error instanceof RemoteCallException remote) {
            log.warn("Remote call to {} failed: status={}, code={}, remoteTraceId={}",
                    remote.service(), remote.remoteStatus(), remote.code(), remote.remoteTraceId());
        }
        return respond(error.status(), error.code(), error.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ProblemDetail> invalid(IllegalArgumentException error) {
        return respond(HttpStatus.BAD_REQUEST, "BAD_REQUEST", error.getMessage());
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<ProblemDetail> missing(NoSuchElementException error) {
        return respond(HttpStatus.NOT_FOUND, "NOT_FOUND", error.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ProblemDetail> conflict(IllegalStateException error) {
        return respond(HttpStatus.CONFLICT, "CONFLICT", error.getMessage());
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException error,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        ProblemDetail body = problem(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "Request validation failed");
        Map<String, String> fields = error.getBindingResult().getFieldErrors().stream()
                .collect(Collectors.toMap(f -> f.getField(), f -> String.valueOf(f.getDefaultMessage()), (a, b) -> a));
        body.setProperty("fields", fields);
        return ResponseEntity.badRequest().body(body);
    }

    /**
     * Spring 自带的异常（包括控制器抛的 ResponseStatusException）也补上 code 和 traceId：
     * code 取 HTTP 状态名（NOT_FOUND、BAD_REQUEST 等），和本类其他处理方法一致。
     */
    @Override
    protected ResponseEntity<Object> createResponseEntity(Object body, HttpHeaders headers, HttpStatusCode status,
                                                          WebRequest request) {
        if (body instanceof ProblemDetail problem) {
            Map<String, Object> properties = problem.getProperties();
            if (properties == null || !properties.containsKey("code")) {
                HttpStatus resolved = HttpStatus.resolve(status.value());
                problem.setProperty("code", resolved == null ? "HTTP_" + status.value() : resolved.name());
            }
            if (properties == null || !properties.containsKey("traceId")) {
                problem.setProperty("traceId", traceIds.current());
            }
        }
        return super.createResponseEntity(body, headers, status, request);
    }

    private ResponseEntity<ProblemDetail> respond(HttpStatus status, String code, String detail) {
        return ResponseEntity.status(status).body(problem(status, code, detail));
    }

    private ProblemDetail problem(HttpStatus status, String code, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setProperty("code", code);
        problem.setProperty("traceId", traceIds.current());
        return problem;
    }
}
