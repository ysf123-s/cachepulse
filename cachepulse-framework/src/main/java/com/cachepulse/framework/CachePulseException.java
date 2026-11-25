package com.cachepulse.framework;

import org.springframework.http.HttpStatus;

/** 三层异常的基类：客户端错误（4xx）、服务端错误（5xx）、远程调用错误（下游失败）。 */
public abstract class CachePulseException extends RuntimeException {
    private final HttpStatus status;
    private final String code;

    protected CachePulseException(HttpStatus status, String code, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
        this.code = code;
    }

    public HttpStatus status() {
        return status;
    }

    public String code() {
        return code;
    }
}
