package com.cachepulse.framework;

import org.springframework.http.HttpStatus;

/** 请求本身有问题：参数错误、资源不存在、状态冲突。 */
public class ClientException extends CachePulseException {
    public ClientException(HttpStatus status, String code, String message) {
        super(status, code, message, null);
    }

    public static ClientException badRequest(String message) {
        return new ClientException(HttpStatus.BAD_REQUEST, "BAD_REQUEST", message);
    }

    public static ClientException notFound(String message) {
        return new ClientException(HttpStatus.NOT_FOUND, "NOT_FOUND", message);
    }

    public static ClientException conflict(String message) {
        return new ClientException(HttpStatus.CONFLICT, "CONFLICT", message);
    }
}
