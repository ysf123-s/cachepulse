package com.cachepulse.framework;

import org.springframework.http.HttpStatus;

/**
 * 调用其他服务失败：下游返回错误、超时、熔断或没有可用实例。保留下游的状态码、错误码和 traceId，
 * 对外统一返回 502（下游出错）或 503（下游不可用）。
 */
public class RemoteCallException extends CachePulseException {
    private final String service;
    private final int remoteStatus;
    private final String remoteTraceId;

    public RemoteCallException(String service, int remoteStatus, String code, String message,
                               String remoteTraceId, Throwable cause) {
        super(remoteStatus >= 500 || remoteStatus <= 0 ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.BAD_GATEWAY,
                code, message, cause);
        this.service = service;
        this.remoteStatus = remoteStatus;
        this.remoteTraceId = remoteTraceId;
    }

    public static RemoteCallException unavailable(String service, Throwable cause) {
        return new RemoteCallException(service, 0, "REMOTE_UNAVAILABLE",
                service + " is unavailable: " + cause.getClass().getSimpleName(), null, cause);
    }

    public String service() {
        return service;
    }

    public int remoteStatus() {
        return remoteStatus;
    }

    public String remoteTraceId() {
        return remoteTraceId;
    }

    /** 下游明确返回 4xx（如事件不存在）时为 true，调用方不应重试。 */
    public boolean clientError() {
        return remoteStatus >= 400 && remoteStatus < 500;
    }
}
