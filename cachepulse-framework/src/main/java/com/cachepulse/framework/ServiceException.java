package com.cachepulse.framework;

import org.springframework.http.HttpStatus;

/** 本服务内部故障，或依赖暂时不可用（503）。 */
public class ServiceException extends CachePulseException {
    public ServiceException(HttpStatus status, String code, String message, Throwable cause) {
        super(status, code, message, cause);
    }

    public static ServiceException unavailable(String message, Throwable cause) {
        return new ServiceException(HttpStatus.SERVICE_UNAVAILABLE, "UNAVAILABLE", message, cause);
    }
}
