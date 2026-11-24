package com.example.canalcache.service;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/** 查库名额已满，本次查询被拒绝。调用方稍后重试即可，不代表数据不存在。 */
@ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
public class CacheLoadRejectedException extends RuntimeException {
    private final CacheLoadGuard.Lane lane;

    public CacheLoadRejectedException(CacheLoadGuard.Lane lane, int limit) {
        super("Too many concurrent cache loads (" + lane + " lane, limit " + limit + "), retry later");
        this.lane = lane;
    }

    /** 合并等待的请求在自己的线程里重新抛出，原异常作为 cause。 */
    public CacheLoadRejectedException(CacheLoadRejectedException original) {
        super(original.getMessage(), original);
        this.lane = original.lane;
    }

    public CacheLoadGuard.Lane lane() {
        return lane;
    }
}
