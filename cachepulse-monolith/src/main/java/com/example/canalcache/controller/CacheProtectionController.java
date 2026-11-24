package com.example.canalcache.controller;

import com.example.canalcache.service.QueryProtection;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 查询保护状态：Redis 熔断、查库并发与拒绝次数、主键布隆过滤器。 */
@RestController
@RequestMapping("/cache")
public class CacheProtectionController {
    private final QueryProtection protection;

    public CacheProtectionController(QueryProtection protection) {
        this.protection = protection;
    }

    @GetMapping("/protection")
    public QueryProtection.Status protection() {
        return protection.status();
    }
}
