package com.cachepulse.cache.starter.fixture;

import com.cachepulse.cache.annotation.AutoCache;
import com.cachepulse.cache.annotation.CacheStrategy;

import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

/** 测试用实体，和 demo-user-service 的 departments 分组定义相同。 */
@Data
@AutoCache(table = "departments", keyTemplate = "department:#id", strategy = CacheStrategy.DELETE_MODE,
        expireSeconds = 600, enableVersionControl = false, bloomFilter = true)
public class Department {
    @TableId("id")
    private Long id;
    private String name;
    private String code;
}
