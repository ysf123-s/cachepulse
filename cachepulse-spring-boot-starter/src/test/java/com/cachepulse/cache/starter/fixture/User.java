package com.cachepulse.cache.starter.fixture;

import com.cachepulse.cache.annotation.AutoCache;
import com.cachepulse.cache.annotation.CacheStrategy;

import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

/** 测试用实体，和 demo-user-service 的 users 分组定义相同。 */
@Data
@AutoCache(table = "users", keyTemplate = "user:#id", indexKeys = "user-email:#email",
        strategy = CacheStrategy.DELETE_MODE, expireSeconds = 1800, enableVersionControl = false, bloomFilter = true)
public class User {
    @TableId("id")
    private Long id;
    private String name;
    private String email;
}
