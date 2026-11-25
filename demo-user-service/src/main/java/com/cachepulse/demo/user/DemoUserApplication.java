package com.cachepulse.demo.user;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 演示接入方：用户、部门。写库不改代码，读缓存通过接入组件 CacheQueryService。 */
@SpringBootApplication
@MapperScan("com.cachepulse.demo.user.mapper")
public class DemoUserApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoUserApplication.class, args);
    }
}
