package com.cachepulse.archive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 归档服务：按自己的消费组归档变更、保存表结构快照、生成回滚 SQL。 */
@SpringBootApplication(scanBasePackages = {"com.cachepulse.archive", "com.cachepulse.common.parser", "com.cachepulse.common.kafka"})
public class ArchiveApplication {
    public static void main(String[] args) {
        SpringApplication.run(ArchiveApplication.class, args);
    }
}
