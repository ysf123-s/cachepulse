package com.cachepulse.delivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

/** 投递服务：按自己的消费组匹配订阅、生成投递任务、投递和重试；重放时经 Feign 向归档服务取历史事件。 */
@SpringBootApplication(scanBasePackages = {"com.cachepulse.delivery", "com.cachepulse.common.parser", "com.cachepulse.common.kafka"})
@EnableFeignClients(basePackages = "com.cachepulse.delivery.replay")
public class DeliveryApplication {
    public static void main(String[] args) {
        SpringApplication.run(DeliveryApplication.class, args);
    }
}
