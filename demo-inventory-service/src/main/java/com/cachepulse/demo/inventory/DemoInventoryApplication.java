package com.cachepulse.demo.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 演示接入方：作为订阅的接收端，库存降到 0 时生成缺货待办。 */
@SpringBootApplication
public class DemoInventoryApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoInventoryApplication.class, args);
    }
}
