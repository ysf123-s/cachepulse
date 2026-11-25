package com.cachepulse.demo.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 原单体里缺货待办只在 stockout-demo profile 下加载；拆分后这是库存服务的全部功能，
 * 去掉了 profile 限制（设计文档第 14.1 节第 8 条）。这里验证不设任何 profile 也会加载并能落库。
 */
class StockoutProfileTest {

    @Test
    void controllerAndStoreLoadWithoutAnyProfile() throws Exception {
        DataSource db = StockoutTestDatabase.create("stockout_profile");
        new ApplicationContextRunner()
                .withUserConfiguration(StockoutController.class, StockoutStore.class)
                .withBean("dataSource", DataSource.class, () -> db)
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> {
                    assertThat(context.getEnvironment().getActiveProfiles()).isEmpty();
                    assertThat(context).hasNotFailed().hasSingleBean(StockoutController.class)
                            .hasSingleBean(StockoutStore.class);
                    assertThat(context.getBean(StockoutStore.class)
                            .receive("a".repeat(64), 1, "SKU-1", 1700000000000L).status()).isEqualTo("PENDING");
                });
    }
}
