package com.cachepulse.delivery;

import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.delivery.replay.ArchiveClient;
import com.cachepulse.delivery.replay.ReplayService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.sql.DataSource;
import java.sql.SQLException;

/** 投递服务的数据源和组件：业务库只读表结构，平台数据写入 cp_delivery。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PlatformProperties.class)
@EnableScheduling
public class DeliveryConfiguration {
    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("delivery-task-");
        return scheduler;
    }

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties businessDataSourceProperties() { return new DataSourceProperties(); }

    @Bean(name = "dataSource")
    @Primary
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource businessDataSource(
            @Qualifier("businessDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    @ConfigurationProperties("platform.datasource")
    public DataSourceProperties platformDataSourceProperties() { return new DataSourceProperties(); }

    @Bean
    @ConfigurationProperties("platform.datasource.hikari")
    public HikariDataSource platformDataSource(
            @Qualifier("platformDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    public DeliveryStore deliveryStore(@Qualifier("platformDataSource") DataSource platform, ObjectMapper json,
                                       PlatformProperties properties) throws SQLException {
        try (var connection = platform.getConnection()) {
            String catalog = connection.getCatalog();
            if (properties.getDatabase().equalsIgnoreCase(catalog)
                    || PlatformProperties.LEGACY_PLATFORM_DATABASE.equalsIgnoreCase(catalog)) {
                throw new IllegalArgumentException("Delivery storage must be its own database, not " + catalog);
            }
        }
        return new DeliveryStore(new JdbcTemplate(platform), json);
    }

    @Bean
    public TableCatalog tableCatalog(PlatformProperties properties, @Qualifier("dataSource") DataSource business,
            ObjectMapper json,
            @Value("${canal.mq.kafka.flat-message:false}") boolean flatMessage,
            @Value("${cachepulse.cache-sync-group:cachepulse-cache-sync}") String cacheGroup) throws SQLException {
        properties.validate(flatMessage, cacheGroup);
        return TableCatalog.load(properties, business, json);
    }

    @Bean
    public DeliveryExecutor deliveryExecutor(PlatformProperties properties, DeliveryStore store, ObjectMapper json) {
        return new DeliveryExecutor(properties, store, null, json);
    }

    @Bean
    public ArchiveClient.Fallback archiveClientFallback() {
        return new ArchiveClient.Fallback();
    }

    @Bean
    public ReplayService replayService(PlatformProperties properties, DeliveryStore store, ArchiveClient archive) {
        return new ReplayService(properties, store, archive);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public DeliveryConsumer deliveryConsumer(CanalConnectorWrapper connector, BinlogParser parser,
            PlatformProperties properties, TableCatalog tables, DeliveryStore store) {
        return new DeliveryConsumer(connector, parser, properties, tables, store);
    }
}
