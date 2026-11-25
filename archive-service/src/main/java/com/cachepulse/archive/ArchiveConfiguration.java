package com.cachepulse.archive;

import com.cachepulse.archive.recovery.RecoveryService;
import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;

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
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;

/** 归档服务的数据源和组件：业务库只读（读表结构、生成回滚 SQL），平台数据写入 cp_archive。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PlatformProperties.class)
public class ArchiveConfiguration {
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
    public ArchiveStore archiveStore(@Qualifier("platformDataSource") DataSource platform, ObjectMapper json) {
        return new ArchiveStore(new JdbcTemplate(platform), json);
    }

    @Bean
    public TableRegistry platformTableRegistry(PlatformProperties properties,
            ArchiveStore store, @Qualifier("dataSource") DataSource business,
            @Qualifier("platformDataSource") DataSource platform,
            @Value("${canal.mq.kafka.flat-message:false}") boolean flatMessage,
            @Value("${cachepulse.cache-sync-group:cachepulse-cache-sync}") String cacheGroup) throws SQLException {
        properties.validate(flatMessage, cacheGroup);
        return TableRegistry.capture(properties, store, business, platform);
    }

    @Bean
    public ArchiveService archiveService(PlatformProperties properties,
            TableRegistry tables, BinlogParser parser, ArchiveStore store,
            @Qualifier("platformDataSource") DataSource platform) {
        TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(platform));
        transaction.setTimeout(20);
        return new ArchiveService(properties, tables, parser, store, transaction);
    }

    @Bean
    public RecoveryService recoveryService(PlatformProperties properties, TableRegistry tables,
            ArchiveStore store, @Qualifier("dataSource") DataSource business, ObjectMapper json) {
        return new RecoveryService(properties, tables, store, new JdbcTemplate(business), json);
    }



    @Bean(initMethod = "start", destroyMethod = "close")
    public ArchiveConsumer archiveConsumer(CanalConnectorWrapper connector, ArchiveService archive,
            PlatformProperties properties) {
        return new ArchiveConsumer(connector, archive, properties.getGroupId());
    }
}
