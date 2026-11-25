package com.cachepulse.archive;

import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** ArchiveConfiguration wiring: business datasource stays primary, archive storage is separate and guarded. */
class PlatformConfigurationTest {
    private ArchiveTestFixture fixture;
    private ApplicationContextRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        fixture = new ArchiveTestFixture();
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.createArchiveConsumer(anyString())).thenThrow(new IllegalStateException("Broker intentionally absent in datasource test"));
        runner = new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class,
                        JdbcTemplateAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class))
                .withUserConfiguration(ArchiveConfiguration.class)
                .withBean(BinlogParser.class, BinlogParser::new).withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(CanalConnectorWrapper.class, () -> connector)
                .withPropertyValues("spring.datasource.url=" + fixture.business.getURL(), "spring.datasource.username=sa",
                        "spring.datasource.driver-class-name=org.h2.Driver", "platform.database=" + fixture.properties.getDatabase(),
                        "platform.tables[0]=inventory", "platform.datasource.url=" + fixture.platform.getURL(),
                        "platform.datasource.username=sa", "platform.datasource.driver-class-name=org.h2.Driver");
    }

    @Test
    void businessDatasourceStaysPrimaryAndArchiveStorageIsSeparate() {
        new JdbcTemplate(fixture.business).update("INSERT INTO inventory (id,name) VALUES (1,'business database')");
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBeansOfType(DataSource.class)).hasSize(2);
            DataSource primary = context.getBean("dataSource", DataSource.class);
            assertThat(context.getBean(JdbcTemplate.class).getDataSource()).isSameAs(primary);
            assertThat(context.getBean(JdbcTemplate.class).queryForObject("SELECT name FROM inventory WHERE id=1", String.class))
                    .isEqualTo("business database");
            assertThat(((DataSourceTransactionManager) context.getBean(PlatformTransactionManager.class)).getDataSource()).isSameAs(primary);
            assertThat(context.getBean(TableRegistry.class).views()).hasSize(1);
        });
    }

    @Test
    void flatMessageCannotSilentlyEnableReducedPlatformCapabilities() {
        runner.withPropertyValues("canal.mq.kafka.flat-message=true").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage("Platform archiving requires canal.mq.kafka.flat-message=false");
        });
    }

    @Test
    void sharingCacheGroupIsRejectedBeforeStartingArchiveConsumer() {
        runner.withPropertyValues("platform.group-id=cache", "cachepulse.cache-sync-group=cache").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage("Platform consumer group must differ from cache group");
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"canal-cache-sync-mq", "platform-change-archive", "platform-change-archive-v2"})
    void originalProjectConsumerGroupsAreRejected(String legacy) {
        PlatformProperties archive = new PlatformProperties();
        archive.setGroupId(legacy);
        assertThatThrownBy(() -> archive.validate(false, "cachepulse-cache-sync"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("original project");
        PlatformProperties cacheGroup = new PlatformProperties();
        assertThatThrownBy(() -> cacheGroup.validate(false, legacy))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("original project");
        runner.withPropertyValues("platform.group-id=" + legacy).run(context ->
                assertThat(context.getStartupFailure()).hasRootCauseMessage(
                        "Consumer group belongs to the original project: " + legacy + ", cachepulse-cache-sync"));
    }

    @Test
    void originalPlatformDatabaseIsRejectedAsCapturedAndAsStorageDatabase() {
        PlatformProperties properties = new PlatformProperties();
        properties.setDatabase("cdc_platform");
        assertThatThrownBy(() -> properties.validate(false, "cachepulse-cache-sync"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Platform storage database cannot be captured");
        assertThat(new PlatformProperties()).satisfies(defaults -> defaults.validate(false, "cachepulse-cache-sync"));

        runner.withPropertyValues("platform.datasource.url=jdbc:h2:mem:cdc_platform;MODE=MySQL;DATABASE_TO_LOWER=TRUE").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage(
                    "Archive storage must be its own database, not the business or original platform database");
        });
    }
}
