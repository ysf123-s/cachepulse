package com.cachepulse.archive;

import com.cachepulse.common.kafka.CanalConnectorWrapper;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.List;
import java.util.Map;

import static com.cachepulse.archive.ArchiveTestFixture.inserted;
import static org.assertj.core.api.Assertions.*;

/**
 * Real Boot scanning, framework auto-configuration, HTTP endpoints and relational transactions of the archive
 * service alone. Nacos is replaced by the repository's config/nacos files; Kafka is mocked. Delivery has its own
 * boot test (DeliveryApplicationTest) since the archive no longer creates delivery tasks (design 6.4).
 */
@SpringBootTest(classes = ArchiveApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "cachepulse.config-prefix=optional:file:./config/nacos/",
        "spring.cloud.nacos.discovery.enabled=false", "spring.cloud.nacos.config.enabled=false",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "spring.cloud.service-registry.auto-registration.enabled=false",
        "server.address=127.0.0.1", "LOG_DIR=archive-service/target/test-logs"})
class PlatformApplicationTest {
    static final ArchiveTestFixture fixture;
    static {
        try { fixture = new ArchiveTestFixture(); }
        catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("platform.database", fixture.properties::getDatabase);
        p.add("platform.tables[0]", () -> "inventory");
        p.add("platform.datasource.url", fixture.platform::getURL);
        p.add("platform.datasource.username", () -> "sa");
        p.add("platform.datasource.password", () -> "");
        p.add("platform.datasource.driver-class-name", () -> "org.h2.Driver");
        p.add("spring.datasource.url", fixture.business::getURL);
        p.add("spring.datasource.username", () -> "sa");
        p.add("spring.datasource.password", () -> "");
        p.add("spring.datasource.driver-class-name", () -> "org.h2.Driver");
    }

    @MockitoBean CanalConnectorWrapper connector;
    @MockitoBean ArchiveConsumer consumer;
    @Autowired TestRestTemplate http;
    @Autowired ArchiveService archive;

    @Test
    @SuppressWarnings("unchecked")
    void actualApplicationArchivesAndServesHistoryAndInternalEvents() {
        archive.archive(fixture.record(88, fixture.rows(EventType.INSERT, inserted("88", "actual HTTP path"))));

        var page = http.getForEntity("/platform/events?table=inventory", Map.class);
        assertThat(page.getStatusCode()).isEqualTo(HttpStatus.OK);
        var items = (List<Map<String, Object>>) page.getBody().get("items");
        assertThat(items).singleElement().satisfies(item -> assertThat(item).containsEntry("primaryKeyValue", "88"));
        String eventId = (String) items.get(0).get("eventId");

        var internal = http.getForEntity("/internal/events/" + eventId, Map.class);
        assertThat(internal.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat((Map<String, Object>) internal.getBody().get("afterData")).containsEntry("name", "actual HTTP path");
        var missing = http.getForEntity("/internal/events/" + "f".repeat(64), Map.class);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        // 真实服务里的错误体：code、traceId 在顶层（框架的 ObjectMapper 注册了 ProblemDetail mixin），Feign 错误解码靠它们
        assertThat(missing.getBody()).containsEntry("status", 404).containsEntry("code", "NOT_FOUND")
                .containsKey("traceId").doesNotContainKey("properties");
        assertThat(http.getForEntity("/internal/events/not-a-hash", Map.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        assertThat(http.getForObject("/platform/tables", List.class)).hasSize(1);
        assertThat(http.getForEntity("/platform/events?table=not_registered", Map.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        // Delivery endpoints belong to the delivery service now.
        assertThat(http.getForEntity("/platform/subscriptions", String.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
