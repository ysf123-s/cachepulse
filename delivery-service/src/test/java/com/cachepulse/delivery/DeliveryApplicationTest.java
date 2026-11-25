package com.cachepulse.delivery;

import com.cachepulse.common.kafka.CanalConnectorWrapper;
import com.cachepulse.common.parser.BinlogParser;
import com.cachepulse.common.platform.PlatformProperties;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.cachepulse.delivery.DeliveryTestFixture.inserted;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Real Boot scanning, HTTP endpoints, scheduler, relational transactions and localhost HTTP delivery of the delivery
 * service alone (the delivery half of the monolith's PlatformApplicationTest). Nacos is replaced by the repository's
 * config/nacos files; Kafka is mocked and the consumer's handle() is fed directly. Not a MySQL/Canal/Kafka E2E test.
 */
@SpringBootTest(classes = DeliveryApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "cachepulse.config-prefix=optional:file:./config/nacos/",
        "spring.cloud.nacos.discovery.enabled=false", "spring.cloud.nacos.config.enabled=false",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "spring.cloud.service-registry.auto-registration.enabled=false",
        "server.address=127.0.0.1", "LOG_DIR=delivery-service/target/test-logs",
        "platform.dispatch-delay-ms=50"})
class DeliveryApplicationTest {
    static final DeliveryTestFixture fixture;
    static final HttpServer receiver;
    static final AtomicReference<String> received = new AtomicReference<>();
    static {
        try {
            fixture = new DeliveryTestFixture();
            receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            receiver.createContext("/hook", exchange -> {
                received.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
            });
            receiver.start();
        } catch (Exception e) { throw new ExceptionInInitializerError(e); }
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
        p.add("platform.targets.hook.url", () -> "http://127.0.0.1:" + receiver.getAddress().getPort() + "/hook");
    }

    @AfterAll static void stopReceiver() { receiver.stop(0); }

    @MockitoBean CanalConnectorWrapper connector;
    @MockitoBean DeliveryConsumer backgroundConsumer;
    @Autowired TestRestTemplate http;
    @Autowired PlatformProperties properties;
    @Autowired TableCatalog tables;
    @Autowired DeliveryStore store;
    @Autowired BinlogParser parser;

    @Test
    void actualApplicationStartsAndConsumedEventsProduceAutomaticHttpDelivery() {
        assertThat(http.getForObject("/platform/examples", List.class)).containsExactly("stockout");
        assertThat(http.getForObject("/platform/targets", List.class)).contains("hook", "demo-webhook");
        var created = http.postForEntity("/platform/subscriptions",
                Map.of("table", "inventory", "targetName", "hook", "operations", List.of("INSERT")), Map.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String subId = (String) created.getBody().get("subscriptionId");

        var consumer = new DeliveryConsumer(connector, parser, properties, tables, store);
        assertThat(consumer.handle(List.of(fixture.record(88, fixture.rows(EventType.INSERT, inserted("88", "actual HTTP path")))))
                .tasks()).isEqualTo(1);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(received.get()).contains("\"deliveryId\"", "actual HTTP path");
            assertThat(store.listDeliveryTasks("SUCCEEDED", subId, null, 10)).hasSize(1);
        });

        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        assertThat(http.exchange("/platform/subscriptions/" + subId + "/enabled?expectedVersion=1", HttpMethod.PUT,
                new HttpEntity<>(Map.of("enabled", false), headers), Map.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(http.postForEntity("/platform/subscriptions", Map.of("table", "inventory", "targetName", "hook",
                "operations", List.of("UPDATE"), "filterField", "available", "filterValue", "not-a-number"), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        // Archive endpoints belong to the archive service now.
        assertThat(http.getForEntity("/platform/events?table=inventory", String.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
