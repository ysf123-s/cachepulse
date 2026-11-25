package com.cachepulse.delivery;

import com.cachepulse.delivery.replay.ArchiveClient;
import com.cachepulse.delivery.replay.ReplayJob;
import com.cachepulse.delivery.replay.ReplayService;
import com.cachepulse.framework.RemoteCallException;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.cachepulse.delivery.DeliveryTestFixture.inserted;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Replay fetches archived events through ArchiveClient (here a stub over the fixture's archived events). */
class ReplayPipelineTest {

    private DeliveryTestFixture fixture;
    private ReplayService replayService;

    @BeforeEach
    void setUp() throws Exception {
        fixture = new DeliveryTestFixture();
        fixture.target("replay-hook", "http://replay-target/hook");
        replayService = new ReplayService(fixture.properties, fixture.store, fixture.archive);
    }

    private String consume(long offset, String id, String name) {
        var record = fixture.record(offset, fixture.rows(CanalEntry.EventType.INSERT, inserted(id, name)));
        fixture.publish(record);
        return fixture.eventIds(record).get(0);
    }

    @Test
    @DisplayName("Replay job generates new delivery round with distinct deliveryId; an event missing from the archive is skipped")
    void replayJobGeneratesNewRound() {
        fixture.subscribe("sub-replay-1", "replay-hook", "INSERT", true);
        String eventId1 = consume(501, "501", "Item A");
        consume(502, "502", "Item B");

        List<DeliveryTask> initialTasks = fixture.store.listDeliveryTasks(null, "sub-replay-1", eventId1, 10);
        assertThat(initialTasks).hasSize(1);
        String initialDeliveryId = initialTasks.get(0).deliveryId();

        // The archive answers 404 (Optional.empty) for the unknown event: skipped, not a failure.
        ReplayJob job = replayService.createReplayJob(new ReplayJob.Request("req-rep-001", "sub-replay-1", List.of(eventId1, "f".repeat(64))));

        assertThat(job.status()).isEqualTo("COMPLETED");
        assertThat(job.totalEvents()).isEqualTo(2);
        assertThat(job.matchedEvents()).isEqualTo(1);
        assertThat(job.skippedEvents()).isEqualTo(1);
        assertThat(job.lastError()).isNull();

        List<DeliveryTask> replayTasks = fixture.store.listDeliveryTasks(null, "sub-replay-1", eventId1, 10);
        assertThat(replayTasks).hasSize(2); // 1 INITIAL + 1 REPLAY
        DeliveryTask replayTask = replayTasks.stream().filter(t -> "REPLAY".equals(t.roundType())).findFirst().orElseThrow();
        assertThat(replayTask.roundId()).isEqualTo(job.jobId());
        assertThat(replayTask.deliveryId()).isNotEqualTo(initialDeliveryId);
        assertThat(replayTask.status()).isEqualTo("PENDING");
        assertThat(replayTask.targetUrl()).isEqualTo("http://replay-target/hook");
        assertThat(fixture.store.decode(replayTask.eventPayload(), java.util.Map.class)).containsEntry("tableName", "inventory");
    }

    @Test
    @DisplayName("Archive unavailable: the job stays PENDING with lastError and completes once the archive answers")
    void remoteFailureLeavesJobPendingForRetry() {
        fixture.subscribe("sub-remote", "replay-hook", "INSERT", true);
        String eventId = consume(505, "505", "Item E");
        AtomicReference<RuntimeException> outage = new AtomicReference<>(
                RemoteCallException.unavailable("archive-service", new SocketTimeoutException("read timed out")));
        ArchiveClient flaky = id -> {
            if (outage.get() != null) throw outage.get();
            return fixture.archive.event(id);
        };
        ReplayService service = new ReplayService(fixture.properties, fixture.store, flaky);

        ReplayJob job = service.createReplayJob(new ReplayJob.Request("req-remote", "sub-remote", List.of(eventId)));
        assertThat(job.status()).isEqualTo("PENDING");
        assertThat(job.lastError()).startsWith("archive-service: ").contains("unavailable");
        assertThat(fixture.store.replayDeliveryStats(job.jobId())).isEmpty();

        outage.set(null);
        service.resumePending();
        ReplayJob done = fixture.store.findReplayJob(job.jobId());
        assertThat(done.status()).isEqualTo("COMPLETED");
        assertThat(done.matchedEvents()).isEqualTo(1);
        assertThat(done.lastError()).isNull();
    }

    @Test
    @DisplayName("Every event missing from the archive counts as skipped")
    void allMissingEventsAreSkipped() {
        fixture.subscribe("sub-missing", "replay-hook", "INSERT", true);
        ReplayService service = new ReplayService(fixture.properties, fixture.store, id -> Optional.empty());
        ReplayJob job = service.createReplayJob(new ReplayJob.Request("req-missing", "sub-missing", List.of("a".repeat(64), "b".repeat(64))));
        assertThat(job.status()).isEqualTo("COMPLETED");
        assertThat(job.matchedEvents()).isZero();
        assertThat(job.skippedEvents()).isEqualTo(2);
        assertThat(fixture.store.replayDeliveryStats(job.jobId())).isEmpty();
    }

    @Test
    @DisplayName("Duplicate replay request key is idempotent and does not create duplicate jobs")
    void replayDeduplication() {
        fixture.subscribe("sub-replay-2", "replay-hook", "INSERT", true);
        String eventId = consume(503, "503", "Item C");

        ReplayJob.Request request = new ReplayJob.Request("same-key-123", "sub-replay-2", List.of(eventId));
        ReplayJob first = replayService.createReplayJob(request);
        ReplayJob second = replayService.createReplayJob(request);

        assertThat(second.jobId()).isEqualTo(first.jobId());
        // 1 initial + 1 replay = exactly 2 tasks
        assertThat(fixture.store.listDeliveryTasks(null, "sub-replay-2", eventId, 10)).hasSize(2);
    }

    @Test
    @DisplayName("Replay rejects disabled subscription and oversized event batch")
    void replayValidation() {
        fixture.subscribe("sub-disabled", "replay-hook", "INSERT", false);

        ReplayJob.Request req1 = new ReplayJob.Request("req-fail-1", "sub-disabled", List.of("e".repeat(64)));
        assertThatThrownBy(() -> replayService.createReplayJob(req1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disabled subscription");

        List<String> largeList = new ArrayList<>();
        for (int i = 0; i < 101; i++) largeList.add("event-" + i);
        ReplayJob.Request req2 = new ReplayJob.Request("req-fail-2", "sub-disabled", largeList);
        assertThatThrownBy(() -> replayService.createReplayJob(req2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limited to at most 100 events");
    }

    @Test
    @DisplayName("HTTP controller creates and queries replay jobs")
    void controllerReplayEndpoints() throws Exception {
        fixture.subscribe("sub-http-replay", "replay-hook", "INSERT", true);
        String eventId = consume(504, "504", "Item D");

        MockMvc mvc = MockMvcBuilders.standaloneSetup(new SubscriptionController(
                fixture.properties, fixture.tables, fixture.store,
                mock(DeliveryExecutor.class), replayService, fixture.consumer)).build();

        String body = String.format("""
                {
                    "requestKey": "http-key-001",
                    "subscriptionId": "sub-http-replay",
                    "eventIds": ["%s"]
                }
                """, eventId);

        String res = mvc.perform(post("/platform/replay-jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.jobId").exists())
                .andExpect(jsonPath("$.matchedEvents").value(1))
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andReturn().getResponse().getContentAsString();

        String jobId = new com.fasterxml.jackson.databind.ObjectMapper().readTree(res).get("jobId").asText();

        mvc.perform(get("/platform/replay-jobs/{id}", jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.requestKey").value("http-key-001"))
                .andExpect(jsonPath("$.totalEvents").value(1));
        mvc.perform(get("/platform/replay-jobs/{id}/delivery-stats", jobId))
                .andExpect(status().isOk()).andExpect(jsonPath("$.PENDING").value(1));
        mvc.perform(get("/platform/deliveries/consumer-status"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.groupId").value("cachepulse-delivery"));
    }
}
