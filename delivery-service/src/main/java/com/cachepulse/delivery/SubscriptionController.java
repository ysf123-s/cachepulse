package com.cachepulse.delivery;

import com.cachepulse.common.event.EventIdentity;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.delivery.replay.ReplayJob;
import com.cachepulse.delivery.replay.ReplayService;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.NoSuchElementException;

@RestController
@RequestMapping("/platform")
public class SubscriptionController {

    private final PlatformProperties properties;
    private final TableCatalog tables;
    private final DeliveryStore store;
    private final DeliveryExecutor deliveryExecutor;
    private final ReplayService replayService;
    private final DeliveryConsumer consumer;

    @org.springframework.beans.factory.annotation.Autowired
    public SubscriptionController(PlatformProperties properties, TableCatalog tables,
                                  DeliveryStore store, DeliveryExecutor deliveryExecutor,
                                  ReplayService replayService, DeliveryConsumer consumer) {
        this.properties = properties;
        this.tables = tables;
        this.store = store;
        this.deliveryExecutor = deliveryExecutor;
        this.replayService = replayService;
        this.consumer = consumer;
    }

    public SubscriptionController(PlatformProperties properties, TableCatalog tables,
                                  DeliveryStore store, DeliveryExecutor deliveryExecutor) {
        this(properties, tables, store, deliveryExecutor, null, null);
    }

    /** 投递消费者的状态。拆分后三个服务各报各的消费者，管理页面分别调用后合并展示（设计文档第 9.2 节）。 */
    @GetMapping("/deliveries/consumer-status")
    public DeliveryConsumer.Status consumerStatus() {
        if (consumer == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Delivery consumer is not available");
        }
        return consumer.status();
    }

    @GetMapping("/subscriptions")
    public List<EventSubscription> listSubscriptions() {
        return store.listSubscriptions(properties.getSourceId(), properties.getDatabase());
    }

    @GetMapping("/targets")
    public List<String> targets() { return properties.getTargets().keySet().stream().sorted().toList(); }

    @GetMapping("/examples")
    public List<String> examples() { return properties.getEnabledExamples(); }

    @PostMapping("/subscriptions")
    @ResponseStatus(HttpStatus.CREATED)
    public EventSubscription createSubscription(@RequestBody EventSubscription.Request request) {
        SubscriptionMatcher.validate(request, properties, tables);

        String ops = request.operations().stream().map(s -> s.toUpperCase(java.util.Locale.ROOT)).collect(java.util.stream.Collectors.joining(","));
        String changedFields = request.changedFields() != null ? String.join(",", request.changedFields()) : null;

        String id = EventIdentity.hash(String.join(":", List.of(
                "sub-v1", properties.getSourceId(), properties.getDatabase(),
                request.table(), request.targetName(), ops,
                changedFields != null ? changedFields : "",
                request.filterField() != null ? request.filterField() : "",
                request.filterValue() != null ? request.filterValue() : "",
                java.util.UUID.randomUUID().toString()
        )));

        EventSubscription sub = new EventSubscription(
                id,
                properties.getSourceId(),
                properties.getDatabase(),
                request.table(),
                request.targetName(),
                ops,
                changedFields,
                request.filterField(),
                request.filterValue(),
                request.enabled() != null ? request.enabled() : true,
                1,
                java.time.Instant.now().toString(),
                java.time.Instant.now().toString()
        );

        store.saveSubscription(sub);
        return store.findSubscription(id);
    }

    @PutMapping("/subscriptions/{id}")
    public EventSubscription updateSubscription(@PathVariable String id,
                                               @RequestBody EventSubscription.Request request,
                                               @RequestParam(required = false, defaultValue = "1") int expectedVersion) {
        EventSubscription existing = store.findSubscription(id);
        if (existing == null) {
            throw notFound("Subscription not found: " + id);
        }

        SubscriptionMatcher.validate(request, properties, tables);

        String ops = request.operations().stream().map(s -> s.toUpperCase(java.util.Locale.ROOT)).collect(java.util.stream.Collectors.joining(","));
        String changed = request.changedFields() == null ? null : String.join(",", request.changedFields());
        String filterF = request.filterField();
        String filterV = request.filterValue();
        boolean enabled = request.enabled() != null ? request.enabled() : existing.enabled();

        EventSubscription updated = new EventSubscription(
                id, existing.sourceId(), existing.database(), request.table(),
                request.targetName(), ops, changed, filterF, filterV, enabled, expectedVersion, null, null
        );

        boolean success = store.updateSubscription(updated);
        if (!success) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Rule version mismatch or subscription modified concurrently (expected: " + expectedVersion + ")");
        }

        return store.findSubscription(id);
    }

    public record EnabledRequest(Boolean enabled) { }

    @PutMapping("/subscriptions/{id}/enabled")
    public EventSubscription setEnabled(@PathVariable String id, @RequestBody EnabledRequest request,
                                       @RequestParam int expectedVersion) {
        if (request.enabled() == null) throw badRequest("enabled must be provided");
        EventSubscription current = store.findSubscription(id);
        if (current == null) throw notFound("Subscription not found");
        if (request.enabled() && !properties.getTargets().containsKey(current.targetName())) {
            throw badRequest("Target alias is not registered: " + current.targetName());
        }
        if (!store.updateSubscription(new EventSubscription(id, current.sourceId(), current.database(), current.table(),
                current.targetName(), current.operations(), current.changedFields(), current.filterField(), current.filterValue(),
                request.enabled(), expectedVersion, null, null))) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Rule version mismatch");
        }
        return store.findSubscription(id);
    }

    @GetMapping("/replay-jobs/{id}/delivery-stats")
    public java.util.Map<String, Long> replayStats(@PathVariable String id) {
        replayService.getJob(id);
        return store.replayDeliveryStats(id);
    }

    @GetMapping("/deliveries")
    public DeliveryTask.Page listDeliveries(@RequestParam(required = false) String status,
                                            @RequestParam(required = false) String subscriptionId,
                                            @RequestParam(required = false) String eventId,
                                            @RequestParam(required = false) String roundId,
                                            @RequestParam(required = false) Long cursor,
                                            @RequestParam(defaultValue = "50") int limit) {
        if (limit < 1 || limit > 200) {
            throw badRequest("limit must be between 1 and 200");
        }
        if (cursor != null && cursor <= 0) throw badRequest("cursor must be positive");
        List<DeliveryTask> rows = store.listDeliveryTasks(status, subscriptionId, eventId, roundId, cursor, limit + 1);
        boolean more = rows.size() > limit;
        List<DeliveryTask> items = more ? rows.subList(0, limit) : rows;
        return new DeliveryTask.Page(items, more ? Long.toString(items.get(items.size() - 1).taskId()) : null);
    }

    @GetMapping("/deliveries/{id}/attempts")
    public List<DeliveryAttempt> listAttempts(@PathVariable long id) {
        DeliveryTask task = store.findDeliveryTask(id);
        if (task == null) {
            throw notFound("Delivery task not found: " + id);
        }
        return store.listAttempts(id);
    }

    @PostMapping("/deliveries/{id}/retry")
    public DeliveryTask retryTask(@PathVariable long id) {
        try {
            return deliveryExecutor.manualRetry(id);
        } catch (NoSuchElementException e) {
            throw notFound(e.getMessage());
        } catch (IllegalStateException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, e.getMessage());
        }
    }

    @PostMapping("/replay-jobs")
    @ResponseStatus(HttpStatus.CREATED)
    public ReplayJob createReplayJob(@RequestBody ReplayJob.Request request) {
        if (replayService == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Replay service is not available");
        }
        try {
            return replayService.createReplayJob(request);
        } catch (NoSuchElementException e) {
            throw notFound(e.getMessage());
        } catch (IllegalArgumentException e) {
            throw badRequest(e.getMessage());
        }
    }

    @GetMapping("/replay-jobs/{id}")
    public ReplayJob getReplayJob(@PathVariable String id) {
        if (replayService == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Replay service is not available");
        }
        try {
            return replayService.getJob(id);
        } catch (NoSuchElementException e) {
            throw notFound(e.getMessage());
        }
    }

    private ResponseStatusException badRequest(String reason) { return new ResponseStatusException(HttpStatus.BAD_REQUEST, reason); }
    private ResponseStatusException notFound(String reason) { return new ResponseStatusException(HttpStatus.NOT_FOUND, reason); }
}
