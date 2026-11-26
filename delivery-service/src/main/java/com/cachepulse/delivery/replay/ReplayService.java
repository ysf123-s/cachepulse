package com.cachepulse.delivery.replay;

import com.cachepulse.common.event.EventIdentity;
import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.delivery.DeliveryStore;
import com.cachepulse.delivery.DeliveryTask;
import com.cachepulse.delivery.EventSubscription;
import com.cachepulse.delivery.SubscriptionMatcher;
import com.cachepulse.framework.RemoteCallException;

import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import java.time.Instant;
import java.util.*;

public class ReplayService {
    private final PlatformProperties properties;
    private final DeliveryStore store;
    private final ArchiveClient archive;
    public ReplayService(PlatformProperties properties, DeliveryStore store, ArchiveClient archive) {
        this.properties = properties; this.store = store; this.archive = archive;
    }

    public ReplayJob createReplayJob(ReplayJob.Request req) {
        if (req == null || req.requestKey() == null || req.requestKey().isBlank() || req.requestKey().length() > 128) {
            throw new IllegalArgumentException("requestKey must contain 1-128 characters");
        }
        if (req.subscriptionId() == null || req.subscriptionId().isBlank()) throw new IllegalArgumentException("subscriptionId is required");
        if (req.eventIds() == null || req.eventIds().isEmpty() || req.eventIds().size() > 100) {
            throw new IllegalArgumentException("Replay is limited to at most 100 events per job; at least one is required");
        }
        if (req.eventIds().stream().anyMatch(id -> id == null || !id.matches("[a-f0-9]{64}"))) {
            throw new IllegalArgumentException("Every eventId must be a lowercase SHA-256 value");
        }
        List<String> ids = req.eventIds().stream().distinct().sorted().toList();
        ReplayJob existing = store.findReplayJobByRequestKey(req.requestKey());
        if (existing != null) {
            return reuse(req, ids, existing);
        }
        EventSubscription sub = store.findSubscription(req.subscriptionId());
        if (sub == null) throw new NoSuchElementException("Subscription not found");
        if (!sub.enabled()) throw new IllegalArgumentException("Cannot replay against a disabled subscription");
        PlatformProperties.TargetConfig target = properties.getTargets().get(sub.targetName());
        if (target == null) throw new IllegalArgumentException("Target alias is no longer registered: " + sub.targetName());
        String jobId = EventIdentity.hash("replay:" + UUID.randomUUID());
        ReplayJob job = new ReplayJob(jobId, req.requestKey(), properties.getSourceId(), properties.getDatabase(), sub.table(),
                sub.subscriptionId(), "PENDING", ids.size(), 0, 0, 0, ids, store.encode(sub), target.getUrl(), null,
                Instant.now().toString(), Instant.now().toString());
        if (!store.saveReplayJob(job)) return reuse(req, ids, store.findReplayJobByRequestKey(req.requestKey()));
        // 只保存任务，由 resumePending 在后台取事件、生成投递任务：一个任务最多向归档服务取 100 条事件，
        // 归档服务慢的时候要好几分钟，放在请求里做会让网关先超时（设计文档 14.1 第 21 条）。
        return store.findReplayJob(jobId);
    }

    private ReplayJob reuse(ReplayJob.Request req, List<String> ids, ReplayJob existing) {
        if (!existing.subscriptionId().equals(req.subscriptionId()) || !existing.eventIds().equals(ids)) {
            throw new IllegalStateException("requestKey is already used by a different replay request");
        }
        return existing;
    }

    @Scheduled(fixedDelayString = "${platform.replay-delay-ms:500}")
    public void resumePending() { for (String id : store.pendingReplayIds()) resume(id); }

    private void resume(String id) {
        try { processJob(id); }
        catch (RemoteCallException e) {
            // 归档服务明确拒绝（401、403、400 等）是配置或请求的问题，重试也一样，任务直接失败；
            // 超时、5xx、熔断、限流（429）稍后再试
            store.recordReplayFailure(id, "archive-service: " + e.getMessage(), permanent(e));
        }
        catch (DataAccessException e) { store.recordReplayFailure(id, e.getClass().getSimpleName() + ": " + e.getMessage(), false); }
        catch (IllegalStateException | IllegalArgumentException e) { store.recordReplayFailure(id, e.getMessage(), true); }
    }

    static boolean permanent(RemoteCallException e) {
        return e.clientError() && e.remoteStatus() != 408 && e.remoteStatus() != 429;
    }

    /**
     * Bounded to 100 rows: the row lock, tasks and final counts commit together, or all roll back.
     * 历史事件先在事务外从归档服务取齐，不拿着行锁等远程调用；归档服务不可用时整个任务稍后重试。
     */
    public ReplayJob processJob(String id) {
        ReplayJob pending = getJob(id);
        if (!Set.of("PENDING", "RUNNING").contains(pending.status())) return pending;
        Map<String, BinlogEvent> events = new HashMap<>();
        for (String eventId : pending.eventIds()) {
            archive.event(eventId).ifPresent(event -> events.put(eventId, event));
        }
        return store.withReplayLock(id, job -> {
            if (!Set.of("PENDING", "RUNNING").contains(job.status())) return job;
            if (job.targetUrl() == null) throw new IllegalStateException("Legacy replay has no frozen target; create a new request");
            EventSubscription sub = store.decode(job.subscriptionSnapshot(), EventSubscription.class);
            int matched = 0, skipped = 0;
            for (String eventId : job.eventIds()) {
                BinlogEvent event = events.get(eventId);
                if (event == null || !job.sourceId().equals(properties.getSourceId())
                        || !job.database().equals(properties.getDatabase())
                        || !job.database().equals(event.getDatabaseName()) || !SubscriptionMatcher.matches(event, sub)) {
                    skipped++;
                    continue;
                }
                String deliveryId = EventIdentity.hash("delivery:" + eventId + ":" + sub.subscriptionId() + ":" + job.jobId());
                store.createDeliveryTask(new DeliveryTask(0, deliveryId, eventId, sub.subscriptionId(), "REPLAY", job.jobId(),
                        sub.targetName(), job.targetUrl(), "PENDING", 0, properties.getMaxAttempts(), null, null,
                        Instant.now().toString(), null, store.encode(event), job.subscriptionSnapshot(),
                        Instant.now().toString(), Instant.now().toString()));
                matched++;
            }
            store.updateReplayJobProgress(id, "COMPLETED", matched, skipped, 0);
            return store.findReplayJob(id);
        });
    }

    public ReplayJob getJob(String id) {
        ReplayJob job = store.findReplayJob(id);
        if (job == null) throw new NoSuchElementException("Replay job not found: " + id);
        return job;
    }
}
