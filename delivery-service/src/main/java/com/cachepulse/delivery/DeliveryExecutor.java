package com.cachepulse.delivery;

import com.cachepulse.common.platform.PlatformProperties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DeliveryExecutor {
    private static final Logger log = LoggerFactory.getLogger(DeliveryExecutor.class);
    /** 接收端的响应只存前 500 个字符做摘要，多读一些留给多字节字符，其余不读。 */
    static final int RESPONSE_BODY_LIMIT = 4096;

    private final PlatformProperties properties;
    private final DeliveryStore store;
    private final HttpClient httpClient;
    private final ObjectMapper json;
    private final String instanceId;

    public DeliveryExecutor(PlatformProperties properties, DeliveryStore store,
                            HttpClient httpClient, ObjectMapper json) {
        this.properties = properties;
        this.store = store;
        this.httpClient = httpClient != null ? httpClient : HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        this.json = json;
        this.instanceId = "exec-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Poll and process one batch of claimed delivery tasks.
     * Returns the number of tasks processed.
     */
    @Scheduled(fixedDelayString = "${platform.dispatch-delay-ms:500}")
    public void dispatchPending() { dispatch(10); }

    public int dispatch(int batchSize) {
        int processed = 0;
        while (processed < batchSize && !Thread.currentThread().isInterrupted()) {
            List<DeliveryTask> tasks = store.claimTasks(instanceId, 1, properties.getLeaseSeconds());
            if (tasks.isEmpty()) break;
            executeTask(tasks.get(0));
            processed++;
        }
        return processed;
    }

    /**
     * Append one attempt to a failed task, retaining its delivery ID and attempt history.
     */
    public DeliveryTask manualRetry(long taskId) {
        DeliveryTask task = store.findDeliveryTask(taskId);
        if (task == null) {
            throw new NoSuchElementException("Delivery task not found: " + taskId);
        }
        if (!"FAILED".equalsIgnoreCase(task.status())) {
            throw new IllegalStateException("Task is in '" + task.status() + "' state; only FAILED tasks can be manually retried");
        }

        boolean reset = store.resetTaskForRetry(taskId);
        if (!reset) {
            throw new IllegalStateException("Failed to reset task; status may have changed concurrently");
        }
        return store.findDeliveryTask(taskId);
    }

    private void executeTask(DeliveryTask task) {
        int attemptNum = task.attemptCount() + 1;
        long startNanos = System.nanoTime();

        int timeoutMs = 5000;
        PlatformProperties.TargetConfig targetConfig = properties.getTargets().get(task.targetName());
        if (targetConfig != null && targetConfig.getTimeoutMs() > 0) {
            timeoutMs = targetConfig.getTimeoutMs();
        }

        HttpResponse<String> response = null;
        Exception error = null;
        CompletableFuture<HttpResponse<String>> pending = null;

        try {
            String payload = buildHttpPayload(task, attemptNum);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(task.targetUrl()))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Content-Type", "application/json; charset=UTF-8")
                    .header("X-Delivery-Id", task.deliveryId())
                    .header("X-Event-Id", task.eventId())
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            // HttpRequest.timeout 只管到收到响应头；接收端发完头再慢慢发内容时会一直等。
            // 整次发送（连接、等响应、读内容）共用一个期限，内容只读前 RESPONSE_BODY_LIMIT 字节（设计文档 14.1 第 20 条）。
            pending = httpClient.sendAsync(request, responseBody(RESPONSE_BODY_LIMIT));
            response = pending.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            if (pending != null) pending.cancel(true);
            Thread.currentThread().interrupt();
            return; // Keep the lease for restart recovery; no fabricated HTTP result.
        } catch (TimeoutException e) {
            pending.cancel(true);
            error = new HttpTimeoutException("No complete response within " + timeoutMs + " ms");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            error = cause instanceof Exception exception ? exception : new IOException(cause);
        } catch (RuntimeException e) {
            // 存的事件内容坏了、地址不合法等：记一次失败，不再让任务一直卡在 IN_FLIGHT、每个租约周期重来一遍
            error = e;
        }

        long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;
        if (!handleExecutionResult(task, attemptNum, durationMs, response, error)) {
            log.warn("Delivery result for task {} was discarded: its lease expired or was taken over after {} ms",
                    task.taskId(), durationMs);
        }
    }

    /** 只留前 limit 字节，够了就停止读取并关闭连接；结果按 UTF-8 解码，截断处可能多一个替换字符。 */
    static HttpResponse.BodyHandler<String> responseBody(int limit) {
        return info -> new LimitedBodySubscriber(limit);
    }

    private static final class LimitedBodySubscriber implements HttpResponse.BodySubscriber<String> {
        private final int limit;
        private final ByteArrayOutputStream received = new ByteArrayOutputStream();
        private final CompletableFuture<String> body = new CompletableFuture<>();
        private Flow.Subscription subscription;

        LimitedBodySubscriber(int limit) {
            this.limit = limit;
        }

        @Override
        public CompletionStage<String> getBody() {
            return body;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            if (body.isDone()) {
                return;
            }
            for (ByteBuffer buffer : buffers) {
                int take = Math.min(buffer.remaining(), limit - received.size());
                byte[] bytes = new byte[take];
                buffer.get(bytes);
                received.write(bytes, 0, take);
                if (received.size() >= limit) {
                    subscription.cancel();
                    complete();
                    return;
                }
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            complete();
        }

        private void complete() {
            body.complete(received.toString(StandardCharsets.UTF_8));
        }
    }

    private boolean handleExecutionResult(DeliveryTask task, int attemptNum, long durationMs,
                                          HttpResponse<String> response, Exception error) {
        Integer statusCode = response != null ? response.statusCode() : null;
        String responseSummary = response != null && response.body() != null
                ? truncate(response.body().trim(), 500) : null;
        String errorSummary = error != null ? truncate(error.getClass().getSimpleName() + ": " + error.getMessage(), 500) : null;

        if (statusCode != null && statusCode >= 200 && statusCode < 300) {
            // Success (2xx)
            DeliveryAttempt attempt = new DeliveryAttempt(
                    0L, task.taskId(), attemptNum, "SUCCEEDED", statusCode, durationMs,
                    null, responseSummary, null, Instant.now().toString()
            );
            return store.finishDelivery(task, attempt);
        }

        // Failure handling
        boolean isRetryable = false;
        String failureReason;

        if (error != null) {
            isRetryable = (error instanceof IOException || error instanceof HttpTimeoutException);
            failureReason = errorSummary;
        } else if (statusCode != null) {
            if (statusCode == 429 || statusCode >= 500) {
                isRetryable = true;
                failureReason = "HTTP " + statusCode + (responseSummary != null ? " - " + responseSummary : "");
            } else if (statusCode >= 300 && statusCode < 400) {
                isRetryable = false;
                failureReason = "HTTP " + statusCode + " Redirection not followed";
            } else {
                // Other 4xx
                isRetryable = false;
                failureReason = "HTTP " + statusCode + " Client Error" + (responseSummary != null ? " - " + responseSummary : "");
            }
        } else {
            failureReason = "Unknown error";
        }

        if (isRetryable && attemptNum < task.maxAttempts()) {
            // Schedule next retry with exponential backoff
            long backoffSeconds = Math.min(300, properties.getBackoffInitialSeconds() * (1L << Math.min(attemptNum - 1, 9)));
            Instant nextRetry = Instant.now().plusSeconds(backoffSeconds);

            DeliveryAttempt attempt = new DeliveryAttempt(
                    0L, task.taskId(), attemptNum, "RETRY_WAIT", statusCode, durationMs,
                    failureReason, responseSummary, nextRetry.toString(), Instant.now().toString()
            );
            return store.finishDelivery(task, attempt);
        } else {
            // Final failure
            DeliveryAttempt attempt = new DeliveryAttempt(
                    0L, task.taskId(), attemptNum, "FAILED", statusCode, durationMs,
                    failureReason, responseSummary, null, Instant.now().toString()
            );
            return store.finishDelivery(task, attempt);
        }
    }

    private String buildHttpPayload(DeliveryTask task, int attemptNum) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("deliveryId", task.deliveryId());
        map.put("eventId", task.eventId());
        map.put("subscriptionId", task.subscriptionId());
        map.put("targetName", task.targetName());
        map.put("attempt", attemptNum);
        map.put("timestamp", System.currentTimeMillis());

        try {
            map.put("event", json.readValue(task.eventPayload(), Object.class));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Stored delivery payload is invalid", e);
        }

        try {
            return json.writeValueAsString(map);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Cannot encode delivery payload", e);
        }
    }

    private static String truncate(String text, int maxLength) {
        if (text == null) return null;
        return text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
