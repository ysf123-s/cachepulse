package com.cachepulse.delivery;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.cachepulse.delivery.DeliveryTestFixture.inserted;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 一个接收端不能拖住所有投递（设计文档 14.1 第 20 条）：整次发送共用目标的超时，响应内容只读前几 KB；
 * 投递前就出错的任务记一次失败，不会一直卡在 IN_FLIGHT。用本机真实的 HTTP 服务。
 */
class DeliveryExecutorLimitsTest {
    private final AtomicBoolean stop = new AtomicBoolean();
    private DeliveryTestFixture f;
    private HttpServer server;
    private ExecutorService serverThreads;

    @BeforeEach
    void setUp() throws Exception {
        f = new DeliveryTestFixture();
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        // 先回 200 和响应头，再每 200 ms 发 1 个字节，永远不结束
        server.createContext("/slow", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream out = exchange.getResponseBody()) {
                while (!stop.get()) {
                    out.write('x');
                    out.flush();
                    Thread.sleep(200);
                }
            } catch (IOException | InterruptedException ignored) {
                // 客户端放弃后连接关闭
            }
        });
        // 回 64 MB
        server.createContext("/huge", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            byte[] chunk = new byte[64 * 1024];
            Arrays.fill(chunk, (byte) 'y');
            try (OutputStream out = exchange.getResponseBody()) {
                for (int i = 0; i < 1024 && !stop.get(); i++) {
                    out.write(chunk);
                }
            } catch (IOException ignored) {
                // 客户端读够后关闭连接
            }
        });
        serverThreads = Executors.newCachedThreadPool();
        server.setExecutor(serverThreads);
        server.start();
    }

    @AfterEach
    void tearDown() {
        stop.set(true);
        server.stop(0);
        serverThreads.shutdownNow();
    }

    @Test
    @Timeout(10)
    void bodyThatNeverEndsIsCutOffAtTheTargetTimeout() {
        DeliveryTask task = task("/slow", 800);
        DeliveryExecutor executor = new DeliveryExecutor(f.properties, f.store, null, f.json);

        long started = System.nanoTime();
        assertThat(executor.dispatch(1)).isEqualTo(1);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;

        assertThat(elapsedMs).isBetween(700L, 3000L);
        DeliveryTask after = f.store.findDeliveryTask(task.taskId());
        assertThat(after.status()).isEqualTo("RETRY_WAIT");
        assertThat(after.attemptCount()).isEqualTo(1);
        assertThat(after.lastError()).contains("HttpTimeoutException");
    }

    @Test
    @Timeout(10)
    void hugeBodyIsReadOnlyUpToTheLimit() {
        DeliveryTask task = task("/huge", 3000);
        DeliveryExecutor executor = new DeliveryExecutor(f.properties, f.store, null, f.json);

        assertThat(executor.dispatch(1)).isEqualTo(1);

        DeliveryTask after = f.store.findDeliveryTask(task.taskId());
        assertThat(after.status()).isEqualTo("SUCCEEDED");
        assertThat(f.store.listAttempts(task.taskId())).singleElement()
                .satisfies(attempt -> assertThat(attempt.responseSummary()).hasSize(500).matches("y+"));
    }

    @Test
    void corruptStoredPayloadFailsTheTaskInsteadOfLeavingItInFlight() {
        DeliveryTask task = task("/huge", 1000);
        f.jdbc.update("UPDATE delivery_task SET event_payload='not json' WHERE task_id=?", task.taskId());
        DeliveryExecutor executor = new DeliveryExecutor(f.properties, f.store, null, f.json);

        assertThat(executor.dispatch(1)).isEqualTo(1);

        DeliveryTask after = f.store.findDeliveryTask(task.taskId());
        assertThat(after.status()).isEqualTo("FAILED");
        assertThat(after.attemptCount()).isEqualTo(1);
        assertThat(after.leaseOwner()).isNull();
        assertThat(after.lastError()).contains("Stored delivery payload is invalid");
    }

    private DeliveryTask task(String path, int timeoutMs) {
        f.target("hook", "http://127.0.0.1:" + server.getAddress().getPort() + path);
        f.properties.getTargets().get("hook").setTimeoutMs(timeoutMs);
        f.subscribe("sub", "hook", "INSERT", true);
        f.publish(f.record(1, f.rows(CanalEntry.EventType.INSERT, inserted("1", "A"))));
        return f.store.listDeliveryTasks(null, "sub", null, 1).get(0);
    }
}
