package com.cachepulse.common.kafka;

import java.util.concurrent.TimeUnit;

/** Adds time between acknowledged batches when background writes have an explicit rate target. */
public class BackgroundWritePacer {
    private final int rowsPerSecond;

    public BackgroundWritePacer() {
        this(Integer.parseInt(System.getProperty("cachepulse.background-write-rows-per-second", "0")));
    }

    BackgroundWritePacer(int rowsPerSecond) {
        if (rowsPerSecond < 0) {
            throw new IllegalArgumentException("Background write rate must be zero or positive");
        }
        this.rowsPerSecond = rowsPerSecond;
    }

    public void awaitAfterBatch(long rowsWritten, long batchStarted) throws InterruptedException {
        long remaining = delayNanos(rowsWritten, System.nanoTime() - batchStarted);
        if (remaining > 0) {
            TimeUnit.NANOSECONDS.sleep(remaining);
        }
    }

    long delayNanos(long rowsWritten, long elapsed) {
        if (rowsPerSecond == 0) {
            return 0;
        }
        long interval = Math.min(TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(rowsWritten) / rowsPerSecond);
        return Math.max(0, interval - elapsed);
    }
}
