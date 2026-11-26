package com.cachepulse.common.kafka;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackgroundWritePacerTest {
    @Test
    void deductsProcessingTimeAndCapsTheInterval() {
        BackgroundWritePacer pacer = new BackgroundWritePacer(650);
        long spent = Duration.ofMillis(20).toNanos();

        assertThat(pacer.delayNanos(65, spent)).isEqualTo(Duration.ofMillis(80).toNanos());
        assertThat(pacer.delayNanos(1, spent)).isZero();
        assertThat(pacer.delayNanos(6500, spent)).isEqualTo(Duration.ofMillis(980).toNanos());
        assertThat(new BackgroundWritePacer(0).delayNanos(6500, spent)).isZero();
    }
}
