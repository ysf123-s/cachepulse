package com.example.canalcache.service;

import com.example.canalcache.service.CacheLoadGuard.Lane;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisCircuitBreakerTest {
    private final AtomicLong now = new AtomicLong(1_000);
    private final RedisCircuitBreaker breaker = new RedisCircuitBreaker(3, 5_000, now::get);

    @Test
    void opensOnlyAfterConsecutiveFailures() {
        breaker.recordFailure(failure());
        breaker.recordFailure(failure());
        breaker.recordSuccess();
        breaker.recordFailure(failure());
        breaker.recordFailure(failure());
        assertThat(breaker.allowRequest()).isTrue();

        breaker.recordFailure(failure());

        assertThat(breaker.allowRequest()).isFalse();
        assertThat(breaker.status().state()).isEqualTo(RedisCircuitBreaker.State.OPEN);
        assertThat(breaker.status().openRemainingMs()).isEqualTo(5_000);
        assertThat(breaker.status().lastError()).contains("redis offline");
    }

    @Test
    void letsExactlyOneProbeThroughPerOpenPeriodAndClosesWhenItSucceeds() {
        for (int i = 0; i < 3; i++) {
            breaker.recordFailure(failure());
        }
        now.addAndGet(5_000);

        assertThat(breaker.allowRequest()).isTrue();
        assertThat(breaker.allowRequest()).isFalse();

        now.addAndGet(5_000);
        assertThat(breaker.allowRequest()).isTrue();
        breaker.recordSuccess();

        assertThat(breaker.status().state()).isEqualTo(RedisCircuitBreaker.State.CLOSED);
        assertThat(breaker.allowRequest()).isTrue();
    }

    @Test
    void failedProbeReopensForAnotherPeriod() {
        for (int i = 0; i < 3; i++) {
            breaker.recordFailure(failure());
        }
        now.addAndGet(5_000);
        assertThat(breaker.allowRequest()).isTrue();

        breaker.recordFailure(failure());
        now.addAndGet(4_999);

        assertThat(breaker.allowRequest()).isFalse();
        assertThat(breaker.status().fallbacks()).isEqualTo(4);
    }

    @Test
    void loadGuardRejectsWhenFullAndReleasesPermits() {
        CacheLoadGuard guard = new CacheLoadGuard(1, 0, 1, 0);

        String nested = guard.run(Lane.NORMAL, () -> {
            assertThat(guard.inUse(Lane.NORMAL)).isEqualTo(1);
            assertThatThrownBy(() -> guard.run(Lane.NORMAL, () -> "second"))
                    .isInstanceOf(CacheLoadRejectedException.class);
            return guard.run(Lane.SUSPECT, () -> "other lane");
        });

        assertThat(nested).isEqualTo("other lane");
        assertThat(guard.inUse(Lane.NORMAL)).isZero();
        assertThat(guard.status().get(Lane.NORMAL).rejected()).isEqualTo(1);
        assertThat(guard.status().get(Lane.SUSPECT).rejected()).isZero();
        assertThatThrownBy(() -> new CacheLoadGuard(0, 0, 1, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    private static RedisConnectionFailureException failure() {
        return new RedisConnectionFailureException("redis offline");
    }
}
