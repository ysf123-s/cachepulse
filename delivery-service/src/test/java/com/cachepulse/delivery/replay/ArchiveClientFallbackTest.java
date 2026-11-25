package com.cachepulse.delivery.replay;

import com.cachepulse.framework.RemoteCallException;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.net.ConnectException;

import static org.assertj.core.api.Assertions.*;

/** The fallback never invents data: it only turns the failure into a RemoteCallException (design 9.3). */
class ArchiveClientFallbackTest {
    private final ArchiveClient.Fallback fallback = new ArchiveClient.Fallback();

    @Test
    void remoteCallExceptionFromTheErrorDecoderIsRethrownUnchanged() {
        RemoteCallException decoded = new RemoteCallException("archive-service", 500, "INTERNAL", "boom", "trace-1", null);
        assertThatThrownBy(() -> fallback.create(decoded).event("a".repeat(64))).isSameAs(decoded);
    }

    @Test
    void otherFailuresAreWrappedAsRemoteUnavailable() {
        ConnectException cause = new ConnectException("Connection refused");
        assertThatThrownBy(() -> fallback.create(cause).event("a".repeat(64)))
                .isInstanceOfSatisfying(RemoteCallException.class, error -> {
                    assertThat(error.code()).isEqualTo("REMOTE_UNAVAILABLE");
                    assertThat(error.service()).isEqualTo("archive-service");
                    assertThat(error.status()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(error.clientError()).isFalse();
                    assertThat(error).hasCause(cause);
                });
    }
}
