package com.cachepulse.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GatewayPropertiesTest {
    private static final String HASH = new BCryptPasswordEncoder(4).encode("correct horse battery");

    @Test
    void defaultsUsernameTtlAndTrustedProxies() {
        GatewayProperties properties = new GatewayProperties(null, HASH, secret(32), null, null, null);

        assertThat(properties.adminUsername()).isEqualTo("admin");
        assertThat(properties.tokenTtl()).isEqualTo(Duration.ofMinutes(30));
        assertThat(properties.trustedProxies()).isEqualTo(1);
    }

    @Test
    void acceptsA32ByteSecret() {
        assertThat(new GatewayProperties("admin", HASH, secret(32), null, 0, null).jwtKey()).hasSize(32);
    }

    @Test
    void rejectsAShortOrMissingJwtSecret() {
        assertThatThrownBy(() -> new GatewayProperties("admin", HASH, secret(31), null, 0, null).jwtKey())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("at least 32 bytes");
        assertThatThrownBy(() -> new GatewayProperties("admin", HASH, " ", null, 0, null).jwtKey())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("required");
        assertThatThrownBy(() -> new GatewayProperties("admin", HASH, null, null, 0, null).jwtKey())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("required");
    }

    @Test
    void acceptsABcryptHash() {
        assertThat(new GatewayProperties("admin", HASH, secret(32), null, 0, null).requiredPasswordHash()).isEqualTo(HASH);
    }

    @Test
    void rejectsAPlainPasswordOrMissingHash() {
        for (String hash : new String[]{"correct horse battery", "{noop}secret", "", null}) {
            assertThatThrownBy(() -> new GatewayProperties("admin", hash, secret(32), null, 0, null).requiredPasswordHash())
                    .as(String.valueOf(hash))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("BCrypt");
        }
    }

    private static String secret(int bytes) {
        return Base64.getEncoder().encodeToString(new byte[bytes]);
    }
}
