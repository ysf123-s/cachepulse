package com.cachepulse.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Base64;

/**
 * 管理员账号和 JWT 密钥。都从环境变量读，不放进 Nacos、不写进仓库。
 *
 * @param adminUsername     唯一的管理员账号
 * @param adminPasswordHash 管理员密码的 BCrypt 哈希（不存明文）
 * @param jwtSecret         HS256 密钥，Base64，原文至少 32 字节
 * @param tokenTtl          令牌有效期，默认 30 分钟
 * @param trustedProxies    X-Forwarded-For 里信任的代理层数，用于按客户端 IP 限流；直接对外暴露时设为 0
 */
@ConfigurationProperties("cachepulse.gateway")
public record GatewayProperties(String adminUsername, String adminPasswordHash, String jwtSecret,
                                Duration tokenTtl, Integer trustedProxies) {

    public GatewayProperties {
        if (adminUsername == null || adminUsername.isBlank()) {
            adminUsername = "admin";
        }
        if (tokenTtl == null) {
            tokenTtl = Duration.ofMinutes(30);
        }
        if (trustedProxies == null) {
            trustedProxies = 1;
        }
    }

    /** 启动时校验：缺密钥或哈希时直接失败，不带着空密码上线。 */
    public byte[] jwtKey() {
        if (jwtSecret == null || jwtSecret.isBlank()) {
            throw new IllegalStateException("cachepulse.gateway.jwt-secret (GATEWAY_JWT_SECRET) is required");
        }
        byte[] key = Base64.getDecoder().decode(jwtSecret.trim());
        if (key.length < 32) {
            throw new IllegalStateException("cachepulse.gateway.jwt-secret must decode to at least 32 bytes");
        }
        return key;
    }

    public String requiredPasswordHash() {
        if (adminPasswordHash == null || !adminPasswordHash.startsWith("$2")) {
            throw new IllegalStateException("cachepulse.gateway.admin-password-hash (GATEWAY_ADMIN_PASSWORD_HASH) must be a BCrypt hash");
        }
        return adminPasswordHash;
    }
}
