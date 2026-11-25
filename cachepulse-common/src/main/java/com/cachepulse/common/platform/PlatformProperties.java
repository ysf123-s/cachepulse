package com.cachepulse.common.platform;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@ConfigurationProperties("platform")
public class PlatformProperties {
    /**
     * 原项目正在使用的消费组和平台库（设计文档第 8 节）。开发期间两边同时运行，CachePulse 误用它们会抢走原项目的分区、
     * 写进原项目的库，所以启动时直接拒绝。
     */
    public static final java.util.Set<String> LEGACY_GROUPS =
            java.util.Set.of("canal-cache-sync-mq", "platform-change-archive", "platform-change-archive-v2");
    public static final String LEGACY_PLATFORM_DATABASE = "cdc_platform";

    private String sourceId = "local-mysql-v1";
    private String database = "demo_db";
    private String binlogTimeZone = "Asia/Shanghai";
    private String groupId = "cachepulse-archive";
    private List<String> tables = new ArrayList<>(List.of("users", "departments"));
    private Map<String, TargetConfig> targets = new LinkedHashMap<>();
    private List<String> enabledExamples = new ArrayList<>();
    private int maxAttempts = 3;
    private int leaseSeconds = 30;
    private int backoffInitialSeconds = 5;

    @Data
    public static class TargetConfig {
        private String url;
        private int timeoutMs = 5000;
    }

    public void validate(boolean flatMessage, String cacheGroup) {
        java.time.ZoneId.of(binlogTimeZone);
        if (sourceId == null || !sourceId.matches("[A-Za-z0-9_-]{1,64}")) {
            throw new IllegalArgumentException("platform.source-id must contain 1-64 letters, digits, '_' or '-'");
        }
        requireIdentifier(database);
        if (database.equalsIgnoreCase(LEGACY_PLATFORM_DATABASE)
                || database.equalsIgnoreCase("cp_archive") || database.equalsIgnoreCase("cp_delivery")) {
            throw new IllegalArgumentException("Platform storage database cannot be captured");
        }
        if (flatMessage) {
            throw new IllegalArgumentException("Platform archiving requires canal.mq.kafka.flat-message=false");
        }
        if (groupId == null || groupId.isBlank() || groupId.equals(cacheGroup)) {
            throw new IllegalArgumentException("Platform consumer group must differ from cache group");
        }
        if (LEGACY_GROUPS.contains(groupId) || LEGACY_GROUPS.contains(cacheGroup)) {
            throw new IllegalArgumentException("Consumer group belongs to the original project: " + groupId + ", " + cacheGroup);
        }
        if (tables == null || tables.isEmpty() || tables.stream().distinct().count() != tables.size()) {
            throw new IllegalArgumentException("platform.tables must be non-empty and contain no duplicates");
        }
        tables.forEach(PlatformProperties::requireIdentifier);
        if (maxAttempts < 1 || maxAttempts > 10 || leaseSeconds < 10 || leaseSeconds > 300
                || backoffInitialSeconds < 1 || backoffInitialSeconds > 60) {
            throw new IllegalArgumentException("Invalid delivery limits: attempts 1-10, lease 10-300s, backoff 1-60s required");
        }
        if (targets != null) {
            targets.forEach((name, cfg) -> {
                if (name == null || !name.matches("[A-Za-z0-9_-]{1,64}")) {
                    throw new IllegalArgumentException("Invalid target name: " + name);
                }
                if (cfg == null || cfg.getUrl() == null || (!cfg.getUrl().startsWith("http://") && !cfg.getUrl().startsWith("https://"))) {
                    throw new IllegalArgumentException("Invalid target URL for target: " + name);
                }
                java.net.URI uri = java.net.URI.create(cfg.getUrl());
                if (uri.getHost() == null || uri.getUserInfo() != null || cfg.getUrl().length() > 512
                        || cfg.getTimeoutMs() < 100 || cfg.getTimeoutMs() > 20000
                        || cfg.getTimeoutMs() + 2000L >= leaseSeconds * 1000L) {
                    throw new IllegalArgumentException("Target requires a valid host, no credentials, and timeout below its lease: " + name);
                }
            });
        }
    }

    private static void requireIdentifier(String name) {
        if (name == null || !name.matches("[A-Za-z_][A-Za-z0-9_$]{0,63}")) {
            throw new IllegalArgumentException("Invalid database/table identifier: " + name);
        }
    }
}
