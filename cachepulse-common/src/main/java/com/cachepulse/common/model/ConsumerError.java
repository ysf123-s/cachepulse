package com.cachepulse.common.model;

import java.time.Instant;

/** 消费者最近一次出错的信息，字段与缓存同步服务的 SyncStatus.ErrorInfo 一致，管理页面共用同一种展示。 */
public record ConsumerError(Instant occurredAt, String consumerThread, String type, String message) {
}
