package com.cachepulse.delivery.replay;

import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.framework.RemoteCallException;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.Optional;

/**
 * 平台内唯一的服务间同步调用：重放时向归档服务取历史事件（设计文档第 9.3 节）。
 * 404 表示归档里没有这条事件，返回 Optional.empty()，不算调用失败、不计入熔断；
 * 超时、5xx、熔断打开时走降级，抛出"远程不可用"，重放记为可重试失败。
 */
@FeignClient(name = "archive-service", contextId = "archiveEvents", dismiss404 = true,
        fallbackFactory = ArchiveClient.Fallback.class)
public interface ArchiveClient {

    @GetMapping("/internal/events/{eventId}")
    Optional<BinlogEvent> event(@PathVariable("eventId") String eventId);

    /** 降级不返回伪造数据，只把失败原因转成明确的远程调用异常。 */
    class Fallback implements FallbackFactory<ArchiveClient> {
        @Override
        public ArchiveClient create(Throwable cause) {
            return eventId -> {
                if (cause instanceof RemoteCallException remote) {
                    throw remote;
                }
                throw RemoteCallException.unavailable("archive-service", cause);
            };
        }
    }
}
