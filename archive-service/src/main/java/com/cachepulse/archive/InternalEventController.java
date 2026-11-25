package com.cachepulse.archive;

import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.platform.PlatformProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.NoSuchElementException;

/** 只给服务间调用：投递服务重放时按事件 ID 取历史事件。网关不转发 /internal/**，还要校验内部令牌。 */
@RestController
public class InternalEventController {
    private final ArchiveStore store;
    private final PlatformProperties properties;

    public InternalEventController(ArchiveStore store, PlatformProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    @GetMapping("/internal/events/{eventId}")
    public BinlogEvent event(@PathVariable String eventId) {
        if (!eventId.matches("[a-f0-9]{64}")) {
            throw new IllegalArgumentException("eventId must be a lowercase SHA-256 value");
        }
        BinlogEvent event = store.findEvent(eventId, properties);
        if (event == null) {
            throw new NoSuchElementException("Archived event not found: " + eventId);
        }
        return event;
    }
}
