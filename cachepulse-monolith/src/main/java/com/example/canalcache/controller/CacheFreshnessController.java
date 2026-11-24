package com.example.canalcache.controller;

import com.example.canalcache.service.CdcHeartbeatMonitor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** CDC 心跳状态：链路是否追上、各分区心跳延迟、查询是否正在绕过缓存。 */
@RestController
@RequestMapping("/cache")
public class CacheFreshnessController {
    private final CdcHeartbeatMonitor heartbeat;

    public CacheFreshnessController(CdcHeartbeatMonitor heartbeat) {
        this.heartbeat = heartbeat;
    }

    @GetMapping("/freshness")
    public CdcHeartbeatMonitor.Status freshness() {
        return heartbeat.status();
    }
}
