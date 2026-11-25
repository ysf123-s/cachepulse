package com.cachepulse.sync.controller;

import com.cachepulse.sync.CanalCacheSyncService;
import com.cachepulse.sync.SyncStatus;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sync")
public class SyncStatusController {
    private final CanalCacheSyncService syncService;

    public SyncStatusController(CanalCacheSyncService syncService) {
        this.syncService = syncService;
    }

    @GetMapping("/status")
    public SyncStatus getStatus() {
        return syncService.getStatus();
    }
}
