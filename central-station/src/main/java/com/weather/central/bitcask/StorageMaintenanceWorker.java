package com.weather.central.bitcask;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class StorageMaintenanceWorker {

    private final BitcaskService bitcaskService;

    public StorageMaintenanceWorker(BitcaskService bitcaskService) {
        this.bitcaskService = bitcaskService;
    }

    // Runs automatically every 30 seconds to clean up deleted/stale records
    @Scheduled(fixedRate = 30000)
    public void executeMaintenance() {
        bitcaskService.triggerCompaction();
    }
}