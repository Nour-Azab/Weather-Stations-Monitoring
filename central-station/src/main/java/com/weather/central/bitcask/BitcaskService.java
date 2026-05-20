package com.weather.central.bitcask;

import org.springframework.stereotype.Service;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

@Service
public class BitcaskService {

    private final BitcaskStore store;
    private final Compactor compactor;

    public BitcaskService() throws IOException {
        this.store = new BitcaskStore();
        this.compactor = new Compactor(this.store);
    }

    public void put(String key, String value) throws IOException {
        store.put(key, value);
    }

    public String get(String key) throws IOException {
        return store.get(key);
    }

    public void delete(String key) throws IOException {
        store.delete(key);
    }

    public synchronized Map<String, String> getAllLiveEntries() {
        Map<String, String> plainDataMap = new HashMap<>();
        
        KeyDir keyDir = store.getKeyDir();
        // 1. Get all active, un-deleted keys from memory
        Set<String> keys = keyDir.getAllKeys();
        
        // 2. Resolve their internal coordinates into actual values
        for (String key : keys) {
            try {
                String value = this.get(key); 
                if (value != null) {
                    plainDataMap.put(key, value);
                }
            } catch (IOException e) {
                System.err.println("[SERVICE ERROR] Failed reading key '" + key + "' from disk during map compile: " + e.getMessage());
            }
        }
        
        return plainDataMap;
    }

    public void triggerCompaction() {
        try {
            System.out.println("[COMPACTOR] Starting asynchronous storage compaction sweep...");
            compactor.compact();
            System.out.println("[COMPACTOR] Compaction sweep successfully finished!");
        } catch (IOException e) {
            System.err.println("[COMPACTOR ERROR] Aggressive compaction failure: " + e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        System.out.println("Shutting down Bitcask engine safely...");
        // Close the active segment handle gracefully on system shutdown
        try {
            if (store.getSegments() != null) {
                for (Segment seg : store.getSegments().values()) {
                    seg.close();
                }
            }
        } catch (IOException e) {
            System.err.println("Error closing segments during shutdown: " + e.getMessage());
        }
    }
}