package com.weather.central.bitcask;

import org.springframework.stereotype.Service;
import jakarta.annotation.PreDestroy;
import java.io.IOException;

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