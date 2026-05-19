package com.weather.central.bitcask;

import java.util.concurrent.ConcurrentHashMap;

public class KeyDir {
    // in-memory hash index
    private final java.util.Map<String, KeyEntry> keyMap = new ConcurrentHashMap<>();

    public KeyDir() {
    }

    public void putEntry(String key, KeyEntry entry) {
        keyMap.put(key, entry);
    }

    public KeyEntry getEntry(String key) {
        return keyMap.get(key);
    }

    public void removeEntry(String key) {
        // Atomically remove the entry to prevent race conditions with background compaction
        keyMap.remove(key);
    }

    public void updateEntry(String key, String fileId, long valueOffset, int valueSize, long timestamp) {
        keyMap.compute(key, (k, existing) -> {
            if (existing == null || timestamp >= existing.getTimestamp()) {
                return new KeyEntry(fileId, valueOffset, valueSize, timestamp);
            }
            return existing; // If existing is newer, reject the update and keep it as-is
        });
    }

    public boolean isLiveEntry(String key, long timestamp) {
        KeyEntry existing = keyMap.get(key);
        return existing != null && existing.getTimestamp() == timestamp;
    }

}
