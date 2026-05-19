package com.weather.central.bitcask;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.charset.StandardCharsets;

public class BitcaskStore {
    // Core engine (get/put)
    private final KeyDir keyDir = new KeyDir();
    private final Map<String, Segment> segments = new ConcurrentHashMap<>();
    private final String dataDir;
    private final long maxSegmentSize; // Maximum bytes before rotating files

    private volatile Segment activeSegment;
    private long currentFileSequence = 0;

    public BitcaskStore() throws IOException {
        this.dataDir = "Data";
        this.maxSegmentSize = 1024; // 64MB default segment size

        // Ensure the database directory exists
        File dir = new File(dataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        // Initialize our storage
        initStore();
    }

    public Map<String, Segment> getSegments() {
        return segments;
    }

    public String getDataDir() {
        return dataDir;
    }

    public KeyDir getKeyDir() {
        return keyDir;
    }

    public long getMaxSegmentSize() {
        return maxSegmentSize;
    }

    private void initStore() throws IOException {
        File dir = new File(dataDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".db"));

        if (files != null && files.length > 0) {
            // Find the highest file ID to resume sequence numbering correctly
            for (File f : files) {
                String name = f.getName();
                String idStr = name.substring(0, name.indexOf(".db"));
                try {
                    long id = Long.parseLong(idStr);
                    if (id > currentFileSequence) {
                        currentFileSequence = id;
                    }

                    
                } catch (NumberFormatException e) {
                    // Ignore files that don't match our numeric format
                }
                // Open it as an old read-only/immutable segment
                    Segment seg = new Segment(idStr, f.getAbsolutePath());
                    segments.put(idStr, seg);
                    File HintFile = new File(dataDir + File.separator + idStr + ".hint");

                    if (HintFile.exists()) {
                        HintFile hintFile = new HintFile(HintFile.getAbsolutePath());
                        hintFile.loadIntoKeyDir(idStr, this.keyDir);
                    } else {
                        loadOldSegment(f, idStr);
                    }
            }
        }

        // open a brand new, clean active segment on startup to append writes
        createNewActiveSegment();
    }

    private synchronized void createNewActiveSegment() throws IOException {
        currentFileSequence++;
        String newId = String.valueOf(currentFileSequence);
        String filePath = dataDir + File.separator + newId + ".db";

        // If an active segment already existed, it is now considered an old segment
        if (this.activeSegment != null) {
            segments.put(activeSegment.getFileId(), activeSegment);
        }

        this.activeSegment = new Segment(newId, filePath);
    }

    private void loadOldSegment(File dataFile, String fileId) throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(dataFile, "r")) {
            long fileLength = reader.length();
            long currentPos = 0;

            // format: [Timestamp][KeySize][ValueSize][Key][Value]
            while (currentPos < fileLength) {
                reader.seek(currentPos);

                long timestamp = reader.readLong();
                int keySize = reader.readInt();
                int valueSize = reader.readInt();

                byte[] keyBytes = new byte[keySize];
                reader.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                long valueOffset = reader.getFilePointer();

                if (valueSize == 0) {
                    // It's a tombstone, Remove it from memory index during recovery rebuild
                    this.keyDir.removeEntry(key);
                } else {
                    // It's a valid live write
                    this.keyDir.updateEntry(key, fileId, valueOffset, valueSize, timestamp);
                }

                // Advance past the value bytes to point to the next record
                currentPos = valueOffset + valueSize;
            }
        }

    }

    public void replaceCompactedSegments(java.util.List<String> oldFileIds, String compactFileId,
            Segment compactSegment) {
        // 1. Add the new compacted segment to the segments map
        segments.put(compactFileId, compactSegment);

        // 2. Remove old segments from the segments map and delete their files
        for (String oldId : oldFileIds) {
            Segment oldSeg = segments.remove(oldId);
            if (oldSeg != null) {
                try {
                    oldSeg.close();
                } catch (IOException e) {
                    // Log or handle as needed
                }
                // File A: Clean up the main database log payload
                File oldDbFile = new File(dataDir + File.separator + oldId + ".db");
                if (oldDbFile.exists()) {
                    oldDbFile.delete();
                }

                // File B: Clean up the companion Hint File so it doesn't leak!
                File oldHintFile = new File(dataDir + File.separator + oldId + ".hint");
                if (oldHintFile.exists()) {
                    oldHintFile.delete();
                }
            }
        }
    }

    public synchronized void put(String key, String value) throws IOException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and Value cannot be null");
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        long timestamp = System.currentTimeMillis();

        // 1. Write data to the active log file
        Segment.WriteResult result = activeSegment.append(keyBytes, valueBytes, timestamp);

        // 2. Update the fast in-memory map
        this.keyDir.updateEntry(key, activeSegment.getFileId(), result.valueOffset, result.valueSize, result.timestamp);

        // 3. Check if the file is full. If it is, swap to a brand new clean file.
        if (activeSegment.getFileSize() >= maxSegmentSize) {
            createNewActiveSegment();
        }
    }

    public String get(String key) throws IOException {
        if (key == null) {
            return null;
        }

        // 1. Check the in-memory index
        KeyEntry entry = this.keyDir.getEntry(key);
        if (entry == null) {
            return null; // Key does not exist or was deleted
        }

        // 2. Identify which file contains the value
        Segment targetSegment;
        if (entry.getFileId().equals(activeSegment.getFileId())) {
            targetSegment = activeSegment;
        } else {
            targetSegment = segments.get(entry.getFileId());
        }

        // 3. Read the data directly from that file position
        if (targetSegment != null) {
            return targetSegment.readValue(entry.getValuePos(), entry.getValueSize());
        }

        return null;
    }

    public synchronized void delete(String key) throws IOException {
        if (key == null)
            return;

        // Check if the key even exists before wastefully writing a tombstone
        if (this.keyDir.getEntry(key) == null) {
            return;
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] tombstoneValue = new byte[0]; // 0-length byte array represents deletion

        long timestamp = System.currentTimeMillis();

        // Append the tombstone entry to disk
        activeSegment.append(keyBytes, tombstoneValue, timestamp);

        // Evict it entirely from our fast memory index so readers instantly get 'null'
        // Assuming your KeyDir has a removal or clear helper method
        this.keyDir.removeEntry(key);

        // Check size thresholds for rotation
        if (activeSegment.getFileSize() >= maxSegmentSize) {
            createNewActiveSegment();
        }
    }
}
