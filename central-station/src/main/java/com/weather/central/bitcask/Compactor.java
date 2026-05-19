package com.weather.central.bitcask;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Compactor {
    
    private final BitcaskStore store;

    public Compactor(BitcaskStore store) {
        this.store = store;
    }

    public synchronized void compact() throws IOException {
        // Get a snapshot of all old segments currently eligible for compaction
        List<Map.Entry<String, Segment>> segmentsToCompact = new ArrayList<>(store.getSegments().entrySet());
        if (segmentsToCompact.isEmpty()) {
            return;
        }

        // Create a unique temporary merge segment file
        String compactFileId = "compact_" + System.currentTimeMillis();
        String compactFilePath = store.getDataDir() + File.separator + compactFileId + ".db";
        Segment compactSegment = new Segment(compactFileId, compactFilePath);

        // Initialize the companion HintFile manager
        String hintFilePath = store.getDataDir() + File.separator + compactFileId + ".hint";
        HintFile hintFile = new HintFile(hintFilePath);
        hintFile.openForWriting();

        List<String> filesCompacted = new ArrayList<>();

        // Scan through every old segment file sequentially
        for (Map.Entry<String, Segment> entry : segmentsToCompact) {
            String fileId = entry.getKey();
            File fileOnDisk = new File(store.getDataDir() + File.separator + fileId + ".db");
            
            if (!fileOnDisk.exists()) continue;

            try (RandomAccessFile reader = new RandomAccessFile(fileOnDisk, "r")) {
                long fileLength = reader.length();
                long currentPos = 0;

                // Parse the binary file format: [Timestamp][KeySize][ValueSize][Key][Value]
                while (currentPos < fileLength) {
                    reader.seek(currentPos);
                    
                    long timestamp = reader.readLong();
                    int keySize = reader.readInt();
                    int valueSize = reader.readInt();

                    byte[] keyBytes = new byte[keySize];
                    reader.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    long valueOffset = reader.getFilePointer();
                    
                    // Skip the value bytes for now to stay fast
                    currentPos = valueOffset + valueSize; 

                    // Is this record the latest version of the key?
                    if (store.getKeyDir().isLiveEntry(key, timestamp)) {
                        // Read the value now because we need to migrate it
                        byte[] valueBytes = new byte[valueSize];
                        reader.seek(valueOffset);
                        reader.readFully(valueBytes);

                        // Write it to the new clean compacted segment
                        Segment.WriteResult result = compactSegment.append(keyBytes, valueBytes, timestamp);

                        // update the KeyDir to point to the new compacted file position
                        store.getKeyDir().updateEntry(key, compactFileId, result.valueOffset, result.valueSize, timestamp);

                        KeyEntry currentIndex = store.getKeyDir().getEntry(key);
                        if (currentIndex != null && currentIndex.getFileId().equals(compactFileId)) {
                            // Write to companion Hint File only if this value genuinely survived!
                            hintFile.writeEntry(timestamp, keySize, result.valueOffset, result.valueSize, keyBytes);
                        }
                    }
                }
            }
            filesCompacted.add(fileId);
        }


        // Tell the store to swap the newly compacted segment in,
        // and safely delete the old physically redundant disk files.
        store.replaceCompactedSegments(filesCompacted, compactFileId, compactSegment);
        hintFile.closeWriting();
    }
}