package com.weather.central.bitcask;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class HintFile {
    private final String filePath;
    private RandomAccessFile writeHint;

    // The constructor just remembers WHERE the file lives
    public HintFile(String filePath) {
        this.filePath = filePath;
    }

    public void openForWriting() throws IOException {
        this.writeHint = new RandomAccessFile(this.filePath, "rw");
        this.writeHint.seek(this.writeHint.length());
    }

    public void writeEntry(long timestamp, int keySize, long valueOffset, int valueSize, byte[] keyBytes)
            throws IOException {
        if (writeHint == null) {
            throw new IllegalStateException("Hint file must be opened via openForWriting() before adding entries.");
        }
        // Seek to the end to append data
        writeHint.seek(writeHint.length());

        writeHint.writeLong(timestamp);
        writeHint.writeInt(keySize);
        writeHint.writeLong(valueOffset);
        writeHint.writeInt(valueSize);
        writeHint.write(keyBytes);
    }

    public void closeWriting() throws IOException {
        if (writeHint != null) {
            writeHint.close();
            writeHint = null;
        }
    }

    // Reads a hint file and rapidly populates the memory KeyDir index.
    public void loadIntoKeyDir(String fileId, KeyDir keyDir) throws IOException {
        File fileOnDisk = new File(this.filePath);
        if (!fileOnDisk.exists()) {
            return; // Nothing to load
        }
        try (RandomAccessFile reader = new RandomAccessFile(this.filePath, "r")) {
            long fileLength = reader.length();
            long currentPos = 0;

            while (currentPos < fileLength) {
                reader.seek(currentPos);

                long timestamp = reader.readLong();
                int keySize = reader.readInt();
                long valueOffset = reader.readLong();
                int valueSize = reader.readInt();

                byte[] keyBytes = new byte[keySize];
                reader.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                // Instantly bind the key directly to its data positions
                keyDir.updateEntry(key, fileId, valueOffset, valueSize, timestamp);

                currentPos = reader.getFilePointer();
            }
        }
    }
}