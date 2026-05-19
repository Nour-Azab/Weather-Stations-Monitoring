package com.weather.central.bitcask;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class Segment {
    private final String fileId;
    private final RandomAccessFile file;

    public Segment(String fileId, String filePath) throws IOException {
        this.file = new RandomAccessFile(filePath, "rw");
        this.fileId = fileId;
    }

    public synchronized WriteResult append(byte[] keyBytes, byte[] valueBytes, long timestamp) throws IOException {
        // Always seek to the end to guarantee append-only behavior
        file.seek(file.length());

        file.writeLong(timestamp);        // 8 bytes
        file.writeInt(keyBytes.length);   // 4 bytes
        file.writeInt(valueBytes.length); // 4 bytes

        file.write(keyBytes);

        long valueOffset = file.getFilePointer();

        file.write(valueBytes);

        // Return metadata to the caller to update the KeyDir map
        return new WriteResult(valueOffset, valueBytes.length, timestamp);
    }

    public synchronized String readValue(long pos, int size) throws IOException {
        file.seek(pos);
        byte[] buffer = new byte[size];
        file.readFully(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
    }

    public String getFileId() {
        return fileId;
    }

    public long getFileSize() throws IOException {
        return file.length();
    }

    public synchronized void close() throws IOException {
        if (file != null) {
            file.close();
        }
    }

    //Data carrier to pass back to BitcaskStore so it can update KeyDir
    public static class WriteResult {
        public final long valueOffset;
        public final int valueSize;
        public final long timestamp;

        public WriteResult(long valueOffset, int valueSize, long timestamp) {
            this.valueOffset = valueOffset;
            this.valueSize = valueSize;
            this.timestamp = timestamp;
        }
    }
}