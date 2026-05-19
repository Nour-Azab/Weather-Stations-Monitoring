package com.weather.central.bitcask;


public class KeyEntry {
    // metadata for a key in the keydir
    private String fileId; // segment file identifier
    private long valuePos; // position of the value in the segment file
    private int valueSize; // size of the value in bytes
    private long timestamp; // last update time for the key
    public KeyEntry(String fileId, long valuePos, int valueSize, long timestamp) {
        this.fileId = fileId;
        this.valuePos = valuePos;
        this.valueSize = valueSize;
        this.timestamp = timestamp;
    }
    public String getFileId() {
        return fileId;  
    }
    public long getValuePos() {
        return valuePos;
    }
    public int getValueSize() {
        return valueSize;
    }
    public long getTimestamp() {
        return timestamp;
    }
}
