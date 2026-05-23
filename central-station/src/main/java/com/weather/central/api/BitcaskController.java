package com.weather.central.api;

import com.weather.central.bitcask.BitcaskService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.io.IOException;

@RestController
@RequestMapping("/api/bitcask")
public class BitcaskController {

    private final BitcaskService bitcaskService;

    public BitcaskController(BitcaskService bitcaskService) {
        this.bitcaskService = bitcaskService;
    }

    @GetMapping
    public ResponseEntity<Map<String, String>> getAllEntries() {
        Map<String, String> allData = bitcaskService.getAllLiveEntries();
        return ResponseEntity.ok(allData);
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> getEntryByKey(@PathVariable String key) {
        try {
            String value = bitcaskService.get(key);
            if (value == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(value);
        } catch (IOException e) {
            return ResponseEntity.status(500).build();
        }
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Void> deleteEntryByKey(@PathVariable String key) {
        try {
            // Check if the key exists first
            String existingValue = bitcaskService.get(key);
            if (existingValue == null) {
                return ResponseEntity.notFound().build();
            }

            // Fire the internal Bitcask deletion logic (appends a tombstone to disk)
            bitcaskService.delete(key);
            return ResponseEntity.noContent().build(); // HTTP 204 No Content on success
            
        } catch (Exception e) {
            System.err.println("[API ERROR] Failed executing delete routine: " + e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
}