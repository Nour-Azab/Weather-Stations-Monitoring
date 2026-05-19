package com.weather.central.bitcask;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class BitcaskEngineTester implements CommandLineRunner {

    private final BitcaskService bitcask;

    public BitcaskEngineTester(BitcaskService bitcask) {
        this.bitcask = bitcask;
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("\n--- STARTING BITCASK INTEGRATION TEST ---");

        // 1. Write fresh data
        System.out.println("[TEST] Inserting initial climate metrics...");
        bitcask.put("london_temp", "14.2C");
        bitcask.put("cairo_temp", "34.1C");
        bitcask.put("tokyo_temp", "21.5C");

        // 2. Immediate Read Verification
        System.out.println("[TEST] Reading back entries from active log:");
        System.out.println(" > London: " + bitcask.get("london_temp"));
        System.out.println(" > Cairo: " + bitcask.get("cairo_temp"));

        // 3. Update data (creates dead rows in the log file)
        System.out.println("\n[TEST] Modifying temperatures (Generating historical fragmentation)...");
        bitcask.put("london_temp", "15.5C");
        bitcask.put("london_temp", "16.1C"); // Latest version

        System.out.println(" > Updated London (Should read 16.1C): " + bitcask.get("london_temp"));

        // 4. Test Deletions (Appends tombstones)
        System.out.println("\n[TEST] Deleting 'tokyo_temp' key...");
        bitcask.delete("tokyo_temp");
        System.out.println(" > Read Tokyo (Should read null): " + bitcask.get("tokyo_temp"));

        // 5. Force a Compaction Cycle early to verify thread state
        System.out.println("\n[TEST] Force triggering an early Compactor sweep...");
        bitcask.triggerCompaction();

        // 6. Verify data integrity survived compaction!
        System.out.println("\n[TEST] Post-Compaction Read Validation:");
        System.out.println(" > London (Expected 16.1C): " + bitcask.get("london_temp"));
        System.out.println(" > Cairo (Expected 34.1C): " + bitcask.get("cairo_temp"));
        System.out.println(" > Tokyo (Expected null): " + bitcask.get("tokyo_temp"));

        // 7. Bulk Simulation to test segmentation threshold limit
        System.out.println("\n[TEST] Simulating bulk station flood data to trigger automatic file rotation...");
        for (int i = 0; i < 500; i++) {
            bitcask.put("sensor_" + i, "data_payload_stream_metric_value_" + i);
        }
        System.out.println("[TEST] Bulk load complete. Check your 'Data' directory to watch it fragment!");
        System.out.println("--- BITCASK INTEGRATION TEST COMPLETED ---\n");
    }
}