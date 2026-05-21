package com.weather.central.parquet;

import com.weather.central.kafka.ArchiveConsumer;
import com.weather.central.model.ArchiveTask;
import com.weather.central.model.WeatherMessage;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

@Component
public class ParquetArchiverWorker {

    private static final int BATCH_SIZE_THRESHOLD = 1000;
    private final String archiveDir = System.getenv().getOrDefault("PARQUET_STORAGE_PATH", "./data/parquet");

    public ParquetArchiverWorker() {
        File dir = new File(archiveDir);
        if (!dir.exists())
            dir.mkdirs();
    }

    @Scheduled(fixedRate = 10000)
    public void processArchiveQueue() {
        if (ArchiveConsumer.archiveBuffer.size() < BATCH_SIZE_THRESHOLD) {
            return;
        }

        List<ArchiveTask> batch = new ArrayList<>();
        while (batch.size() < BATCH_SIZE_THRESHOLD && !ArchiveConsumer.archiveBuffer.isEmpty()) {
            ArchiveTask task = ArchiveConsumer.archiveBuffer.poll();
            if (task != null)
                batch.add(task);
        }

        if (batch.isEmpty())
            return;

        long timestamp = System.currentTimeMillis();
        String parquetPath = archiveDir + File.separator + "weather_" + timestamp + ".parquet";

        try {
            // Force class loading to prevent transient driver extraction failures across
            // parallel threads
            Class.forName("org.duckdb.DuckDBDriver");

        } catch (ClassNotFoundException e) {
            System.err.println("[CRITICAL ERROR] DuckDB Dependency is missing from application classpath!");
            ArchiveConsumer.archiveBuffer.addAll(batch);
            return;
        }

        // Open an embedded instance of DuckDB fully in-memory
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement stmt = conn.createStatement()) {

            stmt.execute(
                    "CREATE TABLE temp_weather (station_id BIGINT, s_no BIGINT, battery_status VARCHAR, status_timestamp BIGINT, humidity INT, temperature INT, wind_speed INT);");

            try (PreparedStatement pStmt = conn
                    .prepareStatement("INSERT INTO temp_weather VALUES (?, ?, ?, ?, ?, ?, ?);")) {
                for (ArchiveTask task : batch) {
                    WeatherMessage msg = task.getMessage(); // Extract internal payload data

                    pStmt.setLong(1, msg.getStationId());
                    pStmt.setLong(2, msg.getSno());
                    pStmt.setString(3, msg.getBatteryStatus());
                    pStmt.setLong(4, msg.getStatusTimestamp());

                    if (msg.getWeather() != null) {
                        pStmt.setInt(5, msg.getWeather().getHumidity());
                        pStmt.setInt(6, msg.getWeather().getTemperature());
                        pStmt.setInt(7, msg.getWeather().getWindSpeed());
                    } else {
                        pStmt.setNull(5, java.sql.Types.INTEGER);
                        pStmt.setNull(6, java.sql.Types.INTEGER);
                        pStmt.setNull(7, java.sql.Types.INTEGER);
                    }
                    pStmt.addBatch();
                }
                pStmt.executeBatch();
            }

            // Export memory relational grid to compressed columnar Parquet format
            String exportSql = String.format("COPY temp_weather TO '%s' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');",
                    parquetPath);
            stmt.execute(exportSql);

            System.out
                    .println("[ARCHIVER] Successfully compiled and wrote " + batch.size() + " rows to " + parquetPath);

            // --- LATE ACKNOWLEDGMENT TRANSACTION FINALIZATION ---
            // Only after the disk operation completes without exceptions do we notify
            // Kafka.
            // Spring Kafka optimizes this call so invoking it multiple times on the same
            // batch context is completely safe.
            for (ArchiveTask task : batch) {
                task.getAck().acknowledge();
            }
            System.out.println("[ARCHIVER] Successfully committed Kafka offsets for the exported storage batch.");

        } catch (Exception e) {
            System.err.println("[ARCHIVER ERROR] Failed compiling Parquet file layer: " + e.getMessage());
            // Rollback strategy: On exceptions, safely return tasks to the queue to try
            // again on the next tick
            ArchiveConsumer.archiveBuffer.addAll(batch);
        }
    }
}