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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("[CRITICAL ERROR] DuckDB Dependency is missing from application classpath!");
            ArchiveConsumer.archiveBuffer.addAll(batch);
            return;
        }

        // ── STEP 1: GROUP THE BATCH BY HIVE PARTITION PATHS ──────────────────
        Map<String, List<ArchiveTask>> partitionedBatches = new HashMap<>();
        
        for (ArchiveTask task : batch) {
            WeatherMessage msg = task.getMessage();
            
            // Parse timestamp to UTC Date components
            ZonedDateTime zdt = Instant.ofEpochSecond(msg.getStatusTimestamp()).atZone(ZoneId.of("UTC"));
            
            String partitionPath = String.format("year=%04d/month=%02d/day=%02d/station_id=%d",
                    zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), msg.getStationId());
            
            partitionedBatches.computeIfAbsent(partitionPath, k -> new ArrayList<>()).add(task);
        }

        // ── STEP 2: WRITE EACH PARTITION TO ITS OWN FOLDER ───────────────────
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement stmt = conn.createStatement()) {

            long timestamp = System.currentTimeMillis();

            for (Map.Entry<String, List<ArchiveTask>> entry : partitionedBatches.entrySet()) {
                String partitionPath = entry.getKey();
                List<ArchiveTask> subBatch = entry.getValue();

                // Create partition directory tree layout
                File targetPartitionDir = new File(archiveDir, partitionPath);
                if (!targetPartitionDir.exists()) {
                    targetPartitionDir.mkdirs();
                }

                String parquetPath = targetPartitionDir.getAbsolutePath() + File.separator + "weather_" + timestamp + ".parquet";

                // Reset temporary table for fresh sub-batch write
                stmt.execute("DROP TABLE IF EXISTS temp_weather;");
                stmt.execute("CREATE TABLE temp_weather (station_id BIGINT, s_no BIGINT, battery_status VARCHAR, status_timestamp BIGINT, humidity INT, temperature INT, wind_speed INT);");

                try (PreparedStatement pStmt = conn.prepareStatement("INSERT INTO temp_weather VALUES (?, ?, ?, ?, ?, ?, ?);")) {
                    for (ArchiveTask task : subBatch) {
                        WeatherMessage msg = task.getMessage();
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

                // Export memory grid to localized partition path location
                String exportSql = String.format("COPY temp_weather TO '%s' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');", parquetPath);
                stmt.execute(exportSql);
                System.out.println("[ARCHIVER] Wrote " + subBatch.size() + " rows to partitioned track: " + parquetPath);
            }

            // ── STEP 3: ACKNOWLEDGE ALL KAFKA MESSAGES AT ONCE ───────────────
            for (ArchiveTask task : batch) {
                task.getAck().acknowledge();
            }
            System.out.println("[ARCHIVER] Successfully committed Kafka offsets for the partitioned storage batch.");

        } catch (Exception e) {
            System.err.println("[ARCHIVER ERROR] Failed compiling Parquet file layer: " + e.getMessage());
            ArchiveConsumer.archiveBuffer.addAll(batch); // Rollback
        }
    }
}