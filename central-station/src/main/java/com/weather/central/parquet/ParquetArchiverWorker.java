package com.weather.central.parquet;

import com.weather.central.kafka.ArchiveConsumer;
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
    private final String archiveDir = "Archive";

    public ParquetArchiverWorker() {
        File dir = new File(archiveDir);
        if (!dir.exists()) dir.mkdirs();
    }

    // Run a check every 10 seconds to look at buffer volumes
    @Scheduled(fixedRate = 10000)
    public void processArchiveQueue() {
        // Only flush if we hit our 1k target limit to ensure large, healthy Parquet column groups
        if (ArchiveConsumer.archiveBuffer.size() < BATCH_SIZE_THRESHOLD) {
            return; 
        }

        List<WeatherMessage> batch = new ArrayList<>();
        while (batch.size() < BATCH_SIZE_THRESHOLD && !ArchiveConsumer.archiveBuffer.isEmpty()) {
            WeatherMessage msg = ArchiveConsumer.archiveBuffer.poll();
            if (msg != null) batch.add(msg);
        }

        if (batch.isEmpty()) return;

        long timestamp = System.currentTimeMillis();
        String parquetPath = archiveDir + File.separator + "weather_" + timestamp + ".parquet";

        // Load DuckDB fully in-memory to act as an execution engine
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             java.sql.Statement stmt = conn.createStatement()) {

            // 1. Create a structured temporary in-memory table inside DuckDB
            stmt.execute("CREATE TABLE temp_weather (station_id BIGINT, s_no BIGINT, battery_status VARCHAR, status_timestamp BIGINT, humidity INT, temperature INT, wind_speed INT);");

            // 2. Prepare a high-speed batch insertion pipeline
            try (PreparedStatement pStmt = conn.prepareStatement("INSERT INTO temp_weather VALUES (?, ?, ?, ?, ?, ?, ?);")) {
                for (WeatherMessage msg : batch) {
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
                pStmt.executeBatch(); // Executes inside raw memory layout instantly
            }

            // 3. Trigger the magic DuckDB SQL Query to export directly to a compressed columnar Parquet file!
            String exportSql = String.format("COPY temp_weather TO '%s' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');", parquetPath);
            stmt.execute(exportSql);
            
            System.out.println("[ARCHIVER] Successfully compiled and wrote " + batch.size() + " rows to " + parquetPath);

        } catch (Exception e) {
            System.err.println("[ARCHIVER ERROR] Failed compiling Parquet file layer: " + e.getMessage());
            // Safe recovery: Return the items to the queue so data isn't lost on database write faults
            ArchiveConsumer.archiveBuffer.addAll(batch);
        }
    }
}