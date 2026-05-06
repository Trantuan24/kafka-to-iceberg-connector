package com.example.kafka.connect.iceberg;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom Iceberg Sink Connector với Version Control và CDC logic
 * 
 * Features:
 * 1. Deduplicate messages theo max(version) trong batch
 * 2. Batch query Iceberg để lấy existing versions
 * 3. Apply I/U/D rules với version control
 * 4. DLQ cho INSERT conflicts
 */
public class VersionControlIcebergSinkConnector extends SinkConnector {

    private Map<String, String> configProps;

    @Override
    public String version() {
        return "2.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return VersionControlIcebergSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(configProps));
        }
        return configs;
    }

    @Override
    public void stop() {
        // Cleanup if needed
    }

    @Override
    public ConfigDef config() {
        return VersionControlIcebergSinkConfig.CONFIG_DEF;
    }
}
