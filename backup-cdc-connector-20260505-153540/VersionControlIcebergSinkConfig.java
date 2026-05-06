package com.example.kafka.connect.iceberg;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Configuration class for the CDC Version Control Iceberg Sink Connector.
 * 
 * Defines all configuration properties including:
 * - Iceberg table and catalog settings
 * - S3/MinIO storage settings
 * - Performance tuning parameters
 * - Consumer settings
 */
public class VersionControlIcebergSinkConfig extends AbstractConfig {
    
    // Table configuration
    public static final String TABLE_NAME_CONFIG = "iceberg.table.name";
    private static final String TABLE_NAME_DOC = "Iceberg table name (namespace.table format, e.g., default.tram_quan_trac_cdc)";
    
    // Catalog configuration
    public static final String CATALOG_TYPE_CONFIG = "iceberg.catalog.type";
    private static final String CATALOG_TYPE_DOC = "Catalog type (hive, hadoop, rest)";
    private static final String CATALOG_TYPE_DEFAULT = "hive";
    
    public static final String CATALOG_URI_CONFIG = "iceberg.catalog.uri";
    private static final String CATALOG_URI_DOC = "Hive Metastore URI (e.g., thrift://hive-metastore:9083)";
    
    public static final String CATALOG_WAREHOUSE_CONFIG = "iceberg.catalog.warehouse";
    private static final String CATALOG_WAREHOUSE_DOC = "Warehouse location (S3A path, e.g., s3a://bucket/warehouse/)";
    
    // S3/MinIO configuration
    public static final String S3_ENDPOINT_CONFIG = "iceberg.catalog.s3.endpoint";
    private static final String S3_ENDPOINT_DOC = "S3-compatible endpoint URL (e.g., http://minio:9000)";
    
    public static final String S3_PATH_STYLE_ACCESS_CONFIG = "iceberg.catalog.s3.path-style-access";
    private static final String S3_PATH_STYLE_ACCESS_DOC = "Use path-style access (required for MinIO)";
    private static final boolean S3_PATH_STYLE_ACCESS_DEFAULT = true;
    
    public static final String S3_ACCESS_KEY_CONFIG = "iceberg.catalog.s3.access-key-id";
    private static final String S3_ACCESS_KEY_DOC = "S3 access key ID";
    
    public static final String S3_SECRET_KEY_CONFIG = "iceberg.catalog.s3.secret-access-key";
    private static final String S3_SECRET_KEY_DOC = "S3 secret access key";
    
    public static final String S3_REGION_CONFIG = "iceberg.catalog.s3.region";
    private static final String S3_REGION_DOC = "S3 region";
    private static final String S3_REGION_DEFAULT = "us-east-1";
    
    // Processing configuration
    public static final String MAX_POLL_RECORDS_CONFIG = "consumer.max.poll.records";
    private static final String MAX_POLL_RECORDS_DOC = "Maximum records per batch (10000-50000 recommended)";
    private static final int MAX_POLL_RECORDS_DEFAULT = 50000;
    
    public static final String FLUSH_INTERVAL_CONFIG = "offset.flush.interval.ms";
    private static final String FLUSH_INTERVAL_DOC = "Offset commit interval in milliseconds (30000-60000 recommended)";
    private static final long FLUSH_INTERVAL_DEFAULT = 30000L;
    
    // ConfigDef
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        // Table configuration
        .define(
            TABLE_NAME_CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            TABLE_NAME_DOC
        )
        // Catalog configuration
        .define(
            CATALOG_TYPE_CONFIG,
            Type.STRING,
            CATALOG_TYPE_DEFAULT,
            Importance.HIGH,
            CATALOG_TYPE_DOC
        )
        .define(
            CATALOG_URI_CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            CATALOG_URI_DOC
        )
        .define(
            CATALOG_WAREHOUSE_CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            CATALOG_WAREHOUSE_DOC
        )
        // S3/MinIO configuration
        .define(
            S3_ENDPOINT_CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            S3_ENDPOINT_DOC
        )
        .define(
            S3_PATH_STYLE_ACCESS_CONFIG,
            Type.BOOLEAN,
            S3_PATH_STYLE_ACCESS_DEFAULT,
            Importance.MEDIUM,
            S3_PATH_STYLE_ACCESS_DOC
        )
        .define(
            S3_ACCESS_KEY_CONFIG,
            Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            S3_ACCESS_KEY_DOC
        )
        .define(
            S3_SECRET_KEY_CONFIG,
            Type.PASSWORD,
            ConfigDef.NO_DEFAULT_VALUE,
            Importance.HIGH,
            S3_SECRET_KEY_DOC
        )
        .define(
            S3_REGION_CONFIG,
            Type.STRING,
            S3_REGION_DEFAULT,
            Importance.MEDIUM,
            S3_REGION_DOC
        )
        // Processing configuration
        .define(
            MAX_POLL_RECORDS_CONFIG,
            Type.INT,
            MAX_POLL_RECORDS_DEFAULT,
            ConfigDef.Range.between(1000, 100000),
            Importance.MEDIUM,
            MAX_POLL_RECORDS_DOC
        )
        .define(
            FLUSH_INTERVAL_CONFIG,
            Type.LONG,
            FLUSH_INTERVAL_DEFAULT,
            ConfigDef.Range.between(10000L, 300000L),
            Importance.MEDIUM,
            FLUSH_INTERVAL_DOC
        );
    
    /**
     * Constructs a configuration instance.
     * 
     * @param props Configuration properties
     */
    public VersionControlIcebergSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
    
    // Typed accessor methods
    
    public String getTableName() {
        return getString(TABLE_NAME_CONFIG);
    }
    
    public String getCatalogType() {
        return getString(CATALOG_TYPE_CONFIG);
    }
    
    public String getCatalogUri() {
        return getString(CATALOG_URI_CONFIG);
    }
    
    public String getCatalogWarehouse() {
        return getString(CATALOG_WAREHOUSE_CONFIG);
    }
    
    public String getS3Endpoint() {
        return getString(S3_ENDPOINT_CONFIG);
    }
    
    public boolean getS3PathStyleAccess() {
        return getBoolean(S3_PATH_STYLE_ACCESS_CONFIG);
    }
    
    public String getS3AccessKey() {
        return getString(S3_ACCESS_KEY_CONFIG);
    }
    
    public String getS3SecretKey() {
        return getPassword(S3_SECRET_KEY_CONFIG).value();
    }
    
    public String getS3Region() {
        return getString(S3_REGION_CONFIG);
    }
    
    public int getMaxPollRecords() {
        return getInt(MAX_POLL_RECORDS_CONFIG);
    }
    
    public long getFlushInterval() {
        return getLong(FLUSH_INTERVAL_CONFIG);
    }
}
