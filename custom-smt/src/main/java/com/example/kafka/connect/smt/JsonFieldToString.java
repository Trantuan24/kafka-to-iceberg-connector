package com.example.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom SMT to convert a JSON field (array or object) to a JSON string.
 * This is useful when you want to store complex nested structures as strings in Iceberg.
 */
public abstract class JsonFieldToString<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELD_CONFIG = "field";
    private static final String PURPOSE = "convert JSON field to string";

    private String fieldName;
    private ObjectMapper objectMapper;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldName = config.getString(FIELD_CONFIG);
        objectMapper = new ObjectMapper();
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        if (record.valueSchema() == null) {
            // Schemaless mode - work with Map
            return applySchemaless(record);
        } else {
            // Schema mode - work with Struct
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = (Map<String, Object>) record.value();
        final Map<String, Object> updatedValue = new HashMap<>(value);

        if (value.containsKey(fieldName)) {
            try {
                Object fieldValue = value.get(fieldName);
                String jsonString = objectMapper.writeValueAsString(fieldValue);
                updatedValue.put(fieldName, jsonString);
            } catch (Exception e) {
                throw new RuntimeException("Failed to convert field '" + fieldName + "' to JSON string", e);
            }
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = (Struct) record.value();
        final Schema schema = value.schema();

        // Find the field in the schema
        Field field = schema.field(fieldName);
        if (field == null) {
            return record;
        }

        // Build new schema with the field as STRING
        SchemaBuilder builder = SchemaBuilder.struct();
        for (Field f : schema.fields()) {
            if (f.name().equals(fieldName)) {
                builder.field(f.name(), Schema.OPTIONAL_STRING_SCHEMA);
            } else {
                builder.field(f.name(), f.schema());
            }
        }
        Schema updatedSchema = builder.build();

        // Build new struct with converted field
        Struct updatedValue = new Struct(updatedSchema);
        for (Field f : schema.fields()) {
            if (f.name().equals(fieldName)) {
                try {
                    Object fieldValue = value.get(f);
                    String jsonString = objectMapper.writeValueAsString(fieldValue);
                    updatedValue.put(f.name(), jsonString);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to convert field '" + fieldName + "' to JSON string", e);
                }
            } else {
                updatedValue.put(f.name(), value.get(f));
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "Field name to convert to JSON string");

    public static class Key<R extends ConnectRecord<R>> extends JsonFieldToString<R> {
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
                    record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends JsonFieldToString<R> {
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, updatedValue, record.timestamp());
        }
    }
}
