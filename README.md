# Kafka-connect with Iceberg connector demo

### Run:
- First start `iceberg-kafka-connect` standalone and all dependecies: `docker compose up`
- Optionally create the Iceberg table using the Iceberg REST Catalog. 

If you don't do this, kafka-connect will create it on the first event and you will see an exception you can ignore.

```sh
curl -X POST http://localhost:8181/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": ["default_database"],
    "properties": {}
  }'

curl -X POST http://localhost:8181/v1/namespaces/default_database/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test1",
    "location": "s3://bucket/warehouse/default_database/test1",
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {
          "id": 1,
          "name": "id",
          "required": false,
          "type": "long"
        },
        {
          "id": 2,
          "name": "data",
          "required": false,
          "type": "string"
        }
      ]
    },
    "partition-spec": {
      "spec-id": 0,
      "fields": []
    }
  }'

curl http://localhost:8181/v1/namespaces
curl http://localhost:8181/v1/namespaces/default_database/tables
curl http://localhost:8181/v1/namespaces/default_database/tables/test1
```

- publish an event to kafka: `docker exec -ti kafka bash -c "kafka-console-producer -bootstrap-server localhost:9092 --topic test1"`
    - event: `{"id": 1, "data":"some data"}`
- you will see the successfuly log in the `connect` container: `INFO Received metrics report: CommitReport ....`
- query the Iceberg table with Trino:
    - `docker exec -it iceberg-kafka-connect-demo-trino-1 trino`
    - `SELECT * FROM iceberg.default_database.test1;`
    - you will see the `"id": 1, "data":"some data"` in the table.

### Notes:
The `inceberg-kafka-connect` dependencies were added from the [iceberg source repository](https://github.com/apache/iceberg). I had to manually build Iceberg and all it's dependencies. [This is according to the official documentation](https://iceberg.apache.org/docs/latest/kafka-connect/#installation). All the plugins are from this build.

The connector compose file was inspired from the [official Iceberg repo](https://github.com/apache/iceberg/blob/main/kafka-connect/kafka-connect-runtime/docker/docker-compose.yml) but it's not a copy.

Note I built Iceberg stable version: `1.9.x`.


### Docs:
- https://iceberg.apache.org/docs/latest/kafka-connect/
- https://github.com/apache/iceberg/blob/main/kafka-connect/kafka-connect-runtime/docker/docker-compose.yml
- https://github.com/getindata/kafka-connect-iceberg-sink/blob/develop/README.md
- https://docs.confluent.io/platform/current/connect/userguide.html
- https://gitlab.com/ueisele/kafka-images/-/blob/main/examples/connect-standalone/source-http-json/compose.yaml?ref_type=heads