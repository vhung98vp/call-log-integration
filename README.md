# Call Log Integration
This project is designed to check call log and integrate relation data into the company's systems.

## Features

- Find relation based on call log and integrate to Elasticsearch.
- Scalable and modular architecture.
- Comprehensive logging and error handling.

## Configuration
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap server address
- `KAFKA_CONSUMER_GROUP`: Kafka consumer group name
- `KAFKA_CONSUMER_TIMEOUT`: Kafka consumer timeout (in second)
- `KAFKA_AUTO_OFFSET_RESET`: Kafka offset reset policy
- `KAFKA_INPUT_TOPIC`: Kafka topic for input messages
- `KAFKA_OUTPUT_TOPIC`: Kafka topic for output messages
- `KAFKA_ERROR_TOPIC`: Kafka topic for error messages
- `ES_URL`: URL of the Elasticsearch instance
- `ES_USER`: Elasticsearch username
- `ES_PASSWORD`: Elasticsearch password
- `ES_PHONE_INDEX`: Name of the Elasticsearch index for phone profile
- `ES_RELATION_INDEX`: Name of the Elasticsearch index for relations
- `ES_UUID_NAMESPACE`: UUID Namespace to gen entity UUID
- `ES_RELATION_TYPE`: Relation type between 2 numbers of Elasticsearch
- `ES_ENTITY_TYPE`: Entity type of phone in Elasticsearch
- `ES_ENTITY_SOURCE`: Entity source id of phone in Elasticsearch
- `ES_ENTITY_DATASOURCE`: Entity datasource of phone in Elasticsearch
- `ES_CREATE_USER`: Default user to create relation in Elasticsearch
<!-- - `CLICKHOUSE_URL`: ClickHouse database URL
- `CLICKHOUSE_TABLE`: ClickHouse table name -->
- `LOG_API_URL`: API URL for old logs query
- `THRESHOLD_TOTAL_DURATION`: Threshold for total call duration
- `THRESHOLD_MAX_DURATION`: Threshold for maximum call duration
- `THRESHOLD_AVG_DURATION`: Threshold for average call duration
- `THRESHOLD_TOTAL_CALLS`: Threshold for total call count
- `THRESHOLD_AVG_DAYS`: Threshold for average days of calls
- `THRESHOLD_SPAM_*`: Thresholds for spam number detection
- `THRESHOLD_SERVICE_*`: Thresholds for service number detection
<!-- - `MAX_WORKERS`: Maximum number of worker threads -->