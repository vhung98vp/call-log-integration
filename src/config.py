import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA = {
    'brokers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
    'consumer_group': os.environ.get('KAFKA_CONSUMER_GROUP', 'default_consumer_group'),
    'consumer_timeout': float(os.environ.get('KAFKA_CONSUMER_TIMEOUT', 1)),
    'auto_offset_reset': os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
    'input_topic': os.environ.get('KAFKA_INPUT_TOPIC'),
    'output_topic': os.environ.get('KAFKA_OUTPUT_TOPIC'),
    'error_topic': os.environ.get('KAFKA_ERROR_TOPIC'),
    'relation_type': os.environ.get('KAFKA_RELATION_TYPE'),
    'entity_type': os.environ.get('KAFKA_ENTITY_TYPE'),
    'source': int(os.environ.get('KAFKA_ENTITY_SOURCE')),
    'datasource': int(os.environ.get('KAFKA_ENTITY_DATASOURCE')),
    'es_create_user': int(os.environ.get('KAFKA_ES_CREATE_USER'))
}

KAFKA_CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA['brokers'],
    'group.id': KAFKA['consumer_group'],
    'auto.offset.reset': KAFKA['auto_offset_reset']
}

KAFKA_PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA['brokers']
}

ES = {
    'url': os.environ.get('ES_URL'),
    'user': os.environ.get('ES_USER'),
    'password': os.environ.get('ES_PASSWORD'),
    'phone_index': os.environ.get('ES_PHONE_INDEX'),
    'relation_index': os.environ.get('ES_RELATION_INDEX')
}

CLICKHOUSE = {
    'url': os.environ.get('CLICKHOUSE_URL'),
    'table': os.environ.get('CLICKHOUSE_TABLE', 'call_logs')
}

THRESHOLDS = {
    'total_duration': int(os.environ.get('THRESHOLD_TOTAL_DURATION', '120')),
    'max_duration': int(os.environ.get('THRESHOLD_MAX_DURATION', '60')),
    'avg_duration': int(os.environ.get('THRESHOLD_AVG_DURATION', '30')),
    'total_calls': int(os.environ.get('THRESHOLD_TOTAL_CALLS', '4')),
    'avg_days': int(os.environ.get('THRESHOLD_AVG_DAYS', '1'))
}

MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '10'))

if not KAFKA['brokers']:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Please set it to the Kafka brokers address.")
if not ES['url']:
    raise ValueError("ES_URL environment variable is not set. Please set it to the Elasticsearch URL.")
if not CLICKHOUSE['url']:
    raise ValueError("CLICKHOUSE_URL environment variable is not set. Please set it to the ClickHouse URL.")