import os
import logging
import json
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__)

MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 4))

KAFKA = {
    'brokers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
    'consumer_group': os.environ.get('KAFKA_CONSUMER_GROUP', 'default_consumer_group'),
    'consumer_timeout': float(os.environ.get('KAFKA_CONSUMER_TIMEOUT', 1)),
    'auto_offset_reset': os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
    'input_topic': os.environ.get('KAFKA_INPUT_TOPIC'),
    'output_topic': os.environ.get('KAFKA_OUTPUT_TOPIC'),
    'error_topic': os.environ.get('KAFKA_ERROR_TOPIC')
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
    'phone_index': os.environ.get('ES_PHONE_INDEX', 'phone_index'),
    'relation_index': os.environ.get('ES_RELATION_INDEX', 'relation_index'),
}

ES_RL_CONF = {
    'relation_type': os.environ.get('ES_RELATION_TYPE'),
    'entity_type': os.environ.get('ES_ENTITY_TYPE'),
    'source': int(os.environ.get('ES_ENTITY_SOURCE')),
    'datasource': int(os.environ.get('ES_ENTITY_DATASOURCE')),
    'create_user': int(os.environ.get('ES_CREATE_USER'))  
}

CLICKHOUSE = {
    'url': os.environ.get('CLICKHOUSE_URL'),
    'table': os.environ.get('CLICKHOUSE_TABLE', 'call_logs')
}

RL_THRESHOLDS = {
    'total_duration': int(os.environ.get('THRESHOLD_TOTAL_DURATION', '120')),
    'max_duration': int(os.environ.get('THRESHOLD_MAX_DURATION', '60')),
    'avg_duration': int(os.environ.get('THRESHOLD_AVG_DURATION', '30')),
    'total_calls': int(os.environ.get('THRESHOLD_TOTAL_CALLS', '4')),
    'avg_days': int(os.environ.get('THRESHOLD_AVG_DAYS', '1'))
}

SPAM_THRESHOLDS = {
    'extreme_avg_call_per_day': int(os.environ.get('THRESHOLD_SPAM_EXTREME_AVG_CALL_PER_DAY', '500')),
    'extreme_total_duration': int(os.environ.get('THRESHOLD_SPAM_EXTREME_TOTAL_DURATION', '1000000')),
    'avg_call_per_day': int(os.environ.get('THRESHOLD_SPAM_AVG_CALL_PER_DAY', '50')),
    'total_contacts': int(os.environ.get('THRESHOLD_SPAM_TOTAL_CONTACTS', '100')),
    'avg_call_per_contact': int(os.environ.get('THRESHOLD_SPAM_AVG_CALL_PER_CONTACT', '3')),
    'call_from_rate': float(os.environ.get('THRESHOLD_SPAM_CALL_FROM_RATE', '0.9')),
    'avg_duration': int(os.environ.get('THRESHOLD_SPAM_AVG_DURATION', '10'))
}

SERVICE_THRESHOLDS = {
    'avg_call_per_day': int(os.environ.get('THRESHOLD_SERVICE_AVG_CALL_PER_DAY', '10')),
    'total_day_from': int(os.environ.get('THRESHOLD_SERVICE_TOTAL_DAY_FROM', '20')),
    'total_contacts': int(os.environ.get('THRESHOLD_SERVICE_TOTAL_CONTACTS', '50')),
    'call_from_rate': float(os.environ.get('THRESHOLD_SERVICE_CALL_FROM_RATE', '0.7')),
    'avg_duration': int(os.environ.get('THRESHOLD_SERVICE_AVG_DURATION', '60'))
}


with open("config.json", "r") as f:
    config = json.load(f)
    ES_PROPERTY = config.get('es_properties', {})
    ES_RL_PROPERTY = config.get('es_relation_properties', {})
    ES_PHONE_PROPERTY = config.get('es_phone_properties', {})
    MES_FIELD = config.get('message_fields', {})
    MES_PHONE_MD = config.get('message_phone_metadata', {})
    MES_PHONE_AGG = config.get('message_phone_agg', {})
    CH_PROPERTY = config.get('clickhouse_properties', {})

if not KAFKA['brokers']:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Please set it to the Kafka brokers address.")
if not ES['url']:
    raise ValueError("ES_URL environment variable is not set. Please set it to the Elasticsearch URL.")
if not CLICKHOUSE['url']:
    raise ValueError("CLICKHOUSE_URL environment variable is not set. Please set it to the ClickHouse URL.")