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
    'suffix_pattern': os.environ.get('ES_PROPERTY_SUFFIX_PATTERN', ''),
}

ES_VALUE_RL = {
    'relation_type': os.environ.get('ES_VALUE_RELATION_TYPE'),
    'entity_type': os.environ.get('ES_VALUE_ENTITY_TYPE'),
    'source': int(os.environ.get('ES_VALUE_ENTITY_SOURCE')),
    'datasource': int(os.environ.get('ES_VALUE_ENTITY_DATASOURCE')),
    'create_user': int(os.environ.get('ES_VALUE_CREATE_USER'))  
}

ES_PROPERTY_SR = {
    'phone_number_search': os.environ.get('ES_PROPERTY_PHONE_NUMBER_SEARCH', 'phone_number_s'),
    'relation_id_search': os.environ.get('ES_PROPERTY_RELATION_ID_SEARCH', 'relation_id'),  
}

ES_PROPERTY_PH = {
    'phone_number': os.environ.get('ES_PROPERTY_PHONE_NUMBER', 'phone_number'),
    'total_calls': os.environ.get('ES_PROPERTY_PHONE_TOTAL_CALLS', 'total_calls'),
    'call_from_rate': os.environ.get('ES_PROPERTY_PHONE_CALL_FROM_RATE', 'call_from_rate'),
    'avg_duration_from': os.environ.get('ES_PROPERTY_PHONE_AVG_DURATION_FROM', 'avg_duration_from'),
    'avg_duration_to': os.environ.get('ES_PROPERTY_PHONE_AVG_DURATION_TO', 'avg_duration_to')
}

ES_PROPERTY_PH_AGG = {
    'total_calls': os.environ.get('ES_PROPERTY_PHONE_AGG_TOTAL_CALLS', 'total_calls'),
    'call_from_rate': os.environ.get('ES_PROPERTY_PHONE_AGG_CALL_FROM_RATE', 'call_from_rate'),
    'max_duration': os.environ.get('ES_PROPERTY_PHONE_AGG_MAX_DURATION', 'max_duration'),
    'total_duration': os.environ.get('ES_PROPERTY_PHONE_AGG_TOTAL_DURATION', 'total_duration'),
    'avg_days': os.environ.get('ES_PROPERTY_PHONE_AGG_AVG_DAYS', 'avg_days'),
}

ES_PROPERTY_RL = {
    'relation_type': os.environ.get('ES_PROPERTY_RELATION_TYPE', 'relation_type'),
    'relation_id': os.environ.get('ES_PROPERTY_RELATION_ID', 'relation_id'),
    'from_entity': os.environ.get('ES_PROPERTY_RELATION_FROM_ENTITY', 'from_entity'),
    'to_entity': os.environ.get('ES_PROPERTY_RELATION_TO_ENTITY', 'to_entity'),
    'from_entity_type': os.environ.get('ES_PROPERTY_RELATION_FROM_ENTITY_TYPE', 'from_entity_type'),
    'to_entity_type': os.environ.get('ES_PROPERTY_RELATION_TO_ENTITY_TYPE', 'to_entity_type'),
    'from_source': os.environ.get('ES_PROPERTY_RELATION_FROM_SOURCE', 'from_source'),
    'to_source': os.environ.get('ES_PROPERTY_RELATION_TO_SOURCE', 'to_source'),
    'from_datasource': os.environ.get('ES_PROPERTY_RELATION_FROM_DATASOURCE', 'from_datasource'),
    'to_datasource': os.environ.get('ES_PROPERTY_RELATION_TO_DATASOURCE', 'to_datasource'),
    'create_user': os.environ.get('ES_PROPERTY_RELATION_CREATE_USER', 'create_user')
}

CLICKHOUSE = {
    'url': os.environ.get('CLICKHOUSE_URL'),
    'table': os.environ.get('CLICKHOUSE_TABLE', 'call_logs')
}

CH_PROPERTY = {
    'phone_a': os.environ.get('CH_PROPERTY_PHONE_A', 'phone_a'),
    'phone_b': os.environ.get('CH_PROPERTY_PHONE_B', 'phone_b'),
    'start_time': os.environ.get('CH_PROPERTY_START_TIME', 'start_time'),
    'duration': os.environ.get('CH_PROPERTY_DURATION', 'duration'),
    'call_type': os.environ.get('CH_PROPERTY_CALL_TYPE', 'call_type'),
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