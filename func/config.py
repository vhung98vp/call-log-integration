import os
import logging
import json
from uuid import UUID
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__)


ES_RL_CONF = {
    'uid_namespace': UUID(os.environ.get('ES_UUID_NAMESPACE')),
    'relation_type': os.environ.get('ES_RELATION_TYPE'),
    'entity_type': os.environ.get('ES_ENTITY_TYPE'),
    'source': int(os.environ.get('ES_ENTITY_SOURCE')),
    'datasource': int(os.environ.get('ES_ENTITY_DATASOURCE')),
    'create_user': int(os.environ.get('ES_CREATE_USER'))  
}

RL_THRESHOLDS = {
    'total_duration': int(os.environ.get('THRESHOLD_RELATION_TOTAL_DURATION', '120')),
    'max_duration': int(os.environ.get('THRESHOLD_RELATION_MAX_DURATION', '60')),
    'avg_duration': int(os.environ.get('THRESHOLD_RELATION_AVG_DURATION', '30')),
    'total_calls': int(os.environ.get('THRESHOLD_RELATION_TOTAL_CALLS', '4')),
    'avg_days': int(os.environ.get('THRESHOLD_RELATION_AVG_DAYS', '1'))
}

SPAM_THRESHOLDS = {
    'extreme_avg_call_per_day': int(os.environ.get('THRESHOLD_SPAM_EXTREME_AVG_CALL_PER_DAY', '500')),
    'extreme_total_duration': int(os.environ.get('THRESHOLD_SPAM_EXTREME_TOTAL_DURATION', '1000000')),
    'avg_call_per_day': int(os.environ.get('THRESHOLD_SPAM_AVG_CALL_PER_DAY', '50')),
    'total_contacts': int(os.environ.get('THRESHOLD_SPAM_TOTAL_CONTACTS', '100')),
    'avg_call_per_contact': int(os.environ.get('THRESHOLD_SPAM_AVG_CALL_PER_CONTACT', '3')),
    'call_from_rate': float(os.environ.get('THRESHOLD_SPAM_CALL_FROM_RATE', '0.9')),
    'avg_duration': int(os.environ.get('THRESHOLD_SPAM_AVG_DURATION', '30'))
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
    ES_RL_PROPERTY = config.get('es_relation_properties', {})
    MES_PHONE_MD = config.get('message_phone_metadata', {})
    MES_PHONE_AGG = config.get('message_phone_agg', {})
