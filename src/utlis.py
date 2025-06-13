import uuid
from .config import logger, RL_THRESHOLDS, ES_RL_PROPERTY, ES_PHONE_PROPERTY, \
    ES_RL_CONF, MES_PHONE_AGG, MES_PHONE_MD, SPAM_THRESHOLDS, SERVICE_THRESHOLDS


def build_phone_uid(phone_number, entity_type=ES_RL_CONF['entity_type'], namespace=ES_RL_CONF['uid_namespace']):
    return str(uuid.uuid5(namespace, f"{entity_type}:{phone_number}"))

def build_relation_id(phone_a, phone_b, relation_type=ES_RL_CONF['relation_type'], namespace=ES_RL_CONF['uid_namespace']):
    return str(uuid.uuid5(namespace, f"{relation_type}:{phone_a}:{phone_b}"))


# CHECK RELATION
def check_relation_by_agg(agg_list):
    for log_agg in agg_list:
        if log_agg[MES_PHONE_AGG['total_duration']] >= RL_THRESHOLDS['total_duration'] or \
        log_agg[MES_PHONE_AGG['max_duration']] >= RL_THRESHOLDS['max_duration'] or \
        (log_agg[MES_PHONE_AGG['total_calls']] >= RL_THRESHOLDS['total_calls'] and \
            log_agg[MES_PHONE_AGG['avg_days']] >= RL_THRESHOLDS['avg_days']):
            return True
    return False

def check_relation_by_agg_metadata(agg_list, meta_A):
    multiple = 1 if len(agg_list) <= 3 else 2
    agg_keys = ['total_calls', 'total_duration', 'avg_days']
    agg_total = {k: sum(d.get(MES_PHONE_AGG[k], 0) for d in agg_list) for k in agg_keys}
    agg_total['avg_duration'] = agg_total['total_duration'] / agg_total['total_calls'] if agg_total['total_calls'] else 0
    threshold_avg_duration = calc_threshold_avg_duration(meta_A, MES_PHONE_MD)

    if (agg_total['total_calls'] >= multiple * RL_THRESHOLDS['total_calls'] and
            agg_total['avg_duration'] >= multiple * 0.5 * threshold_avg_duration):
        return True
    return False


# ADDITIONAL UTILS

def calc_threshold_avg_duration(meta_list, property):
    total_calls_all, total_duration_all = 0, 0
    for meta in meta_list:
        total_calls_all += meta[property['total_calls']]
        total_duration_from = meta[property['avg_duration_from']] * meta[property['total_calls']] * meta[property['call_from_rate']]
        total_duration_to = meta[property['avg_duration_to']] * meta[property['total_calls']] * (1 - meta[property['call_from_rate']])
        total_duration_all += total_duration_from + total_duration_to
    avg_duration_all = total_duration_all / total_calls_all if total_calls_all else 0
    return avg_duration_all if total_calls_all > 2 * RL_THRESHOLDS['total_calls'] else RL_THRESHOLDS['avg_duration']


def build_output_message(phone_a, phone_b):
    return {
        ES_RL_PROPERTY['relation_type']: ES_RL_CONF['relation_type'],
        ES_RL_PROPERTY['relation_id']: build_relation_id(phone_a, phone_b),
        ES_RL_PROPERTY['from_entity']: build_phone_uid(phone_a),
        ES_RL_PROPERTY['to_entity']: build_phone_uid(phone_b),
        ES_RL_PROPERTY['from_entity_type']: ES_RL_CONF['entity_type'],
        ES_RL_PROPERTY['to_entity_type']: ES_RL_CONF['entity_type'],
        ES_RL_PROPERTY['from_source']: ES_RL_CONF['source'],
        ES_RL_PROPERTY['to_source']: ES_RL_CONF['source'],
        ES_RL_PROPERTY['from_datasource']: ES_RL_CONF['datasource'],
        ES_RL_PROPERTY['to_datasource']: ES_RL_CONF['datasource'],
        ES_RL_PROPERTY['create_user']: ES_RL_CONF['create_user']
    }


def is_spam_number(phone_number, metadata):
    metadata['avg_call_per_day'] = metadata[MES_PHONE_MD['total_calls']] \
            * metadata[MES_PHONE_MD['call_from_rate']] / metadata[MES_PHONE_MD['total_day_from']] \
                if metadata[MES_PHONE_MD['total_day_from']] else 0
    metadata['avg_call_per_contact'] = metadata[MES_PHONE_MD['total_calls']] \
            * metadata[MES_PHONE_MD['call_from_rate']] / metadata[MES_PHONE_MD['total_contacts']] \
                if metadata[MES_PHONE_MD['total_contacts']] else 0
    metadata['total_duration'] = metadata[MES_PHONE_MD['total_calls']] * metadata[MES_PHONE_MD['call_from_rate']] \
            * metadata[MES_PHONE_MD['avg_duration_from']] + \
            metadata[MES_PHONE_MD['total_calls']] * (1 - metadata[MES_PHONE_MD['call_from_rate']]) \
            * metadata[MES_PHONE_MD['avg_duration_to']]

    # Spam detection with extreme values
    if metadata['avg_call_per_day'] > SPAM_THRESHOLDS['extreme_avg_call_per_day'] or \
        metadata['total_duration'] > SPAM_THRESHOLDS['extreme_total_duration']:
        logger.info(f"Spam number detected by filter 1: {phone_number}")
        return True
    
    # Spam detection
    if metadata['avg_call_per_day'] > SPAM_THRESHOLDS['avg_call_per_day'] and \
        metadata[MES_PHONE_MD['total_contacts']] > SPAM_THRESHOLDS['total_contacts'] and \
        metadata['avg_call_per_contact'] < SPAM_THRESHOLDS['avg_call_per_contact'] and \
        metadata[MES_PHONE_MD['call_from_rate']] > SPAM_THRESHOLDS['call_from_rate'] and \
        metadata[MES_PHONE_MD['avg_duration_from']] < SPAM_THRESHOLDS['avg_duration']:
        logger.info(f"Spam number detected by filter 2: {phone_number}")
        return True

    # Service detection
    if metadata['avg_call_per_day'] > SERVICE_THRESHOLDS['avg_call_per_day'] and \
        metadata[MES_PHONE_MD['total_day_from']] > SERVICE_THRESHOLDS['total_day_from'] and \
        metadata[MES_PHONE_MD['total_contacts']] > SERVICE_THRESHOLDS['total_contacts'] and \
        metadata[MES_PHONE_MD['call_from_rate']] > SERVICE_THRESHOLDS['call_from_rate'] and \
        metadata[MES_PHONE_MD['avg_duration_from']] < SERVICE_THRESHOLDS['avg_duration']:
        logger.info(f"Service number detected by filter: {phone_number}")
        return True

    return False