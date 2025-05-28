import uuid
from .config import logger, RL_THRESHOLDS, ES_RL_PROPERTY, ES_PHONE_PROPERTY, \
    ES_RL_CONF, MES_PHONE_AGG, MES_PHONE_MD, SPAM_THRESHOLDS, SERVICE_THRESHOLDS


def build_phone_uid(phone_number):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, phone_number))

def build_relation_id(phone_a, phone_b, relation_type=ES_RL_CONF['relation_type']):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{relation_type}:{phone_a}:{phone_b}"))


def check_relation_by_agg(log_agg):
    if log_agg[MES_PHONE_AGG['total_duration']] >= RL_THRESHOLDS['total_duration'] or \
      log_agg[MES_PHONE_AGG['max_duration']] >= RL_THRESHOLDS['max_duration'] or \
      (log_agg[MES_PHONE_AGG['total_calls']] >= RL_THRESHOLDS['total_calls'] and \
        log_agg[MES_PHONE_AGG['avg_days']] >= RL_THRESHOLDS['avg_days']):
        return True
    return False

def check_relation_by_agg_metadata(log_agg, meta_A, meta_B):
    log_agg['avg_duration'] = log_agg[MES_PHONE_AGG['total_duration']] / log_agg[MES_PHONE_AGG['total_calls']]
    threshold_avg_duration_A = (calc_avg_duration(meta_A, MES_PHONE_MD) 
                                    if meta_A 
                                    and meta_A[MES_PHONE_MD['total_calls']] > 2 * RL_THRESHOLDS['total_calls'] 
                                    else RL_THRESHOLDS['avg_duration'])
    threshold_avg_duration_B = (calc_avg_duration(meta_B, MES_PHONE_MD)
                                    if meta_B 
                                    and meta_B[MES_PHONE_MD['total_calls']] > 2 * RL_THRESHOLDS['total_calls'] 
                                    else RL_THRESHOLDS['avg_duration'])
    if log_agg['avg_duration'] >= 0.5 * min(threshold_avg_duration_A, threshold_avg_duration_B):
        return True
    return False

def check_relation_by_old_logs(log_agg, old_logs_agg, meta_A, meta_B):
    all_logs_agg = {
        'total_calls': old_logs_agg['total_calls'] + log_agg[MES_PHONE_AGG['total_calls']],
        'total_duration': old_logs_agg['total_duration'] + log_agg[MES_PHONE_AGG['total_duration']],
    }    
    multiple = 1 if old_logs_agg['total_months'] <= 3 else 2

    all_logs_agg['avg_duration'] = all_logs_agg['total_duration'] / all_logs_agg['total_calls']
    threshold_avg_duration_A = (calc_avg_duration(meta_A, ES_PHONE_PROPERTY) 
                                    if meta_A 
                                    and meta_A[ES_PHONE_PROPERTY['total_calls']] > 2 * RL_THRESHOLDS['total_calls'] 
                                    else RL_THRESHOLDS['avg_duration'])
    threshold_avg_duration_B = (calc_avg_duration(meta_B, ES_PHONE_PROPERTY) 
                                    if meta_B 
                                    and meta_B[ES_PHONE_PROPERTY['total_calls']] > 2 * RL_THRESHOLDS['total_calls'] 
                                    else RL_THRESHOLDS['avg_duration'])

    if all_logs_agg['total_duration'] >= multiple * RL_THRESHOLDS['total_duration'] and \
      all_logs_agg['total_calls'] >= multiple * RL_THRESHOLDS['total_calls'] and \
      all_logs_agg['avg_duration'] >= 0.5 * min(threshold_avg_duration_A, threshold_avg_duration_B):
        return True
    
    return False


def calc_avg_duration(metadata, property):
    if metadata and metadata[property['total_calls']] > 0:
        total_calls_from = metadata[property['total_calls']] * metadata[property['call_from_rate']]
        total_calls_to = metadata[property['total_calls']] - total_calls_from
        total_duration_from = metadata[property['avg_duration_from']] * total_calls_from
        total_duration_to = metadata[property['avg_duration_to']] * total_calls_to
        return (total_duration_from + total_duration_to) / metadata[property['total_calls']]
    return 0


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