import uuid
from config import THRESHOLDS, ES_PROPERTY_RL, ES_PROPERTY_PH, ES_VALUE_RL, ES_PROPERTY_PH_AGG, CH_PROPERTY, logger


def build_phone_uid(phone_number):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, phone_number))

def build_relation_id(phone_a, phone_b, relation_type=ES_VALUE_RL['relation_type']):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{relation_type}:{phone_a}:{phone_b}"))


def check_relation_by_agg(log_agg, meta_A, meta_B):
    if log_agg[ES_PROPERTY_PH_AGG['total_duration']] >= THRESHOLDS['total_duration'] or \
      log_agg[ES_PROPERTY_PH_AGG['max_duration']] >= THRESHOLDS['max_duration'] or \
      (log_agg[ES_PROPERTY_PH_AGG['total_calls']] >= THRESHOLDS['total_calls'] and \
        log_agg[ES_PROPERTY_PH_AGG['avg_days']] >= THRESHOLDS['avg_days']):
        logger.info("Found relation based on aggregated logs.")
        return True

    log_agg['avg_duration'] = log_agg[ES_PROPERTY_PH_AGG['total_duration']] / log_agg[ES_PROPERTY_PH_AGG['total_calls']]
    meta_A['avg_duration'] = calc_avg_duration(meta_A)
    meta_B['avg_duration'] = calc_avg_duration(meta_B)
    threshold_avg_duration_A = (meta_A['avg_duration'] 
                                    if meta_A 
                                    and meta_A[ES_PROPERTY_PH['total_calls']] > 2 * THRESHOLDS['total_calls'] 
                                    else THRESHOLDS['avg_duration'])
    threshold_avg_duration_B = (meta_B['avg_duration'] 
                                    if meta_B 
                                    and meta_B[ES_PROPERTY_PH['total_calls']] > 2 * THRESHOLDS['total_calls'] 
                                    else THRESHOLDS['avg_duration'])
    if log_agg['avg_duration'] >= 0.5 * min(threshold_avg_duration_A, threshold_avg_duration_B):
        return True
    return False

def check_relation_by_old_logs(log_agg, old_logs_agg, meta_A, meta_B):
    all_logs_agg = {
        'total_calls': old_logs_agg['total_calls'] + log_agg[ES_PROPERTY_PH_AGG['total_calls']],
        'total_duration': old_logs_agg['total_duration'] + log_agg[ES_PROPERTY_PH_AGG['total_duration']],
    }    
    multiple = 1 if old_logs_agg['total_months'] <= 3 else 2

    meta_A['avg_duration'] = calc_avg_duration(meta_A)
    meta_B['avg_duration'] = calc_avg_duration(meta_B)
    all_logs_agg['avg_duration'] = all_logs_agg['total_duration'] / all_logs_agg['total_calls']
    threshold_avg_duration_A = (meta_A['avg_duration'] 
                                    if meta_A 
                                    and meta_A[ES_PROPERTY_PH['total_calls']] > 2 * THRESHOLDS['total_calls'] 
                                    else THRESHOLDS['avg_duration'])
    threshold_avg_duration_B = (meta_B['avg_duration'] 
                                    if meta_B 
                                    and meta_B[ES_PROPERTY_PH['total_calls']] > 2 * THRESHOLDS['total_calls'] 
                                    else THRESHOLDS['avg_duration'])

    if all_logs_agg['total_duration'] >= multiple * THRESHOLDS['total_duration'] and \
      all_logs_agg['total_calls'] >= multiple * THRESHOLDS['total_calls'] and \
      all_logs_agg['avg_duration'] >= 0.5 * min(threshold_avg_duration_A, threshold_avg_duration_B):
        return True
    
    return False


def calc_avg_duration(metadata):
    if metadata[ES_PROPERTY_PH['total_calls']] > 0:
        total_calls_from = metadata[ES_PROPERTY_PH['total_calls']] * metadata[ES_PROPERTY_PH['call_from_rate']]
        total_calls_to = metadata[ES_PROPERTY_PH['total_calls']] - total_calls_from
        total_duration_from = metadata[ES_PROPERTY_PH['avg_duration_from']] * total_calls_from
        total_duration_to = metadata[ES_PROPERTY_PH['avg_duration_to']] * total_calls_to
        return (total_duration_from + total_duration_to) / metadata[ES_PROPERTY_PH['total_calls']]
    return 0


def build_output_message(phone_a, phone_b):
    return {
        ES_PROPERTY_RL['relation_type']: ES_VALUE_RL['relation_type'],
        ES_PROPERTY_RL['relation_id']: build_relation_id(phone_a, phone_b),
        ES_PROPERTY_RL['from_entity']: build_phone_uid(phone_a),
        ES_PROPERTY_RL['to_entity']: build_phone_uid(phone_b),
        ES_PROPERTY_RL['from_entity_type']: ES_VALUE_RL['entity_type'],
        ES_PROPERTY_RL['to_entity_type']: ES_VALUE_RL['entity_type'],
        ES_PROPERTY_RL['from_source']: ES_VALUE_RL['source'],
        ES_PROPERTY_RL['to_source']: ES_VALUE_RL['source'],
        ES_PROPERTY_RL['from_datasource']: ES_VALUE_RL['datasource'],
        ES_PROPERTY_RL['to_datasource']: ES_VALUE_RL['datasource'],
        ES_PROPERTY_RL['create_user']: ES_VALUE_RL['create_user']
    }