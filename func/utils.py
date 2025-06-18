import uuid
from config import logger, ES_RL_CONF, RL_THRESHOLDS, SPAM_THRESHOLDS, SERVICE_THRESHOLDS,\
    ES_RL_PROPERTY, MES_PHONE_AGG, MES_PHONE_MD

# BUILD
def build_phone_uid(phone_number, entity_type=ES_RL_CONF['entity_type'], namespace=ES_RL_CONF['uid_namespace']):
    return str(uuid.uuid5(namespace, f"{entity_type}:{phone_number}"))

def build_relation_id(phone_a, phone_b, relation_type=ES_RL_CONF['relation_type'], namespace=ES_RL_CONF['uid_namespace']):
    return str(uuid.uuid5(namespace, f"{relation_type}:{phone_a}:{phone_b}"))

def build_output_message(phone_a, phone_b, property=ES_RL_PROPERTY, conf=ES_RL_CONF):
    return {
        property['relation_type']: conf['relation_type'],
        property['relation_id']: build_relation_id(phone_a, phone_b),
        property['from_entity']: build_phone_uid(phone_a),
        property['to_entity']: build_phone_uid(phone_b),
        property['from_entity_type']: conf['entity_type'],
        property['to_entity_type']: conf['entity_type'],
        property['from_source']: conf['source'],
        property['to_source']: conf['source'],
        property['from_datasource']: conf['datasource'],
        property['to_datasource']: conf['datasource'],
        property['create_user']: conf['create_user']
    }


# CALCULATE
def calc_threshold_avg_duration(meta_list, property=MES_PHONE_MD):
    total_calls_all, total_duration_all = 0, 0
    for meta in meta_list:
        total_calls_all += meta[property['total_calls']]
        total_duration_from = meta[property['avg_duration_from']] * meta[property['total_calls']] * meta[property['call_from_rate']]
        total_duration_to = meta[property['avg_duration_to']] * meta[property['total_calls']] * (1 - meta[property['call_from_rate']])
        total_duration_all += total_duration_from + total_duration_to
    avg_duration_all = total_duration_all / total_calls_all if total_calls_all else 0
    return avg_duration_all if total_calls_all > 2 * RL_THRESHOLDS['total_calls'] else RL_THRESHOLDS['avg_duration']


# CHECK RELATION AND SPAM/SERVICE
def check_relation(agg_list, metalist_A, phone_A, phone_B, property=MES_PHONE_AGG):
    # Detect by log aggregation
    for log_agg in agg_list:
        if log_agg[property['total_duration']] >= RL_THRESHOLDS['total_duration'] or \
        log_agg[property['max_duration']] >= RL_THRESHOLDS['max_duration'] or \
        (log_agg[property['total_calls']] >= RL_THRESHOLDS['total_calls'] and \
            log_agg[property['avg_days']] >= RL_THRESHOLDS['avg_days']):
            logger.info(f"Relation detected by log: {phone_A}-{phone_B}")

            return True

    # Detect by log and metadata aggregation
    multiple = 1 if len(agg_list) <= 3 else 2
    agg_keys = ['total_calls', 'total_duration', 'avg_days']
    agg_total = {k: sum(d.get(property[k], 0) for d in agg_list) for k in agg_keys}
    agg_total['avg_duration'] = agg_total['total_duration'] / agg_total['total_calls'] if agg_total['total_calls'] else 0
    threshold_avg_duration = calc_threshold_avg_duration(metalist_A)

    if (agg_total['total_calls'] >= multiple * RL_THRESHOLDS['total_calls'] and
            agg_total['avg_duration'] >= multiple * 0.5 * threshold_avg_duration):
        logger.info(f"Relation detected by log and metadata: {phone_A}-{phone_B}")
        return True

    logger.info(f"Relation not detected: {phone_A}-{phone_B}")
    return False



def is_spam_number(phone_number, meta_list, property=MES_PHONE_MD):
    for metadata in meta_list:
        metadata['avg_call_per_day'] = metadata[property['total_calls']] \
                * metadata[property['call_from_rate']] / metadata[property['total_day_from']] \
                    if metadata[property['total_day_from']] else 0
        metadata['avg_call_per_contact'] = metadata[property['total_calls']] \
                * metadata[property['call_from_rate']] / metadata[property['total_contacts']] \
                    if metadata[property['total_contacts']] else 0
        metadata['total_duration'] = metadata[property['total_calls']] * metadata[property['call_from_rate']] \
                * metadata[property['avg_duration_from']] + \
                metadata[property['total_calls']] * (1 - metadata[property['call_from_rate']]) \
                * metadata[property['avg_duration_to']]

        # Spam detection with extreme values
        if metadata['avg_call_per_day'] > SPAM_THRESHOLDS['extreme_avg_call_per_day'] or \
            metadata['total_duration'] > SPAM_THRESHOLDS['extreme_total_duration']:
            logger.info(f"Spam number detected by filter extreme: {phone_number}")

            return True
        
        # Spam detection
        if metadata['avg_call_per_day'] > SPAM_THRESHOLDS['avg_call_per_day'] and \
            metadata[property['total_contacts']] > SPAM_THRESHOLDS['total_contacts'] and \
            metadata['avg_call_per_contact'] < SPAM_THRESHOLDS['avg_call_per_contact'] and \
            metadata[property['call_from_rate']] > SPAM_THRESHOLDS['call_from_rate'] and \
            metadata[property['avg_duration_from']] < SPAM_THRESHOLDS['avg_duration']:
            logger.info(f"Spam number detected by filter: {phone_number}")

            return True

        # Service detection
        if metadata['avg_call_per_day'] > SERVICE_THRESHOLDS['avg_call_per_day'] and \
            metadata[property['total_day_from']] > SERVICE_THRESHOLDS['total_day_from'] and \
            metadata[property['total_contacts']] > SERVICE_THRESHOLDS['total_contacts'] and \
            metadata[property['call_from_rate']] > SERVICE_THRESHOLDS['call_from_rate'] and \
            metadata[property['avg_duration_from']] < SERVICE_THRESHOLDS['avg_duration']:
            logger.info(f"Service number detected by filter: {phone_number}")

            return True

    return False