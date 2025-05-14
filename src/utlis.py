from config import THRESHOLDS, logger


def check_relation_by_agg(log_agg, meta_A, meta_B):
    if log_agg['total_duration'] >= THRESHOLDS['total_duration'] or \
      log_agg['max_duration'] >= THRESHOLDS['max_duration'] or \
      (log_agg['total_calls'] >= THRESHOLDS['total_calls'] and \
        log_agg['avg_days'] >= THRESHOLDS['avg_days']):
        logger.info("Found relation based on aggregated logs.")
        return True
    threshold_avg_duration_A = meta_A['avg_duration'] if meta_A and meta_A['total_calls'] > 2 * THRESHOLDS['total_calls'] else THRESHOLDS['avg_duration']
    threshold_avg_duration_B = meta_B['avg_duration'] if meta_B and meta_B['total_calls'] > 2 * THRESHOLDS['total_calls'] else THRESHOLDS['avg_duration']
    if log_agg['avg_duration'] >= 0.5 * min(threshold_avg_duration_A, threshold_avg_duration_B):
        return True
    return False

def check_relation_by_old_logs(log_agg, old_logs, meta_A, meta_B):
    all_logs_agg = {
        'total_duration': sum(log['duration'] for log in old_logs) + log_agg['total_duration'],
        'max_duration': max(log['duration'] for log in old_logs),
        'total_calls': len(old_logs) + log_agg['total_calls'],
    }
    number_of_months = len(set(log['start_time'][:7] for log in old_logs))
    multiple = 1 if number_of_months <= 3 else 1.5

    threshold_avg_duration_A = meta_A['avg_duration'] if meta_A and meta_A['total_calls'] > 2 * THRESHOLDS['total_calls'] else THRESHOLDS['avg_duration']
    threshold_avg_duration_B = meta_B['avg_duration'] if meta_B and meta_B['total_calls'] > 2 * THRESHOLDS['total_calls'] else THRESHOLDS['avg_duration']
    if all_logs_agg['total_duration'] >= multiple * THRESHOLDS['total_duration'] and \
      all_logs_agg['total_calls'] >= multiple * THRESHOLDS['total_calls'] and \
      all_logs_agg['avg_duration'] >= 0.5 * min(threshold_avg_duration_A, threshold_avg_duration_B):
        return True
    
    return False