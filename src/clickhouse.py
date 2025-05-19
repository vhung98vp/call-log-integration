import requests
from .config import CLICKHOUSE, CH_PROPERTY, logger

def query_clickhouse(phone_a, phone_b):
    try: # Convert phone numbers to integers for clickhouse query
        phone_a = int(phone_a)
        phone_b = int(phone_b)
    except ValueError:
        logger.error(f"Invalid phone numbers: {phone_a}, {phone_b}")
        return None

    query = f"""
        SELECT {CH_PROPERTY['duration']}, {CH_PROPERTY['start_time']}, {CH_PROPERTY['call_type']}
        FROM {CLICKHOUSE['table']}
        WHERE ({CH_PROPERTY['phone_a']} = '{phone_a}' AND {CH_PROPERTY['phone_b']} = '{phone_b}') 
                OR ({CH_PROPERTY['phone_a']} = '{phone_b}' AND {CH_PROPERTY['phone_b']} = '{phone_a}')
    """

    try:
        response = requests.post(CLICKHOUSE['url'], data={'query': query})
        response.raise_for_status()
        old_logs = response.text.strip().split('\n')
        logger.info(f"{phone_a}-{phone_b}: Received {len(old_logs)} call logs from ClickHouse.")
        return agg_logs(old_logs)
    except Exception as e:
        logger.error(f"Failed to fetch from Clickhouse: {e}")
        return None

def agg_logs(logs):
    unique_logs = {log[CH_PROPERTY['start_time']]: log for log in logs}.values()
    return {} if not unique_logs else {
        'total_calls': len(unique_logs),
        'total_duration': sum(log[CH_PROPERTY['duration']] for log in unique_logs),
        'total_months': len(set(log[CH_PROPERTY['start_time']][:7] for log in unique_logs))
    }
    