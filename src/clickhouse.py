import requests
import json
from .config import CLICKHOUSE, LOG_PROPERTY, logger

def query_clickhouse(phone_a, phone_b):
    try: # Convert phone numbers to integers for clickhouse query
        phone_a = int(phone_a)
        phone_b = int(phone_b)
    except ValueError:
        logger.error(f"Invalid phone numbers: {phone_a}, {phone_b}")
        return None

    queries = []
    for table in CLICKHOUSE['tables']:
        qr = f"""
            SELECT {LOG_PROPERTY['duration']}, {LOG_PROPERTY['start_time']}, {LOG_PROPERTY['call_type']}
            FROM {table}
            WHERE ({LOG_PROPERTY['phone_a']} = '{phone_a}' AND {LOG_PROPERTY['phone_b']} = '{phone_b}') 
                    OR ({LOG_PROPERTY['phone_a']} = '{phone_b}' AND {LOG_PROPERTY['phone_b']} = '{phone_a}')
        """
        queries.append(qr)
    union_query = " UNION ALL ".join(queries)
    query = f"{union_query} LIMIT {CLICKHOUSE['query_limit']} FORMAT JSONEachRow"

    url = CLICKHOUSE['url']
    headers = {'Content-Type': 'application/json'}
    auth = (CLICKHOUSE['user'], CLICKHOUSE['password']) if CLICKHOUSE['user'] and CLICKHOUSE['password'] else None
    try:
        response = requests.post(url=url, 
                                    headers=headers,
                                    auth=auth, 
                                    data=query)
        response.raise_for_status()
        old_logs = [json.loads(log) for log in response.text.strip().splitlines()]
        logger.info(f"{phone_a}-{phone_b}: Received {len(old_logs)} call logs from ClickHouse.")
        return agg_logs(old_logs)
    except Exception as e:
        logger.error(f"Failed to fetch from Clickhouse: {e}")
        return None

def agg_logs(logs):
    unique_logs = {log[LOG_PROPERTY['start_time']]: log for log in logs}.values()
    return {} if not unique_logs else {
        'total_calls': len(unique_logs),
        'total_duration': sum(log[LOG_PROPERTY['duration']] for log in unique_logs),
        'total_months': len(set(log[LOG_PROPERTY['start_time']][:7] for log in unique_logs))
    }
    