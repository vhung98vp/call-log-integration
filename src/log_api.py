import requests
from .config import LOG_API, LOG_PROPERTY, logger

def query_log_api(phone_a, phone_b):
    try: # Convert phone numbers to integers for api query
        phone_a = int(phone_a)
        phone_b = int(phone_b)
    except ValueError:
        logger.error(f"Invalid phone numbers: {phone_a}, {phone_b}")
        return None


    url = LOG_API['url']
    headers = {'Content-Type': 'application/json'}
    params = {
        "phone_a": phone_a, 
        "phone_b": phone_b
    }
    try:
        response = requests.get(url=url, 
                                    headers=headers,
                                    params=params)
        response.raise_for_status()
        old_logs = response.json().get('data', [])
        logger.info(f"{phone_a}-{phone_b}: Received {len(old_logs)} call logs from API.")
        return agg_logs(old_logs)
    except Exception as e:
        logger.error(f"Failed to fetch from Log API: {e}")
        raise e

def agg_logs(logs):
    unique_logs = {log[LOG_PROPERTY['start_time']]: log for log in logs}.values()
    return {} if not unique_logs else {
        'total_calls': len(unique_logs),
        'total_duration': sum(log[LOG_PROPERTY['duration']] for log in unique_logs),
        'total_months': len(set(log[LOG_PROPERTY['start_time']][:7] for log in unique_logs))
    }