import requests
from config import CLICKHOUSE, logger

def query_clickhouse(phone_a, phone_b):
    if CLICKHOUSE['url']:
        query = f"""
            SELECT type, duration
            FROM {CLICKHOUSE['table']}
            WHERE (A = '{phone_a}' AND B = '{phone_b}') OR (A = '{phone_b}' AND B = '{phone_a}')
        """
        data = {'query': query}
        try:
            response = requests.post(CLICKHOUSE['url'], data=data)
            response.raise_for_status()
            old_logs = response.text.strip().split('\n')
            logger.info(f"{phone_a}-{phone_b}: Received {len(old_logs)} call logs from ClickHouse.")
            return old_logs
        except requests.exceptions.RequestException as e:
            logger.error(f"Error querying ClickHouse: {e}")
            return None
    else:
        logger.warning("ClickHouse URL is not configured.")
        return None
    