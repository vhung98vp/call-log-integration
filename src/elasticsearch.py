import requests
import uuid
from config import ES, KAFKA, logger


def query_relation(phone_a, phone_b):
    relation_type = KAFKA['relation_type']
    relation_id_1 = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{relation_type}:{phone_a}:{phone_b}"))
    relation_id_2 = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{relation_type}:{phone_a}:{phone_b}"))
    url = f"http://{ES['url']}/{ES['relation_index']}/_search"
    auth = (ES['user'], ES['password']) if ES['user'] and ES['password'] else None
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "terms": {
                "properties.relation_id.keyword": [relation_id_1, relation_id_2]
            }
        }
    }
    try:
        response = requests.get(url=url,
                                    headers=headers,
                                    auth=auth,
                                    json=query)
        response.raise_for_status()
        response_hits = response.json()['hits']['hits']
        return response_hits[0]['_source'] if response_hits else None
    except Exception as e:
        logger.error(f"Error querying Elasticsearch: {e}")
        return None

def query_phone_entity(phone_a, phone_b):
    url = f"http://{ES['url']}/{ES['phone_index']}/_search"
    auth = (ES['user'], ES['password'])
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "terms": {
                "properties.phone_number.keyword": [phone_a, phone_b]
            }
        }
    }
    try:
        response = requests.get(url=url,
                                    headers=headers,
                                    auth=auth,
                                    json=query)
        response.raise_for_status()
        response_hits = response.json()['hits']['hits']
        if not response_hits:
            logger.warning(f"No phone entity found for {phone_a} or {phone_b}")
            return None, None
        for hit in response_hits:
            if hit['_source']['properties']['phone_number'] == phone_a:
                meta_A = hit['_source']
            elif hit['_source']['properties']['phone_number'] == phone_b:
                meta_B = hit['_source']
        return meta_A, meta_B
    except Exception as e:
        logger.error(f"Error querying Elasticsearch: {e}")
        return None
