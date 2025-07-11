import requests
import re
from .config import ES, ES_PROPERTY, ES_PHONE_PROPERTY, logger
from .utils import build_relation_id, build_phone_uid


def query_relation(phone_a, phone_b):
    relation_id_1 = build_relation_id(phone_a, phone_b)
    relation_id_2 = build_relation_id(phone_b, phone_a)
    url = f"{ES['url']}/{ES['relation_index']}/_search"
    auth = (ES['user'], ES['password']) if ES['user'] and ES['password'] else None
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "terms": {
                f"{ES_PROPERTY['relation_id_search']}.keyword": [relation_id_1, relation_id_2]
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
        return response_hits[0]['_source'][ES_PROPERTY['relation_id_search']] if response_hits else None
    except Exception as e:
        logger.error(f"Failed to fetch from Elasticsearch: {e}")
        return None

def query_phone_entity(phone_a, phone_b):
    uid_a = build_phone_uid(phone_a)
    uid_b = build_phone_uid(phone_b)
    url = f"{ES['url']}/{ES['phone_index']}/_search"
    auth = (ES['user'], ES['password']) if ES['user'] and ES['password'] else None
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "terms": {
                # f"properties.{ES_PROPERTY['phone_number_search']}.keyword": [phone_a, phone_b]
                ES_PROPERTY['phone_id']: [uid_a, uid_b]
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
        meta_A, meta_B = None, None
        if not response_hits:
            logger.warning(f"No phone entity found for {phone_a} or {phone_b}")
        else:
            for hit in response_hits:
                hit_properties = transform_properties(hit['_source']['properties'])
                if hit_properties[ES_PHONE_PROPERTY['phone_number']] == phone_a:
                    meta_A = hit_properties
                elif hit_properties[ES_PHONE_PROPERTY['phone_number']] == phone_b:
                    meta_B = hit_properties
        return meta_A, meta_B
    except Exception as e:
        logger.error(f"Failed to fetch from Elasticsearch: {e}")
        return None

def transform_properties(properties):
    normal_dict = {}
    for key, value in properties.items():
        if isinstance(value, list) and len(value) == 1:
            value = value[0]
        if ES_PROPERTY['suffix_pattern']:
            key = re.sub(ES['suffix_pattern'], '', key)
        if key in ES_PHONE_PROPERTY:
            normal_dict[key] = float(value) # Map all selected es properties to float
    return normal_dict