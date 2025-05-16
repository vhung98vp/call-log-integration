import requests
import re
from config import ES, ES_PROPERTY_SR, ES_PROPERTY_PH, logger
from .utlis import build_relation_id


def query_relation(phone_a, phone_b):
    relation_id_1 = build_relation_id(phone_a, phone_b)
    relation_id_2 = build_relation_id(phone_b, phone_a)
    url = f"http://{ES['url']}/{ES['relation_index']}/_search"
    auth = (ES['user'], ES['password']) if ES['user'] and ES['password'] else None
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "terms": {
                f"{ES_PROPERTY_SR['relation_id_search']}.keyword": [relation_id_1, relation_id_2]
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
        return response_hits[0]['_source'][ES_PROPERTY_SR['relation_id_search']] if response_hits else None
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
                f"properties.{ES_PROPERTY_SR['phone_number_search']}.keyword": [phone_a, phone_b]
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
            hit_properties = transform_properties(hit['_source']['properties'])
            if hit_properties[ES_PROPERTY_PH['phone_number']] == phone_a:
                meta_A = hit_properties
            elif hit_properties[ES_PROPERTY_PH['phone_number']] == phone_b:
                meta_B = hit_properties
        return meta_A, meta_B
    except Exception as e:
        logger.error(f"Error querying Elasticsearch: {e}")
        return None

def transform_properties(properties):
    normal_dict = {}
    for key, value in properties.items():
        if isinstance(value, list) and len(value) == 1:
            value = value[0]
        if ES['suffix_pattern']:
            key = re.sub(ES['suffix_pattern'], '', key)
        if key in ES_PROPERTY_PH:
            normal_dict[key] = value
    return normal_dict