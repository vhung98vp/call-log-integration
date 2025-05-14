import json
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Consumer, Producer
from .elasticsearch import query_phone_entity, query_relation
from .clickhouse import query_clickhouse
from .utlis import check_relation_by_agg, check_relation_by_old_logs
from .config import logger, KAFKA, KAFKA_CONSUMER_CONFIG, KAFKA_PRODUCER_CONFIG, MAX_WORKERS

producer = Producer(KAFKA_PRODUCER_CONFIG)
consumer = Consumer(KAFKA_CONSUMER_CONFIG)
consumer.subscribe([KAFKA['input_topic']])

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


def process_message(msg_key, msg):
    start_time = time.time()
    try:
        data = json.loads(msg)
        phone_a = data.get('phone_a')
        phone_b = data.get('phone_b')
        log_agg = data.get('log_agg')
        metadata_A = data.get('meta_A')
        metadata_B = data.get('meta_B')
        if any(not data[key] for key in ['phone_a', 'phone_b', 'log_agg', 'meta_A', 'meta_B']):
            logger.warning(f"Invalid message data: {msg}")
            return

        logger.info(f"Processing Kafka message for {phone_a}-{phone_b}...")

        current_relation = query_relation(phone_a, phone_b)
        if current_relation:
            logger.info(f"Found relation_id from Elasticsearch for {phone_a}-{phone_b}: {current_relation['relation_id']}")
            return
        
        if check_relation_by_agg(log_agg, metadata_A, metadata_B):
            logger.info(f"Relation detected for {phone_a}-{phone_b} by current data")
            send_output_to_kafka(phone_a, phone_b)
            return

        logger.info(f"{phone_a}-{phone_b} has no relation. Checking old data...")

        # Check old logs in ClickHouse
        old_logs = query_clickhouse(phone_a, phone_b)
        if old_logs:
            meta_A, meta_B = query_phone_entity(phone_a, phone_b)
            if check_relation_by_old_logs(log_agg, old_logs, meta_A, meta_B):
                logger.info(f"Relation detected for {phone_a}-{phone_b} by old data")    
                send_output_to_kafka(phone_a, phone_b)
                return

        logger.info(f"Relation not found between {phone_a} and {phone_b}.")

    except Exception as e:
        logger.exception(f"Error while processing message {msg_key}:{msg}: {e}")
        log_error_to_kafka(msg_key, { 
            "error": str(e), 
            "message": msg 
        })
    finally:
        logger.info(f"Processed message in {time.time() - start_time:.4f} seconds")


def start_kafka_consumer():
    processed_count = 0
    error_count = 0
    try:
        while True:
            msg = consumer.poll(KAFKA['consumer_timeout'])
            if msg is None or msg.error():
                if msg.error():
                    logger.error(f"Message error: {msg.error()}")
                else:
                    logger.info("Waiting for messages...")
                continue
            try:
                message = msg.value().decode("utf-8")
                message_key = msg.key().decode("utf-8") if msg.key() else None
                if not message_key:
                    logger.warning(f"Received message without key: {message}")
                executor.submit(process_message, message_key, message)
                consumer.commit(asynchronous=False)
                processed_count += 1
            except Exception as e:
                logger.exception(f"Failed to process message: {e}")
                error_count += 1
    except Exception as e:
        logger.exception(f"Consumer process terminated: {e}")
    finally:
        consumer.close()
        logger.info(f"Processed {processed_count} messages with {error_count} errors.")

def send_output_to_kafka(phone_a, phone_b):
    output_topic = output_topic
    relation_type = KAFKA['relation_type']
    from_entity = str(uuid.uuid5(uuid.NAMESPACE_DNS, phone_a))
    to_entity = str(uuid.uuid5(uuid.NAMESPACE_DNS, phone_b))
    relation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{relation_type}:{phone_a}:{phone_b}"))

    output_message = {
        'relationType': relation_type,
        'fromEntity': from_entity,
        'toEntity': to_entity,
        'fromEntityType': KAFKA['entity_type'],
        'toEntityType': KAFKA['entity_type'],
        'fromSource': KAFKA['source'],
        'toSource': KAFKA['source'],
        'fromDataSource': KAFKA['datasource'],
        'toDataSource': KAFKA['datasource'],
        'createUser': KAFKA['es_create_user'],
        'relationId': relation_id
    }

    try:
        producer.produce(output_topic, key=str(uuid.uuid4()), value=json.dumps(output_message))
        producer.poll(0)
        logger.info(f"Sent relation uid '{relation_id}' to topic '{output_topic}'.")
    except Exception as e:
        logger.error(f"Error sending result to output topic': {e}")

def log_error_to_kafka(msg_key, error_info: dict):
    try:
        producer.produce(KAFKA['error_topic'], key=msg_key, value=json.dumps(error_info))
        producer.flush()
    except Exception as e:
        logger.exception(f"Error sending to error topic: {e}")