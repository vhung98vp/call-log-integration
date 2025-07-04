import json
import uuid
import time
from confluent_kafka import Consumer, Producer
from .utils import check_relation_by_agg, check_relation_by_agg_metadata, build_output_message, is_spam_number
from .config import logger, KAFKA, KAFKA_CONSUMER_CONFIG, KAFKA_PRODUCER_CONFIG, MES_FIELD

producer = Producer(KAFKA_PRODUCER_CONFIG)
consumer = Consumer(KAFKA_CONSUMER_CONFIG)
consumer.subscribe([KAFKA['input_topic']])


def process_message(msg_key, msg):
    start_time = time.time()
    try:
        data = json.loads(msg)
        phone_a = data.get(MES_FIELD['phone_a'])
        phone_b = data.get(MES_FIELD['phone_b'])
        log_agg = [json.loads(log_agg) for log_agg in data.get(MES_FIELD['log_agg'], [])]
        metadata_A = [json.loads(meta) for meta in data.get(MES_FIELD['meta_a'], [])]
        # metadata_B = json.loads(data.get(MES_FIELD['meta_b']))
        if any(not item for item in [phone_a, phone_b, log_agg, metadata_A]):
            logger.error(f"Invalid message data: {msg}")
            raise e

        if is_spam_number(phone_a, metadata_A):
            return
        
        if check_relation_by_agg(log_agg):
            logger.info(f"Relation detected for {phone_a}-{phone_b} by agg data")
            send_output_to_kafka(build_output_message(phone_a, phone_b))
            return

        if check_relation_by_agg_metadata(log_agg, metadata_A):
            logger.info(f"Relation detected for {phone_a}-{phone_b} by agg and metadata")
            send_output_to_kafka(build_output_message(phone_a, phone_b))
            return

        logger.info(f"Relation not detected for {phone_a}-{phone_b}.")

    except Exception as e:
        logger.exception(f"Error while processing message {msg_key}:{msg}: {e}")
        log_error_to_kafka(msg_key, { 
            "error": str(e), 
            "message": msg 
        })
        raise e
    finally:
        logger.info(f"Processed message {msg_key} in {time.time() - start_time:.4f} seconds")


def start_kafka_consumer():
    processed_count = 0
    error_count = 0
    last_wait_time = 0
    try:
        while True:
            msg = consumer.poll(KAFKA['consumer_timeout'])
            if msg is None or msg.error():
                if msg is None:
                    cur_time = time.time()
                    if cur_time - last_wait_time > 60:
                        logger.info("Waiting for messages...")
                        last_wait_time = cur_time
                else:
                    logger.error(f"Message error: {msg.error()}")
                continue
            try:
                message = msg.value().decode("utf-8")
                message_key = msg.key().decode("utf-8") if msg.key() else None
                if not message_key:
                    logger.warning(f"Received message without key: {message}")
                process_message(message_key, message)
                consumer.commit(asynchronous=False)
                processed_count += 1
            except Exception as e:
                error_count += 1
    except Exception as e:
        logger.exception(f"Consumer process terminated: {e}")
    finally:
        consumer.close()
        producer.flush()
        logger.info(f"Processed {processed_count} messages with {error_count} errors.")

def send_output_to_kafka(result: dict):
    try:
        producer.produce(KAFKA['output_topic'], key=str(uuid.uuid4()), value=json.dumps(result, ensure_ascii=False))
        producer.poll(0)
    except Exception as e:
        logger.error(f"Error sending result to output topic': {e}")

def log_error_to_kafka(msg_key, error_info: dict):
    try:
        producer.produce(KAFKA['error_topic'], key=msg_key, value=json.dumps(error_info, ensure_ascii=False))
        producer.flush()
    except Exception as e:
        logger.exception(f"Error sending to error topic: {e}")