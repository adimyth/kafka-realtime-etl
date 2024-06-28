import json
import time

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from loguru import logger
from schema_registry import event_json_serializer
from settings import settings


# Kafka consumer setup for DLQ
dlq_consumer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "dlq_processor",
    # Start reading from the earliest available message in the DLQ
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": StringDeserializer("utf_8"),
}
dlq_consumer = DeserializingConsumer(dlq_consumer_conf)
dlq_consumer.subscribe([settings.DLQ_TOPIC])

# Kafka producer setup for reprocessing
reprocess_producer_conf = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": event_json_serializer,
}
reprocess_producer = SerializingProducer(reprocess_producer_conf)


def process_dlq_event(event):
    """Process a single event from the DLQ."""
    try:
        event_data = json.loads(event)
        # TODO: Implement the logic to fix the event here
        fixed_event = fix_event(event_data)

        # Push the fixed event back to the main topic for reprocessing
        reprocess_producer.produce(
            topic=f"event_{fixed_event['event_type']}",
            key=str(fixed_event["event_name"]),
            value=fixed_event,
            on_delivery=reprocess_delivery_report,
        )
        reprocess_producer.flush()

        logger.info(f"Successfully reprocessed event: {fixed_event['event_name']}")
        return True
    except Exception as e:
        logger.error(f"Failed to process DLQ event: {str(e)}")
        return False


def fix_event(event_data):
    """
    This is where we would handle different error cases and apply corrections.
    """
    # For example, we might handle missing fields, incorrect data types, etc.
    if "user_details" not in event_data:
        event_data["user_details"] = {}
    if "event_details" not in event_data:
        event_data["event_details"] = {}
    # Add more fixing logic as needed
    return event_data


def reprocess_delivery_report(err, msg):
    if err is not None:
        logger.error(f"Failed to reprocess event: {str(err)}")
    else:
        logger.info(
            f"Event reprocessed to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def run_correction_mechanism():
    while True:
        try:
            msg = dlq_consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"DLQ Consumer error: {msg.error()}")
                continue

            if process_dlq_event(msg.value()):
                dlq_consumer.commit()
        except Exception as e:
            logger.error(f"Unexpected error in correction mechanism: {str(e)}")

        # Sleep for the specified interval
        time.sleep(settings.CORRECTION_INTERVAL)


if __name__ == "__main__":
    run_correction_mechanism()
